"""Pure v3 QQBot protocol adapter."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import mimetypes
import time
from collections.abc import AsyncIterable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, cast

import httpx
import websockets

from agent.plugin_composition.channels import (
    AttachmentKind,
    AttachmentRef,
    ChannelAdapter,
    ChannelCleanupFailure,
    ChannelFactoryContext,
    ChannelInboundMessage,
    ChannelPresentationPorts,
    ChannelReady,
    ControlResponseBodies,
    CredentialRef,
    DeliveryStatus,
    PresentationReceipt,
    ProviderDeliveryReceipt,
    ProviderDeliveryRequest,
    RawInbound,
    StopReceipt,
    StreamDeltaPresentation,
    TurnOutputCompletedPresentation,
    TurnStartedPresentation,
    TurnStreamEvent,
    TurnStreamEventKind,
)


logger = logging.getLogger(__name__)

_CHANNEL = "qqbot"
_API_BASE = "https://api.sgroup.qq.com"
_TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken"
_REJECTED_HTTP_STATUSES = frozenset({400, 401, 403, 404, 405, 413, 415, 422})
_LIVE_STREAM_MIN_CHARS = 120
_LIVE_STREAM_MIN_INTERVAL_S = 1.5
_LIVE_MAX_FAILURES = 3
_REPLY_LIVE_TAIL = 900
_CREDENTIAL_ALIASES = {
    "app_id": ("appId", "app_id"),
    "client_secret": ("clientSecret", "client_secret"),
}
_MAX_ATTACHMENT_BYTES = 50 * 1024 * 1024


@dataclass(slots=True)
class _TokenCache:
    token: str
    expires_at: float


@dataclass(slots=True)
class _LiveStreamState:
    openid: str
    msg_id: str
    msg_seq: int
    stream_msg_id: str = ""
    index: int = 0


def build_qqbot_channel(context: ChannelFactoryContext) -> ChannelAdapter:
    """Build a side-effect-free QQBot adapter for one exact binding."""

    if not isinstance(context, ChannelFactoryContext):
        raise TypeError("QQBot channel factory 只接受 ChannelFactoryContext")
    if context.ingress is None or context.identity is None:
        raise RuntimeError("QQBot v3 channel 需要 Core ingress/identity ports")
    if context.control is None or context.turn_stream is None:
        raise RuntimeError("QQBot v3 channel 需要 Core control/turn-stream ports")
    return QQBotAdapter(context)


class QQBotAdapter:
    """Translate QQBot text, control, delivery, and preview events to C14 ports."""

    name = _CHANNEL

    def __init__(self, context: ChannelFactoryContext) -> None:
        self._context = context
        self._identity = context.identity
        self._ingress = context.ingress
        self._provider_factory = context.provider_client_factory
        self._credentials = context.credentials
        self._config = context.config
        self._binding_token = context.binding_token
        self._allow_from = _allow_from(self._config)

        self._presentation: ChannelPresentationPorts | None = None
        self._stream_subscription: Any | None = None
        self._provider_client: Any | None = None
        self._client: httpx.AsyncClient | None = None
        self._token: _TokenCache | None = None
        self._gateway_task: asyncio.Task[None] | None = None
        self._stop_task: asyncio.Task[StopReceipt] | None = None
        self._stopped = asyncio.Event()
        self._started = False
        self._stopping = False

        self._message_recipients: dict[str, str] = {}
        self._message_identities: dict[str, str] = {}
        self._presentation_recipients: dict[str, str] = {}
        self._presentation_message_ids: dict[str, str] = {}
        self._reply_buffers: dict[str, str] = {}
        self._live_states: dict[str, _LiveStreamState] = {}
        self._live_next_at: dict[str, float] = {}
        self._live_last_lengths: dict[str, int] = {}
        self._live_failures: dict[str, int] = {}
        self._live_disabled: set[str] = set()
        self._live_uncertain: set[str] = set()
        self._live_locks: dict[str, asyncio.Lock] = {}

    def attach_presentation(self, ports: ChannelPresentationPorts) -> None:
        """Bind exact control and turn-stream facades before start."""

        if self._presentation is not None:
            raise RuntimeError("QQBot presentation ports 不能重复绑定")
        if ports.control is None or ports.turn_stream is None:
            raise RuntimeError("QQBot v3 必须同时绑定 control 与 turn_stream")
        self._presentation = ports

    async def start(self) -> ChannelReady:
        """Create formal provider resources and start the gateway closed."""

        if self._started or self._stopping:
            raise RuntimeError("QQBot adapter 已启动或正在停止")
        if self._presentation is None:
            raise RuntimeError("QQBot adapter 缺少 presentation ports")

        try:
            # 1. Validate both credential identities before acquiring any resource.
            self._credential_ref("app_id")
            self._credential_ref("client_secret")

            # 2. Resolve only through the formal provider factory.
            self._provider_client = await self._provider_factory.create(self._credentials)
            self._client = httpx.AsyncClient(timeout=30.0)

            # 3. Attach the exact presentation callback before receiving provider input.
            turn_stream = self._presentation.turn_stream
            if turn_stream is None:
                raise RuntimeError("QQBot turn stream port 未绑定")
            self._stream_subscription = turn_stream.subscribe(self._on_turn_stream)
            self._stopped.clear()
            self._gateway_task = asyncio.create_task(
                self._gateway_loop(),
                name=f"qqbot-gateway:{self._context.generation_id}",
            )
            self._started = True
            return ChannelReady(
                self._binding_token,
                subscriptions=("qqbot.gateway", "qqbot.turn_stream"),
                admission_open=False,
            )
        except BaseException as error:
            cleanup = await _await_task_after_cancellation(
                asyncio.create_task(
                    self._stop_impl(),
                    name=f"qqbot-start-cleanup:{self._context.generation_id}",
                )
            )
            if cleanup.failures:
                error.add_note(
                    "QQBot start cleanup failed: "
                    + "; ".join(
                        f"{item.resource}: {item.message}" for item in cleanup.failures
                    )
                )
            raise

    async def deliver(self, request: ProviderDeliveryRequest) -> ProviderDeliveryReceipt:
        """Read exact Core attachments, send ordered parts, and settle one receipt."""

        if not isinstance(request, ProviderDeliveryRequest):
            raise TypeError("QQBot deliver 只接受 ProviderDeliveryRequest")
        if request.binding_token != self._binding_token:
            raise RuntimeError("QQBot delivery binding token 不匹配")
        if not request.body.strip() and not request.attachments:
            return ProviderDeliveryReceipt(
                request.delivery_id,
                DeliveryStatus.REJECTED,
                error="QQBot 空消息被拒绝",
            )
        if not isinstance(request.recipient, str):
            return ProviderDeliveryReceipt(
                request.delivery_id,
                DeliveryStatus.REJECTED,
                error="QQBot recipient 必须是字符串",
            )
        try:
            _parse_recipient(request.recipient)
        except ValueError as error:
            return ProviderDeliveryReceipt(
                request.delivery_id,
                DeliveryStatus.REJECTED,
                error=str(error),
            )
        # 1. Read and hash-check every Core-owned attachment before provider effect.
        try:
            attachment_data = await self._read_attachments(request.attachments)
        except asyncio.CancelledError:
            raise
        except (RuntimeError, TypeError, ValueError) as error:
            return ProviderDeliveryReceipt(
                request.delivery_id,
                DeliveryStatus.REJECTED,
                error=f"QQBot 附件读取失败: {error}",
            )

        # 2. Send text first, then media in exact request order.
        provider_ids: list[str] = []
        if request.body.strip():
            status, provider_id, error = await self._send_text(
                request.recipient,
                request.body,
            )
            if provider_id:
                provider_ids.append(provider_id)
            if status is not DeliveryStatus.DELIVERED:
                if provider_ids and status is DeliveryStatus.REJECTED:
                    status = DeliveryStatus.UNKNOWN
                return ProviderDeliveryReceipt(
                    request.delivery_id,
                    status,
                    tuple(provider_ids),
                    error=error,
                )
        for ref, data in attachment_data:
            status, provider_id, error = await self._send_attachment(
                request.recipient,
                ref,
                data,
            )
            if provider_id:
                provider_ids.append(provider_id)
            if status is not DeliveryStatus.DELIVERED:
                return ProviderDeliveryReceipt(
                    request.delivery_id,
                    status,
                    tuple(provider_ids),
                    error=error,
                )
        return ProviderDeliveryReceipt(
            request.delivery_id,
            DeliveryStatus.DELIVERED,
            tuple(provider_ids),
        )

    async def _read_attachments(
        self,
        refs: tuple[AttachmentRef, ...],
    ) -> list[tuple[AttachmentRef, bytes]]:
        """Read and hash-check Core attachments while retaining no path access."""

        if not refs:
            return []
        attachment_read = self._context.attachment_read
        if attachment_read is None:
            raise RuntimeError("QQBot outbound 附件缺少 Core attachment_read")
        result: list[tuple[AttachmentRef, bytes]] = []
        for ref in refs:
            lease = await attachment_read.acquire(ref)
            try:
                if lease.ref != ref:
                    raise RuntimeError("QQBot attachment read lease ref 不匹配")
                data = await lease.read_bytes(
                    max_bytes=min(max(ref.size_bytes, 1), _MAX_ATTACHMENT_BYTES)
                )
                if len(data) != ref.size_bytes:
                    raise ValueError(
                        f"附件大小不匹配: expected={ref.size_bytes} actual={len(data)}"
                    )
                if hashlib.sha256(data).hexdigest() != ref.sha256:
                    raise ValueError("附件 sha256 不匹配")
                result.append((ref, data))
            finally:
                await _close_attachment_lease(lease)
        return result

    async def _send_attachment(
        self,
        recipient: str,
        ref: AttachmentRef,
        data: bytes,
    ) -> tuple[DeliveryStatus, str | None, str | None]:
        """Upload one verified attachment and send its rich-media message."""

        _, openid = _parse_recipient(recipient)
        upload_status, upload_payload, upload_error = await self._request_with_status(
            "POST",
            f"/v2/users/{openid}/files",
            {
                "file_type": 1 if ref.kind is AttachmentKind.IMAGE else 4,
                "file_data": base64.b64encode(data).decode("ascii"),
                "srv_send_msg": False,
                **(
                    {"file_name": ref.filename}
                    if ref.kind is AttachmentKind.FILE and ref.filename
                    else {}
                ),
            },
        )
        if upload_status is not DeliveryStatus.DELIVERED:
            return upload_status, None, upload_error
        file_info = str(upload_payload.get("file_info") or "").strip()
        if not file_info:
            return DeliveryStatus.UNKNOWN, None, "QQBot media upload response 缺少 file_info"
        send_status, send_payload, send_error = await self._request_with_status(
            "POST",
            f"/v2/users/{openid}/messages",
            {
                "msg_type": 7,
                "media": {"file_info": file_info},
                "msg_seq": _next_msg_seq(),
            },
        )
        # Upload has already changed provider state; a failed follow-up send is unknown.
        if send_status is not DeliveryStatus.DELIVERED:
            return DeliveryStatus.UNKNOWN, None, send_error
        provider_id = str(send_payload.get("id") or "").strip()
        if not provider_id:
            return DeliveryStatus.UNKNOWN, None, "QQBot rich-media response 缺少 message id"
        return DeliveryStatus.DELIVERED, provider_id, None

    async def stop(self) -> StopReceipt:
        """Close gateway, stream subscription, HTTP, and provider resources."""

        task = self._stop_task
        if task is None or task.done():
            task = asyncio.create_task(
                self._stop_impl(),
                name=f"qqbot-stop:{self._context.generation_id}",
            )
            self._stop_task = task
        return await _await_task_after_cancellation(task)

    async def _stop_impl(self) -> StopReceipt:
        """Close every owned resource and retain failed owners for retry."""

        self._stopping = True
        failures: list[ChannelCleanupFailure] = []

        # 1. Close callback admission before provider resources.
        subscription = self._stream_subscription
        if subscription is not None:
            try:
                subscription.close_admission()
                await subscription.await_quiescence()
                await subscription.close()
                self._stream_subscription = None
            except BaseException as error:
                failures.append(self._cleanup_failure("turn-stream", error))

        # 2. Stop the receive loop and drain its heartbeat child.
        self._stopped.set()
        gateway = self._gateway_task
        if gateway is not None:
            gateway.cancel()
            result = await asyncio.gather(gateway, return_exceptions=True)
            error = result[0]
            if isinstance(error, BaseException) and not isinstance(
                error, asyncio.CancelledError
            ):
                failures.append(self._cleanup_failure("gateway", error))
            else:
                self._gateway_task = None

        # 3. Release formal clients; failed owners remain for exact retry.
        if self._client is not None:
            try:
                await self._client.aclose()
            except BaseException as error:
                failures.append(self._cleanup_failure("http-client", error))
            else:
                self._client = None
        if self._provider_client is not None:
            try:
                await self._provider_client.aclose()
            except BaseException as error:
                failures.append(self._cleanup_failure("provider-client", error))
            else:
                self._provider_client = None

        self._token = None
        if failures:
            return StopReceipt(self._binding_token, False, tuple(failures))
        self._started = False
        self._stopping = False
        self._clear_presentations()
        return StopReceipt(self._binding_token, True)

    async def _gateway_loop(self) -> None:
        """Reconnect the external gateway until Core stops this exact binding."""

        while not self._stopped.is_set():
            try:
                token = await self._get_access_token()
                gateway = await self._api_request("GET", "/gateway", token=token)
                await self._run_gateway(str(gateway["url"]), token)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning("[qqbot] gateway 连接失败: %s", error)
                await asyncio.sleep(5)

    async def _run_gateway(self, url: str, token: str) -> None:
        last_seq: int | None = None
        heartbeat_task: asyncio.Task[None] | None = None
        try:
            async with websockets.connect(url) as websocket:
                async for raw in websocket:
                    payload = json.loads(raw)
                    if not isinstance(payload, dict):
                        logger.warning("[qqbot] 拒绝非 object gateway payload")
                        continue
                    raw_data = payload.get("d")
                    if not isinstance(raw_data, dict):
                        logger.warning("[qqbot] 拒绝非 object gateway data")
                        continue
                    data = cast(dict[str, Any], raw_data)
                    if isinstance(payload.get("s"), int):
                        last_seq = int(payload["s"])
                    if payload.get("op") == 10:
                        await websocket.send(
                            json.dumps(
                                {
                                    "op": 2,
                                    "d": {
                                        "token": f"QQBot {token}",
                                        "intents": 1 << 25,
                                        "shard": [0, 1],
                                    },
                                }
                            )
                        )
                        if heartbeat_task is not None:
                            heartbeat_task.cancel()
                            await asyncio.gather(heartbeat_task, return_exceptions=True)
                        heartbeat_task = asyncio.create_task(
                            self._heartbeat(
                                websocket,
                                int(data["heartbeat_interval"]),
                                lambda: last_seq,
                            )
                        )
                    elif payload.get("op") == 0:
                        await self._handle_dispatch(str(payload.get("t") or ""), data)
                    elif payload.get("op") == 7:
                        break
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)

    async def _heartbeat(
        self,
        websocket: Any,
        heartbeat_ms: int,
        sequence: Callable[[], int | None],
    ) -> None:
        while True:
            await asyncio.sleep(max(1, heartbeat_ms / 1000))
            await websocket.send(json.dumps({"op": 1, "d": sequence()}))

    async def _handle_dispatch(self, event_type: str, data: dict[str, Any]) -> None:
        if event_type == "C2C_MESSAGE_CREATE":
            await self._handle_c2c(data)
        elif event_type.startswith("GROUP_"):
            logger.debug("[qqbot] 当前仅启用私聊模式，忽略群事件 event=%s", event_type)

    async def _handle_c2c(self, data: dict[str, Any]) -> DeliveryStatus:
        if not isinstance(data, dict):
            logger.warning("[qqbot] 拒绝非 object 私聊 data")
            return DeliveryStatus.REJECTED
        raw_author = data.get("author")
        if raw_author is not None and not isinstance(raw_author, dict):
            logger.warning("[qqbot] 拒绝非 object 私聊 author")
            return DeliveryStatus.REJECTED
        author = raw_author if isinstance(raw_author, dict) else {}
        raw_openid = (
            author["user_openid"]
            if "user_openid" in author
            else data.get("user_openid")
        )
        if not isinstance(raw_openid, str):
            logger.warning("[qqbot] 拒绝非 string user_openid")
            return DeliveryStatus.REJECTED
        openid = raw_openid.strip()
        raw_message_id = data.get("id")
        raw_content = data.get("content")
        if not isinstance(raw_message_id, str):
            logger.warning("[qqbot] 拒绝非 string identity/message")
            return DeliveryStatus.REJECTED
        if raw_content is not None and not isinstance(raw_content, str):
            logger.warning("[qqbot] 拒绝非 string content")
            return DeliveryStatus.REJECTED
        message_id = raw_message_id.strip()
        content = raw_content.strip() if isinstance(raw_content, str) else ""
        if not openid or not message_id:
            logger.warning("[qqbot] 拒绝缺少 identity/message 的私聊事件")
            return DeliveryStatus.REJECTED
        if not self._allow_from or openid not in self._allow_from:
            logger.warning("[qqbot] 拒绝未授权私聊用户 user_openid=%s", openid)
            return DeliveryStatus.REJECTED
        try:
            provider_attachments = _provider_attachments(data)
            if provider_attachments is None:
                raise ValueError("QQBot attachments 字段格式非法")
            attachments = await self._import_provider_attachments(provider_attachments)
        except asyncio.CancelledError:
            raise
        except (httpx.HTTPStatusError, RuntimeError, TypeError, ValueError) as error:
            logger.warning(
                "[qqbot] 入站附件未能导入 message_id=%s err=%s",
                message_id,
                error,
            )
            return DeliveryStatus.REJECTED
        if not content and not attachments:
            logger.warning("[qqbot] 拒绝缺少 content/attachments 的私聊事件")
            return DeliveryStatus.REJECTED
        if not content:
            content = "[附件]"
        raw = RawInbound(
            message_id=message_id,
            message=ChannelInboundMessage(
                channel=_CHANNEL,
                sender=openid,
                chat_id=f"c2c:{openid}",
                content=content,
                timestamp=datetime.now(timezone.utc),
                metadata={
                    "chat_type": "private",
                    "user_openid": openid,
                    "message_id": message_id,
                },
                attachments=tuple(attachments),
            ),
            provider_identity=openid,
            recipient=f"c2c:{openid}",
        )
        if content == "/stop":
            presentation = self._require_presentation()
            control = presentation.control
            if control is None:
                raise RuntimeError("QQBot control port 未绑定")
            result = await control.interrupt(
                raw,
                response_bodies=ControlResponseBodies(
                    interrupted="已停止当前回复。",
                    idle="当前没有正在进行的回复。",
                ),
            )
            if result.response is None:
                return DeliveryStatus.REJECTED
            return result.response.status
        ingress = self._ingress
        if ingress is None:
            raise RuntimeError("QQBot ingress port 未绑定")
        if await ingress.admit(raw):
            self._message_recipients[message_id] = f"c2c:{openid}"
            self._message_identities[message_id] = openid
            return DeliveryStatus.DELIVERED
        return DeliveryStatus.REJECTED

    async def _import_provider_attachments(
        self,
        provider_attachments: list[Mapping[str, Any]],
    ) -> list[AttachmentRef]:
        """Download QQ media URLs and import every byte through Core."""

        if not provider_attachments:
            return []
        attachment_import = self._context.attachment_import
        if attachment_import is None:
            raise RuntimeError("QQBot 入站附件缺少 Core attachment_import")
        if self._client is None:
            raise RuntimeError("QQBot HTTP client 尚未 start")
        refs: list[AttachmentRef] = []
        for item in provider_attachments:
            url = item.get("url") or item.get("resolved_url")
            if not isinstance(url, str) or not url.startswith(("https://", "http://")):
                raise ValueError("QQBot 入站附件缺少安全下载 URL")
            declared_size = item.get("size")
            if isinstance(declared_size, int) and declared_size > _MAX_ATTACHMENT_BYTES:
                raise ValueError("QQBot 入站附件超过大小上限")
            response = await self._client.get(url)
            response.raise_for_status()
            data = await _bounded_response_bytes(response)
            filename = item.get("filename")
            filename = filename.strip() if isinstance(filename, str) and filename.strip() else "attachment"
            media_type = item.get("content_type")
            media_type = (
                media_type.strip()
                if isinstance(media_type, str) and media_type.strip()
                else mimetypes.guess_type(filename)[0] or "application/octet-stream"
            )
            kind = AttachmentKind.IMAGE if media_type.startswith("image/") else AttachmentKind.FILE
            ref = await attachment_import.import_bytes(
                data,
                kind=kind,
                filename=filename,
                media_type=media_type,
            )
            if not isinstance(ref, AttachmentRef):
                raise TypeError("QQBot attachment_import 必须返回 AttachmentRef")
            refs.append(ref)
        return refs

    async def _on_turn_stream(self, event: TurnStreamEvent) -> PresentationReceipt:
        """Project input notify and temporary stream without replacing final delivery."""

        if event.kind is TurnStreamEventKind.TURN_STARTED:
            payload = cast(TurnStartedPresentation, event.payload)
            recipient = self._message_recipients.pop(payload.client_message_id, None)
            provider_identity = self._message_identities.pop(
                payload.client_message_id,
                None,
            )
            if recipient is None and provider_identity is not None:
                identity = self._identity
                if identity is None:
                    raise RuntimeError("QQBot identity port 未绑定")
                recipient = identity.resolve(provider_identity)
            if recipient is None:
                self._live_disabled.add(event.presentation_id)
                return PresentationReceipt(
                    event.presentation_id,
                    DeliveryStatus.REJECTED,
                    error="QQBot turn 缺少 accepted provider message identity",
                )
            self._presentation_recipients[event.presentation_id] = recipient
            self._presentation_message_ids[event.presentation_id] = payload.client_message_id
            status, provider_id, error = await self._send_input_notify(
                recipient,
                payload.client_message_id,
            )
            if status is DeliveryStatus.UNKNOWN:
                self._live_uncertain.add(event.presentation_id)
                self._live_disabled.add(event.presentation_id)
            elif status is DeliveryStatus.REJECTED:
                self._live_disabled.add(event.presentation_id)
            return PresentationReceipt(
                event.presentation_id,
                status,
                (provider_id,) if provider_id else (),
                error,
            )
        if event.kind is TurnStreamEventKind.STREAM_DELTA:
            payload = cast(StreamDeltaPresentation, event.payload)
            reply = self._reply_buffers.get(event.presentation_id, "")
            self._reply_buffers[event.presentation_id] = reply + payload.text_delta
            return await self._refresh_preview(event.presentation_id)
        if event.kind is TurnStreamEventKind.TURN_OUTPUT_COMPLETED:
            _ = cast(TurnOutputCompletedPresentation, event.payload)
            return await self._finish_preview(event.presentation_id)
        return PresentationReceipt(event.presentation_id, DeliveryStatus.DELIVERED)

    async def _refresh_preview(self, presentation_id: str) -> PresentationReceipt:
        if presentation_id in self._live_uncertain:
            return PresentationReceipt(
                presentation_id,
                DeliveryStatus.UNKNOWN,
                error="QQBot preview 外部效果未确认，已停止后续 patch",
            )
        if presentation_id in self._live_disabled:
            return PresentationReceipt(
                presentation_id,
                DeliveryStatus.REJECTED,
                error="QQBot preview 已关闭",
            )
        recipient = self._presentation_recipients.get(presentation_id)
        text = _tail_text(self._reply_buffers.get(presentation_id, "").strip(), _REPLY_LIVE_TAIL)
        if recipient is None or not text:
            return PresentationReceipt(presentation_id, DeliveryStatus.DELIVERED)
        now = asyncio.get_running_loop().time()
        previous = self._live_last_lengths.get(presentation_id, 0)
        if (
            now < self._live_next_at.get(presentation_id, 0.0)
            and len(text) - previous < _LIVE_STREAM_MIN_CHARS
        ):
            return PresentationReceipt(presentation_id, DeliveryStatus.DELIVERED)
        self._live_next_at[presentation_id] = now + _LIVE_STREAM_MIN_INTERVAL_S
        self._live_last_lengths[presentation_id] = len(text)
        return await self._send_preview(presentation_id, recipient, text)

    async def _send_preview(
        self,
        presentation_id: str,
        recipient: str,
        text: str,
    ) -> PresentationReceipt:
        if presentation_id in self._live_uncertain:
            return PresentationReceipt(
                presentation_id,
                DeliveryStatus.UNKNOWN,
                error="QQBot preview 外部效果未确认，已停止后续 patch",
            )
        if presentation_id in self._live_disabled:
            return PresentationReceipt(
                presentation_id,
                DeliveryStatus.REJECTED,
                error="QQBot preview 已关闭",
            )
        _, openid = _parse_recipient(recipient)
        state = self._live_states.get(presentation_id)
        if state is None:
            message_id = self._presentation_message_ids.get(presentation_id)
            if message_id is None:
                raise RuntimeError(
                    f"QQBot preview 缺少 provider message id: {presentation_id}"
                )
            state = _LiveStreamState(openid, message_id, _next_msg_seq())
            self._live_states[presentation_id] = state
        lock = self._live_locks.setdefault(presentation_id, asyncio.Lock())
        async with lock:
            body: dict[str, Any] = {
                "input_mode": "replace",
                "input_state": 1,
                "content_type": "markdown",
                "content_raw": text,
                "event_id": state.msg_id,
                "msg_id": state.msg_id,
                "msg_seq": state.msg_seq,
                "index": state.index,
            }
            if state.stream_msg_id:
                body["stream_msg_id"] = state.stream_msg_id
            try:
                status, payload, error = await self._request_with_status(
                    "POST",
                    f"/v2/users/{openid}/stream_messages",
                    body,
                )
            except asyncio.CancelledError:
                self._live_uncertain.add(presentation_id)
                self._live_disabled.add(presentation_id)
                raise
            if status is DeliveryStatus.DELIVERED:
                remote_id = payload.get("id")
                if isinstance(remote_id, str) and remote_id.strip():
                    state.stream_msg_id = remote_id.strip()
                    state.index += 1
                    self._live_failures[presentation_id] = 0
                    self._live_uncertain.discard(presentation_id)
                else:
                    status = DeliveryStatus.UNKNOWN
                    error = (
                        "QQBot preview 2xx response 缺少 stream message id，"
                        "外部效果未确认"
                    )
                    self._live_uncertain.add(presentation_id)
            if status is DeliveryStatus.UNKNOWN:
                self._live_uncertain.add(presentation_id)
                self._live_disabled.add(presentation_id)
            elif status is DeliveryStatus.REJECTED:
                self._live_disabled.add(presentation_id)
            if status is not DeliveryStatus.DELIVERED:
                failures = self._live_failures.get(presentation_id, 0) + 1
                self._live_failures[presentation_id] = failures
            return PresentationReceipt(
                presentation_id,
                status,
                (state.stream_msg_id,) if state.stream_msg_id else (),
                error,
            )

    async def _finish_preview(self, presentation_id: str) -> PresentationReceipt:
        state = self._live_states.get(presentation_id)
        clear_state = True
        try:
            if presentation_id in self._live_uncertain:
                clear_state = False
                return PresentationReceipt(
                    presentation_id,
                    DeliveryStatus.UNKNOWN,
                    error="QQBot preview 外部效果未确认，保留本地失败状态",
                )
            if presentation_id in self._live_disabled and (
                state is None or not state.stream_msg_id
            ):
                return PresentationReceipt(
                    presentation_id,
                    DeliveryStatus.REJECTED,
                    error="QQBot preview 已拒绝，未产生可清理的远端消息",
                )
            if state is None:
                return PresentationReceipt(presentation_id, DeliveryStatus.DELIVERED)
            if not state.stream_msg_id:
                return PresentationReceipt(presentation_id, DeliveryStatus.DELIVERED)
            try:
                status, _payload, error = await self._request_with_status(
                    "DELETE",
                    f"/v2/users/{state.openid}/messages/{state.stream_msg_id}",
                )
            except asyncio.CancelledError:
                clear_state = False
                self._live_uncertain.add(presentation_id)
                self._live_disabled.add(presentation_id)
                raise
            if status is DeliveryStatus.UNKNOWN:
                clear_state = False
                self._live_uncertain.add(presentation_id)
                self._live_disabled.add(presentation_id)
            return PresentationReceipt(
                presentation_id,
                status,
                (state.stream_msg_id,),
                error,
            )
        finally:
            if clear_state:
                self._clear_presentation(presentation_id)

    async def _send_input_notify(
        self,
        recipient: str,
        message_id: str,
    ) -> tuple[DeliveryStatus, str | None, str | None]:
        _, openid = _parse_recipient(recipient)
        status, payload, error = await self._request_with_status(
            "POST",
            f"/v2/users/{openid}/messages",
            {
                "msg_type": 6,
                "input_notify": {"input_type": 1, "input_second": 60},
                "msg_seq": _next_msg_seq(),
                "msg_id": message_id,
            },
        )
        provider_id = str(payload.get("id") or "").strip() or None
        return status, provider_id, error

    async def _send_text(
        self,
        recipient: str,
        message: str,
    ) -> tuple[DeliveryStatus, str | None, str | None]:
        _, openid = _parse_recipient(recipient)
        status, payload, error = await self._request_with_status(
            "POST",
            f"/v2/users/{openid}/messages",
            {
                "markdown": {"content": message},
                "msg_type": 2,
                "msg_seq": _next_msg_seq(),
            },
        )
        provider_id = str(payload.get("id") or "").strip() or None
        if status is DeliveryStatus.DELIVERED and provider_id is None:
            return DeliveryStatus.UNKNOWN, None, "QQBot response 缺少 message id"
        return status, provider_id, error

    async def _request_with_status(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
    ) -> tuple[DeliveryStatus, dict[str, Any], str | None]:
        try:
            payload = await self._api_request(method, path, body)
        except asyncio.CancelledError:
            raise
        except httpx.HTTPStatusError as error:
            status = error.response.status_code
            delivery = (
                DeliveryStatus.REJECTED
                if status in _REJECTED_HTTP_STATUSES
                else DeliveryStatus.UNKNOWN
            )
            return delivery, {}, f"HTTP {status}"
        except Exception as error:
            return DeliveryStatus.UNKNOWN, {}, str(error) or type(error).__name__
        return DeliveryStatus.DELIVERED, payload, None

    async def _get_access_token(self) -> str:
        now = time.time()
        if self._token is not None and now < self._token.expires_at - 300:
            return self._token.token
        client = self._require_client()
        provider = self._provider_client
        if provider is None:
            raise RuntimeError("QQBot provider client 尚未创建")
        app_id = provider.credential(self._credential_ref("app_id"))
        client_secret = provider.credential(self._credential_ref("client_secret"))
        response = await client.post(
            _TOKEN_URL,
            json={"appId": app_id, "clientSecret": client_secret},
        )
        response.raise_for_status()
        data = response.json()
        token = str(data["access_token"])
        self._token = _TokenCache(token, now + int(data.get("expires_in") or 7200))
        return token

    async def _api_request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        token: str | None = None,
    ) -> dict[str, Any]:
        access_token = token or await self._get_access_token()
        kwargs: dict[str, Any] = {
            "headers": {
                "Authorization": f"QQBot {access_token}",
                "Content-Type": "application/json",
            }
        }
        if body is not None:
            kwargs["json"] = body
        response = await self._require_client().request(
            method,
            f"{_API_BASE}{path}",
            **kwargs,
        )
        response.raise_for_status()
        if not response.content:
            return {}
        payload = response.json()
        return cast(dict[str, Any], payload) if isinstance(payload, dict) else {}

    def _credential_ref(self, name: str) -> CredentialRef:
        matches = [
            ref
            for path, ref in self._credentials.items()
            if path in _CREDENTIAL_ALIASES[name]
        ]
        if len(matches) != 1:
            raise RuntimeError(
                f"QQBot credential {name} 必须恰好有一个 physical alias"
            )
        return matches[0]

    def _require_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("QQBot adapter 尚未 start")
        return self._client

    def _require_presentation(self) -> ChannelPresentationPorts:
        if self._presentation is None:
            raise RuntimeError("QQBot presentation ports 未绑定")
        return self._presentation

    def _cleanup_failure(self, resource: str, error: BaseException) -> ChannelCleanupFailure:
        return ChannelCleanupFailure(
            stage="channel-stop",
            plugin_id=_CHANNEL,
            generation_id=self._context.generation_id,
            binding_token=self._binding_token,
            resource=resource,
            error_type=type(error).__name__,
            message=str(error) or type(error).__name__,
            retry_action="retry_generation_cleanup",
        )

    def _clear_presentation(self, presentation_id: str) -> None:
        self._presentation_recipients.pop(presentation_id, None)
        self._presentation_message_ids.pop(presentation_id, None)
        self._reply_buffers.pop(presentation_id, None)
        self._live_states.pop(presentation_id, None)
        self._live_next_at.pop(presentation_id, None)
        self._live_last_lengths.pop(presentation_id, None)
        self._live_failures.pop(presentation_id, None)
        self._live_disabled.discard(presentation_id)
        self._live_uncertain.discard(presentation_id)
        self._live_locks.pop(presentation_id, None)

    def _clear_presentations(self) -> None:
        for presentation_id in tuple(self._presentation_recipients):
            self._clear_presentation(presentation_id)
        self._message_recipients.clear()
        self._message_identities.clear()


def _allow_from(config: Mapping[str, object]) -> frozenset[str]:
    aliases = [config[key] for key in ("allow_from", "allowFrom") if key in config]
    if len(aliases) > 1 and aliases[0] != aliases[1]:
        raise RuntimeError("QQBot allow_from/allowFrom 声明冲突")
    value = aliases[0] if aliases else ()
    if not isinstance(value, tuple) or any(not isinstance(item, str) for item in value):
        raise TypeError("QQBot allow_from 必须是字符串 tuple")
    return frozenset(item for item in value if item)


def _provider_attachments(data: Mapping[str, Any]) -> list[Mapping[str, Any]] | None:
    """Normalize QQ provider attachment objects without retaining provider paths."""

    raw = data.get("attachments")
    if raw is None:
        aliases: list[Mapping[str, Any]] = []
        for key in ("image", "file", "media"):
            value = data.get(key)
            if value in (None, "", [], {}):
                continue
            if not isinstance(value, Mapping):
                return None
            aliases.append(value)
        return aliases
    if not isinstance(raw, list):
        return None
    result: list[Mapping[str, Any]] = []
    for item in raw:
        if not isinstance(item, Mapping):
            return None
        result.append(item)
    return result


def _has_provider_attachments(data: Mapping[str, Any]) -> bool:
    """Return whether a provider payload carries a non-empty attachment list."""

    attachments = _provider_attachments(data)
    return attachments is None or bool(attachments)


def _parse_recipient(recipient: str) -> tuple[str, str]:
    value = recipient.strip()
    if value.startswith("qqbot:"):
        value = value[len("qqbot:") :]
    if not value:
        raise ValueError(f"无效的 QQBot recipient: {recipient!r}")
    if ":" not in value:
        return "c2c", value
    kind, target = value.split(":", 1)
    if kind != "c2c" or not target:
        raise ValueError(f"无效的 QQBot recipient: {recipient!r}")
    return kind, target


def _next_msg_seq() -> int:
    return int(time.time() * 1000) % 65536


def _tail_text(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return "..." + text[-(limit - 3) :]


async def _close_attachment_lease(lease: Any) -> None:
    """Finish attachment lease cleanup even when the caller is cancelled."""

    task = asyncio.create_task(lease.aclose(), name="qqbot-attachment-lease-close")
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
            continue
    if task.cancelled():
        raise asyncio.CancelledError
    result = task.result()
    if cancelled:
        raise asyncio.CancelledError
    return result


async def _bounded_response_bytes(response: Any) -> bytes:
    """Collect a provider response without exceeding the attachment memory bound."""

    chunks: list[bytes] = []
    total = 0
    aiter_bytes = getattr(response, "aiter_bytes", None)
    if callable(aiter_bytes):
        aiter_bytes = cast(Callable[[], AsyncIterable[bytes]], aiter_bytes)
        async for chunk in aiter_bytes():
            if not isinstance(chunk, bytes):
                raise TypeError("QQBot provider response chunk 必须是 bytes")
            total += len(chunk)
            if total > _MAX_ATTACHMENT_BYTES:
                raise ValueError("QQBot 入站附件超过大小上限")
            chunks.append(chunk)
        return b"".join(chunks)
    data = response.content
    if not isinstance(data, bytes):
        raise TypeError("QQBot provider response content 必须是 bytes")
    if len(data) > _MAX_ATTACHMENT_BYTES:
        raise ValueError("QQBot 入站附件超过大小上限")
    return data


async def _await_task_after_cancellation(task: asyncio.Task[Any]) -> Any:
    """Finish critical cleanup before restoring caller cancellation."""

    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
            continue
    result = task.result()
    if cancelled:
        raise asyncio.CancelledError
    return result
