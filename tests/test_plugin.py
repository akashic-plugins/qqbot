from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import httpx

from agent.plugin_composition.channels import (
    AttachmentKind,
    AttachmentRef,
    AttachmentKind,
    AttachmentRef,
    ChannelDeliveryReceipt,
    ChannelFactoryContext,
    ChannelPresentationPorts,
    ControlReceipt,
    CredentialRef,
    DeliveryStatus,
    PresentationReceipt,
    ProviderDeliveryReceipt,
    ProviderDeliveryRequest,
    RawInbound,
    StreamDeltaPresentation,
    TurnOutputCompletedPresentation,
    TurnStartedPresentation,
    TurnStreamEvent,
    TurnStreamEventKind,
)


ROOT = Path(__file__).parents[1]


def _load_plugin_module():
    spec = importlib.util.spec_from_file_location(
        "qqbot_v3_test_plugin",
        ROOT / "plugin.py",
        submodule_search_locations=[str(ROOT)],
    )
    if spec is None or spec.loader is None:
        raise ImportError("unable to load QQBot plugin")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


module = _load_plugin_module()


class FakeProviderClient:
    def __init__(self) -> None:
        self.closed = False
        self.requested: list[tuple[str, ...]] = []

    def credential(self, ref: CredentialRef) -> str:
        self.requested.append(ref.path)
        if ref.path == ("appId",):
            return "app"
        if ref.path == ("clientSecret",):
            return "secret"
        raise KeyError(ref.path)

    async def aclose(self) -> None:
        self.closed = True


class FakeProviderFactory:
    def __init__(self) -> None:
        self.client = FakeProviderClient()
        self.create_calls = 0
        self.received: dict[str, CredentialRef] | None = None

    async def create(self, credentials):
        self.create_calls += 1
        self.received = dict(credentials)
        return self.client

    async def aclose(self) -> None:
        return None


class FakeIngress:
    def __init__(self, accepted: bool = True) -> None:
        self.accepted = accepted
        self.raw: list[RawInbound] = []

    async def admit(self, raw: RawInbound) -> bool:
        self.raw.append(raw)
        return self.accepted


class FakeIdentity:
    def __init__(self, values: dict[str, str] | None = None) -> None:
        self.values = values or {}
        self.lookups: list[str] = []

    def resolve(self, provider_identity: str) -> str | None:
        self.lookups.append(provider_identity)
        return self.values.get(provider_identity)


class FakeControl:
    def __init__(self) -> None:
        self.raw: RawInbound | None = None

    async def interrupt(self, raw: RawInbound, *, response_bodies) -> ControlReceipt:
        self.raw = raw
        return ControlReceipt(
            accepted=True,
            reason="interrupted",
            response=ChannelDeliveryReceipt("control", DeliveryStatus.DELIVERED),
        )


class FakeSubscription:
    def __init__(self, callback) -> None:
        self.callback = callback
        self.admission_closed = False
        self.closed = False

    def close_admission(self) -> None:
        self.admission_closed = True

    async def await_quiescence(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True


class FakeTurnStream:
    def __init__(self, *, fail_subscribe: bool = False) -> None:
        self.subscription: FakeSubscription | None = None
        self.fail_subscribe = fail_subscribe

    def subscribe(self, callback) -> FakeSubscription:
        if self.fail_subscribe:
            raise RuntimeError("stream subscribe failed")
        self.subscription = FakeSubscription(callback)
        return self.subscription


class FakeAttachmentReadLease:
    def __init__(self, ref: AttachmentRef, data: bytes) -> None:
        self.ref = ref
        self.data = data
        self.closed = False
        self.max_bytes: int | None = None

    async def read_bytes(self, *, max_bytes: int) -> bytes:
        self.max_bytes = max_bytes
        if len(self.data) > max_bytes:
            raise ValueError("read exceeded bound")
        return self.data

    async def aclose(self) -> None:
        self.closed = True


class FakeAttachmentRead:
    def __init__(self, values: dict[str, tuple[AttachmentRef, bytes]] | None = None) -> None:
        self.values = values or {}
        self.leases: list[FakeAttachmentReadLease] = []

    async def acquire(self, ref: AttachmentRef) -> FakeAttachmentReadLease:
        actual_ref, data = self.values.get(ref.artifact_id, (ref, b""))
        lease = FakeAttachmentReadLease(actual_ref, data)
        self.leases.append(lease)
        return lease


class FakeAttachmentImport:
    def __init__(self) -> None:
        self.calls: list[tuple[bytes, AttachmentKind, str | None, str | None]] = []

    async def import_bytes(
        self,
        data: bytes,
        *,
        kind: AttachmentKind,
        filename: str | None,
        media_type: str | None,
    ) -> AttachmentRef:
        self.calls.append((data, kind, filename, media_type))
        return AttachmentRef(
            artifact_id=f"imported-{len(self.calls)}",
            kind=kind,
            filename=filename,
            media_type=media_type,
            size_bytes=len(data),
            sha256=hashlib.sha256(data).hexdigest(),
        )


def _context(
    *,
    factory: FakeProviderFactory | None = None,
    ingress: FakeIngress | None = None,
    identity: FakeIdentity | None = None,
    control: FakeControl | None = None,
    stream: FakeTurnStream | None = None,
    attachment_read: FakeAttachmentRead | None = None,
    attachment_import: FakeAttachmentImport | None = None,
    config: dict[str, object] | None = None,
) -> ChannelFactoryContext:
    return ChannelFactoryContext(
        snapshot_id="snapshot-1",
        generation_id="generation-1",
        binding_token="binding-1",
        config=config or {"allow_from": ("allowed",)},
        credentials={
            "appId": CredentialRef(("appId",)),
            "clientSecret": CredentialRef(("clientSecret",)),
        },
        provider_client_factory=factory or FakeProviderFactory(),
        ingress=ingress or FakeIngress(),
        identity=identity or FakeIdentity(),
        attachment_import=attachment_import or FakeAttachmentImport(),
        attachment_read=attachment_read or FakeAttachmentRead(),
        control=control or FakeControl(),
        turn_stream=stream or FakeTurnStream(),
    )


def test_plugin_is_pure_v3_and_declares_exact_channel() -> None:
    from agent.plugins.composable import ComposablePlugin
    from agent.plugins.static_manifest import load_static_plugin_manifest

    instance = ComposablePlugin.from_module(module)
    assert instance.api_version == 3
    assert not hasattr(module, "QQBotPlugin")
    manifest = load_static_plugin_manifest(ROOT)
    assert manifest.channel_credentials == (
        (
            "qqbot",
            ("appId", "app_id", "clientSecret", "client_secret"),
        ),
    )


def test_config_accepts_credential_refs_and_both_allowlist_aliases() -> None:
    from pydantic import ValidationError

    config = module.Config.model_validate(
        {
            "appId": CredentialRef(("appId",)),
            "clientSecret": CredentialRef(("clientSecret",)),
            "allowFrom": ["alice"],
        }
    )
    assert config.allow_from == ("alice",)
    with pytest.raises(ValidationError):
        module.Config.model_validate({"appId": "raw-secret"})


@pytest.mark.asyncio
async def test_apply_registers_exact_channel_definition() -> None:
    calls = []

    class Channels:
        async def register(self, ctx, definition) -> None:
            calls.append((ctx, definition))

    class Context:
        def require(self, key):
            assert key.name == "core.channels"
            return Channels()

    await module.apply(Context(), module.Config())
    definition = calls[0][1]
    assert definition.name == "qqbot"
    assert {item.value for item in definition.capabilities} == {
        "inbound",
        "outbound",
        "control",
        "turn_stream",
    }
    assert definition.factory_export == "build_qqbot_channel"


def test_candidate_factory_has_no_network_or_secret_effect() -> None:
    factory = FakeProviderFactory()
    adapter = module.build_qqbot_channel(_context(factory=factory))
    assert factory.create_calls == 0
    assert adapter._provider_client is None
    assert adapter._client is None
    assert adapter._gateway_task is None


@pytest.mark.asyncio
async def test_formal_start_delivery_and_stop_use_controlled_client() -> None:
    factory = FakeProviderFactory()
    stream = FakeTurnStream()
    adapter = module.build_qqbot_channel(_context(factory=factory, stream=stream))

    async def gateway() -> None:
        await adapter._stopped.wait()

    adapter._gateway_loop = gateway
    adapter.attach_presentation(ChannelPresentationPorts(FakeControl(), stream))
    ready = await adapter.start()
    assert not ready.admission_open
    assert factory.create_calls == 1

    async def sent(_recipient: str, _message: str):
        return DeliveryStatus.DELIVERED, "provider-1", None

    adapter._send_text = sent
    receipt = await adapter.deliver(
        ProviderDeliveryRequest("binding-1", "delivery-1", "c2c:alice", "hello")
    )
    assert receipt == ProviderDeliveryReceipt(
        "delivery-1",
        DeliveryStatus.DELIVERED,
        ("provider-1",),
    )
    stopped = await adapter.stop()
    assert stopped.resources_closed
    assert factory.client.closed
    assert stream.subscription is not None and stream.subscription.closed


@pytest.mark.asyncio
async def test_stop_cancellation_waits_for_internal_cleanup() -> None:
    release = asyncio.Event()
    cleanup_started = asyncio.Event()

    class BlockingSubscription(FakeSubscription):
        async def await_quiescence(self) -> None:
            cleanup_started.set()
            await release.wait()

    class BlockingTurnStream(FakeTurnStream):
        def subscribe(self, callback) -> BlockingSubscription:
            self.subscription = BlockingSubscription(callback)
            return self.subscription

    factory = FakeProviderFactory()
    stream = BlockingTurnStream()
    adapter = module.build_qqbot_channel(_context(factory=factory, stream=stream))

    async def gateway() -> None:
        await adapter._stopped.wait()

    adapter._gateway_loop = gateway
    adapter.attach_presentation(ChannelPresentationPorts(FakeControl(), stream))
    await adapter.start()

    stopping = asyncio.create_task(adapter.stop())
    await cleanup_started.wait()
    stopping.cancel()
    await asyncio.sleep(0)
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await stopping
    assert stream.subscription is not None and stream.subscription.closed
    assert factory.client.closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("credentials", "missing"),
    [
        ({"appId": CredentialRef(("appId",))}, "client_secret"),
        ({"clientSecret": CredentialRef(("clientSecret",))}, "app_id"),
    ],
)
async def test_formal_start_rejects_missing_credential_before_resources(
    credentials: dict[str, CredentialRef],
    missing: str,
) -> None:
    factory = FakeProviderFactory()
    stream = FakeTurnStream()
    context = replace(
        _context(factory=factory, stream=stream),
        credentials=credentials,
    )
    adapter = module.build_qqbot_channel(context)
    adapter.attach_presentation(ChannelPresentationPorts(FakeControl(), stream))

    with pytest.raises(RuntimeError, match=missing):
        await adapter.start()

    assert factory.create_calls == 0
    assert adapter._provider_client is None
    assert adapter._client is None
    assert adapter._gateway_task is None


@pytest.mark.asyncio
async def test_start_failure_closes_provider_resources_before_reraising() -> None:
    factory = FakeProviderFactory()
    stream = FakeTurnStream(fail_subscribe=True)
    adapter = module.build_qqbot_channel(_context(factory=factory, stream=stream))
    adapter.attach_presentation(ChannelPresentationPorts(FakeControl(), stream))

    with pytest.raises(RuntimeError, match="stream subscribe failed"):
        await adapter.start()

    assert factory.client.closed
    assert adapter._provider_client is None
    assert adapter._client is None
    assert adapter._gateway_task is None


@pytest.mark.asyncio
async def test_attachment_delivery_reads_exact_bytes_and_preserves_text_file_order() -> None:
    data = b"x"
    attachment = AttachmentRef(
        artifact_id="artifact-1",
        kind=AttachmentKind.FILE,
        filename="a.txt",
        media_type="text/plain",
        size_bytes=len(data),
        sha256=hashlib.sha256(data).hexdigest(),
    )
    read = FakeAttachmentRead({"artifact-1": (attachment, data)})
    adapter = module.build_qqbot_channel(_context(attachment_read=read))
    calls: list[tuple[str, dict[str, object]]] = []

    async def request(method: str, path: str, body: dict[str, object] | None = None):
        calls.append((path, body or {}))
        if path.endswith("/files"):
            return DeliveryStatus.DELIVERED, {"file_info": "file-info"}, None
        return DeliveryStatus.DELIVERED, {"id": f"provider-{len(calls)}"}, None

    adapter._request_with_status = request
    receipt = await adapter.deliver(
        ProviderDeliveryRequest(
            "binding-1",
            "delivery-1",
            "c2c:alice",
            "body",
            (attachment,),
        )
    )
    assert receipt.status is DeliveryStatus.DELIVERED
    assert [path for path, _body in calls] == [
        "/v2/users/alice/messages",
        "/v2/users/alice/files",
        "/v2/users/alice/messages",
    ]
    assert calls[1][1]["file_data"] == "eA=="
    assert calls[2][1] == {"msg_type": 7, "media": {"file_info": "file-info"}, "msg_seq": calls[2][1]["msg_seq"]}
    assert read.leases[0].max_bytes == 1
    assert read.leases[0].closed


@pytest.mark.asyncio
async def test_attachment_upload_failure_is_rejected_without_rich_media_message() -> None:
    data = b"x"
    attachment = AttachmentRef(
        artifact_id="artifact-failure",
        kind=AttachmentKind.FILE,
        filename="a.txt",
        media_type="text/plain",
        size_bytes=1,
        sha256=hashlib.sha256(data).hexdigest(),
    )
    adapter = module.build_qqbot_channel(
        _context(attachment_read=FakeAttachmentRead({"artifact-failure": (attachment, data)}))
    )
    paths: list[str] = []

    async def request(method: str, path: str, body: dict[str, object] | None = None):
        paths.append(path)
        if path.endswith("/files"):
            return DeliveryStatus.REJECTED, {}, "HTTP 400"
        return DeliveryStatus.DELIVERED, {"id": "text-id"}, None

    adapter._request_with_status = request
    receipt = await adapter.deliver(
        ProviderDeliveryRequest("binding-1", "delivery-failure", "c2c:alice", "", (attachment,))
    )
    assert receipt.status is DeliveryStatus.REJECTED
    assert paths == ["/v2/users/alice/files"]


@pytest.mark.asyncio
async def test_attachment_delivery_propagates_cancel_and_closes_read_lease() -> None:
    data = b"x"
    attachment = AttachmentRef(
        artifact_id="artifact-cancel",
        kind=AttachmentKind.FILE,
        filename="a.txt",
        media_type="text/plain",
        size_bytes=1,
        sha256=hashlib.sha256(data).hexdigest(),
    )
    read = FakeAttachmentRead({"artifact-cancel": (attachment, data)})
    adapter = module.build_qqbot_channel(
        _context(attachment_read=read)
    )

    async def request(method: str, path: str, body: dict[str, object] | None = None):
        raise asyncio.CancelledError

    adapter._request_with_status = request
    with pytest.raises(asyncio.CancelledError):
        await adapter.deliver(
            ProviderDeliveryRequest("binding-1", "delivery-cancel", "c2c:alice", "", (attachment,))
        )
    assert read.leases[0].closed


@pytest.mark.asyncio
async def test_delivery_after_prior_success_aggregates_later_rejection_as_unknown() -> None:
    data = b"x"
    attachment = AttachmentRef(
        artifact_id="aggregate-1",
        kind=AttachmentKind.FILE,
        filename="a.txt",
        media_type="text/plain",
        size_bytes=1,
        sha256=hashlib.sha256(data).hexdigest(),
    )
    adapter = module.build_qqbot_channel(
        _context(attachment_read=FakeAttachmentRead({"aggregate-1": (attachment, data)}))
    )

    async def read(_refs):
        return [(attachment, data)]

    async def send_text(_recipient, _message):
        return DeliveryStatus.DELIVERED, "text-id", None

    async def send_attachment(_recipient, _ref, _data):
        return DeliveryStatus.REJECTED, None, "HTTP 400"

    adapter._read_attachments = read
    adapter._send_text = send_text
    adapter._send_attachment = send_attachment
    receipt = await adapter.deliver(
        ProviderDeliveryRequest(
            "binding-1", "aggregate-delivery", "c2c:alice", "body", (attachment,)
        )
    )
    assert receipt.status is DeliveryStatus.UNKNOWN
    assert receipt.provider_ids == ("text-id",)


@pytest.mark.asyncio
async def test_outbound_attachment_count_limit_rejects_before_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(module.channel, "_MAX_ATTACHMENT_COUNT", 1)
    data = b"x"
    first = AttachmentRef(
        "limit-1", AttachmentKind.FILE, "a.txt", "text/plain", 1, hashlib.sha256(data).hexdigest()
    )
    second = AttachmentRef(
        "limit-2", AttachmentKind.FILE, "b.txt", "text/plain", 1, hashlib.sha256(data).hexdigest()
    )
    read = FakeAttachmentRead(
        {"limit-1": (first, data), "limit-2": (second, data)}
    )
    adapter = module.build_qqbot_channel(_context(attachment_read=read))
    receipt = await adapter.deliver(
        ProviderDeliveryRequest(
            "binding-1", "limit-delivery", "c2c:alice", "", (first, second)
        )
    )
    assert receipt.status is DeliveryStatus.REJECTED
    assert read.leases == []


@pytest.mark.asyncio
@pytest.mark.parametrize("recipient", ["group:group-1"])
async def test_invalid_recipient_returns_rejected_without_provider_effect(
    recipient: str,
) -> None:
    adapter = module.build_qqbot_channel(_context())
    called = False

    async def send(_recipient: str, _message: str):
        nonlocal called
        called = True
        return DeliveryStatus.DELIVERED, "provider-1", None

    adapter._send_text = send
    receipt = await adapter.deliver(
        ProviderDeliveryRequest("binding-1", "delivery-1", recipient, "body")
    )

    assert receipt.status is DeliveryStatus.REJECTED
    assert receipt.error is not None
    assert not called


@pytest.mark.asyncio
async def test_inbound_is_allowlisted_and_stop_uses_control_port() -> None:
    ingress = FakeIngress()
    control = FakeControl()
    stream = FakeTurnStream()
    adapter = module.build_qqbot_channel(
        _context(ingress=ingress, control=control, stream=stream)
    )
    adapter.attach_presentation(ChannelPresentationPorts(control, stream))
    adapter.open_admission()
    await adapter._handle_c2c(
        {"id": "msg-1", "content": "hello", "author": {"user_openid": "allowed"}}
    )
    assert ingress.raw[0].provider_identity == "allowed"
    assert ingress.raw[0].recipient == "c2c:allowed"

    await adapter._handle_c2c(
        {"id": "msg-2", "content": "/stop", "author": {"user_openid": "allowed"}}
    )
    assert control.raw is not None and control.raw.message_id == "msg-2"
    assert [raw.message_id for raw in ingress.raw] == ["msg-1"]

    await adapter._handle_c2c(
        {"id": "msg-3", "content": "blocked", "author": {"user_openid": "mallory"}}
    )
    assert [raw.message_id for raw in ingress.raw] == ["msg-1"]

    closed_ingress = FakeIngress()
    closed = module.build_qqbot_channel(
        _context(ingress=closed_ingress, config={"allow_from": ()})
    )
    await closed._handle_c2c(
        {"id": "msg-4", "content": "blocked", "author": {"user_openid": "allowed"}}
    )
    assert closed_ingress.raw == []


@pytest.mark.asyncio
async def test_inbound_attachment_downloads_and_imports_before_admission() -> None:
    ingress = FakeIngress()
    adapter = module.build_qqbot_channel(_context(ingress=ingress))
    adapter.open_admission()

    class Response:
        content = b"image-bytes"
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        async def aiter_bytes(self):
            yield self.content

    class Stream:
        async def __aenter__(self) -> Response:
            return Response()

        async def __aexit__(self, *_args) -> None:
            return None

    class Client:
        def stream(self, method: str, url: str, **kwargs) -> Stream:
            assert method == "GET"
            assert url == "https://multimedia.nt.qq.com.cn/image"
            assert kwargs["follow_redirects"] is False
            return Stream()

    adapter._client = Client()

    status = await adapter._handle_c2c(
        {
            "id": "media-1",
            "content": "image",
            "attachments": [{"url": "https://multimedia.nt.qq.com.cn/image"}],
            "author": {"user_openid": "allowed"},
        }
    )

    assert status is DeliveryStatus.DELIVERED
    assert len(ingress.raw) == 1
    assert ingress.raw[0].message.content == "image"
    assert ingress.raw[0].message.attachments[0].size_bytes == len(b"image-bytes")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "http://multimedia.nt.qq.com.cn/image",
        "https://evil.example/image",
        "https://multimedia.nt.qq.com.cn@127.0.0.1/image",
    ],
)
async def test_inbound_attachment_url_is_restricted_before_http_request(url: str) -> None:
    imported = FakeAttachmentImport()
    adapter = module.build_qqbot_channel(_context(attachment_import=imported))
    adapter.open_admission()

    class Client:
        async def get(self, *_args, **_kwargs):
            raise AssertionError("unsafe URL must not reach HTTP client")

    adapter._client = Client()
    status = await adapter._handle_c2c(
        {
            "id": "unsafe-url",
            "content": "image",
            "attachments": [{"url": url}],
            "author": {"user_openid": "allowed"},
        }
    )
    assert status is DeliveryStatus.REJECTED
    assert imported.calls == []


@pytest.mark.asyncio
async def test_inbound_redirect_and_batch_limit_create_no_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(module.channel, "_MAX_ATTACHMENT_BATCH_BYTES", 3)
    imported = FakeAttachmentImport()
    adapter = module.build_qqbot_channel(_context(attachment_import=imported))
    adapter.open_admission()

    class Response:
        content = b"xx"
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        async def aiter_bytes(self):
            yield self.content

    class Stream:
        async def __aenter__(self) -> Response:
            return Response()

        async def __aexit__(self, *_args) -> None:
            return None

    class Client:
        def stream(self, method: str, url: str, **kwargs) -> Stream:
            assert method == "GET"
            assert kwargs["follow_redirects"] is False
            return Stream()

    adapter._client = Client()
    status = await adapter._handle_c2c(
        {
            "id": "batch-limit",
            "content": "images",
            "attachments": [
                {"url": "https://multimedia.nt.qq.com.cn/one"},
                {"url": "https://multimedia.nt.qq.com.cn/two"},
            ],
            "author": {"user_openid": "allowed"},
        }
    )
    assert status is DeliveryStatus.REJECTED
    assert imported.calls == []


@pytest.mark.asyncio
async def test_stop_with_attachment_rejects_before_download_or_import() -> None:
    imported = FakeAttachmentImport()
    adapter = module.build_qqbot_channel(_context(attachment_import=imported))
    adapter.open_admission()

    class Client:
        async def get(self, *_args, **_kwargs):
            raise AssertionError("/stop with attachment must not download")

    adapter._client = Client()
    status = await adapter._handle_c2c(
        {
            "id": "stop-media",
            "content": "/stop",
            "attachments": [{"url": "https://multimedia.nt.qq.com.cn/media"}],
            "author": {"user_openid": "allowed"},
        }
    )
    assert status is DeliveryStatus.REJECTED
    assert imported.calls == []


@pytest.mark.asyncio
async def test_connection_setup_request_error_is_rejected_without_gateway_exception() -> None:
    adapter = module.build_qqbot_channel(_context())

    async def request(_method: str, _path: str, _body=None):
        raise httpx.ConnectError("connect failed")

    adapter._api_request = request
    status, payload, error = await adapter._request_with_status("POST", "/v2/users/alice/messages")
    assert status is DeliveryStatus.REJECTED
    assert payload == {}
    assert error == "connect failed"


@pytest.mark.asyncio
async def test_runtime_lifecycle_blocks_closed_dispatch_and_stop_drains_accepted_work() -> None:
    adapter = module.build_qqbot_channel(_context())
    adapter.attach_runtime(SimpleNamespace(binding_token="binding-1"))
    called = asyncio.Event()
    released = asyncio.Event()

    async def accepted(_data) -> DeliveryStatus:
        called.set()
        await released.wait()
        return DeliveryStatus.DELIVERED

    adapter._handle_c2c = accepted
    await adapter._handle_dispatch("C2C_MESSAGE_CREATE", {})
    assert not called.is_set()

    adapter.open_admission()
    await adapter._handle_dispatch("C2C_MESSAGE_CREATE", {})
    await called.wait()
    adapter.close_admission()
    stopping = asyncio.create_task(adapter.stop())
    await asyncio.sleep(0)
    assert not stopping.done()
    released.set()
    assert (await stopping).resources_closed


@pytest.mark.asyncio
async def test_opaque_recipient_path_segment_is_rejected_before_provider_effect() -> None:
    adapter = module.build_qqbot_channel(_context())
    receipt = await adapter.deliver(
        ProviderDeliveryRequest(
            "binding-1", "invalid-recipient", "c2c:alice/escape", "hello"
        )
    )
    assert receipt.status is DeliveryStatus.REJECTED


@pytest.mark.asyncio
async def test_turn_started_uses_core_identity_when_local_recipient_is_missing() -> None:
    identity = FakeIdentity({"allowed": "c2c:resolved"})
    adapter = module.build_qqbot_channel(_context(identity=identity))
    adapter._message_identities["msg-1"] = "allowed"

    async def notify(_recipient: str, _message_id: str):
        return DeliveryStatus.DELIVERED, None, None

    adapter._send_input_notify = notify
    receipt = await adapter._on_turn_stream(
        TurnStreamEvent(
            "preview:turn-identity",
            TurnStreamEventKind.TURN_STARTED,
            TurnStartedPresentation("turn-identity", "msg-1"),
        )
    )

    assert receipt.status is DeliveryStatus.DELIVERED
    assert identity.lookups == ["allowed"]
    assert adapter._presentation_recipients["preview:turn-identity"] == "c2c:resolved"


@pytest.mark.asyncio
async def test_turn_started_without_identity_closes_preview_as_rejected() -> None:
    adapter = module.build_qqbot_channel(_context())
    started = await adapter._on_turn_stream(
        TurnStreamEvent(
            "preview:turn-missing",
            TurnStreamEventKind.TURN_STARTED,
            TurnStartedPresentation("turn-missing", "unknown-message"),
        )
    )
    delta = await adapter._on_turn_stream(
        TurnStreamEvent(
            "preview:turn-missing",
            TurnStreamEventKind.STREAM_DELTA,
            StreamDeltaPresentation("turn-missing", 1, "answer", ""),
        )
    )
    completed = await adapter._on_turn_stream(
        TurnStreamEvent(
            "preview:turn-missing",
            TurnStreamEventKind.TURN_OUTPUT_COMPLETED,
            TurnOutputCompletedPresentation("turn-missing", 2),
        )
    )

    assert started.status is DeliveryStatus.REJECTED
    assert delta.status is DeliveryStatus.REJECTED
    assert completed.status is DeliveryStatus.REJECTED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"id": "msg-1", "content": "hello", "author": {"user_openid": 7}},
        {"id": "msg-2", "content": 7, "author": {"user_openid": "allowed"}},
        {"id": 7, "content": "hello", "author": {"user_openid": "allowed"}},
        {"id": "msg-4", "content": "hello", "author": []},
        {"id": "msg-5", "content": "hello", "user_openid": 7},
    ],
)
async def test_inbound_identity_and_payload_types_fail_closed(
    payload: dict[str, object],
) -> None:
    ingress = FakeIngress()
    adapter = module.build_qqbot_channel(_context(ingress=ingress))

    await adapter._handle_c2c(payload)

    assert ingress.raw == []


@pytest.mark.asyncio
async def test_turn_preview_never_substitutes_final_delivery() -> None:
    adapter = module.build_qqbot_channel(_context())
    adapter._message_recipients["msg-1"] = "c2c:allowed"
    calls: list[tuple[str, str]] = []

    async def request(method: str, path: str, body=None):
        calls.append((method, path))
        return DeliveryStatus.DELIVERED, {"id": f"provider-{len(calls)}"}, None

    adapter._request_with_status = request
    started = await adapter._on_turn_stream(
        TurnStreamEvent(
            "preview:turn-1",
            TurnStreamEventKind.TURN_STARTED,
            TurnStartedPresentation("turn-1", "msg-1"),
        )
    )
    delta = await adapter._on_turn_stream(
        TurnStreamEvent(
            "preview:turn-1",
            TurnStreamEventKind.STREAM_DELTA,
            StreamDeltaPresentation("turn-1", 1, "answer", ""),
        )
    )
    completed = await adapter._on_turn_stream(
        TurnStreamEvent(
            "preview:turn-1",
            TurnStreamEventKind.TURN_OUTPUT_COMPLETED,
            TurnOutputCompletedPresentation("turn-1", 2),
        )
    )

    assert started.status is DeliveryStatus.DELIVERED
    assert delta.status is DeliveryStatus.DELIVERED
    assert completed.status is DeliveryStatus.DELIVERED
    assert [method for method, _path in calls] == ["POST", "POST", "DELETE"]
    assert "preview:turn-1" not in adapter._live_states

    final_calls: list[str] = []

    async def final(_recipient: str, body: str):
        final_calls.append(body)
        return DeliveryStatus.DELIVERED, "final-provider", None

    adapter._send_text = final
    final_receipt = await adapter.deliver(
        ProviderDeliveryRequest("binding-1", "final-1", "c2c:allowed", "answer")
    )
    assert final_receipt.status is DeliveryStatus.DELIVERED
    assert final_calls == ["answer"]


@pytest.mark.asyncio
async def test_preview_missing_remote_id_is_unknown_and_not_deleted() -> None:
    adapter = module.build_qqbot_channel(_context())
    adapter._message_recipients["msg-1"] = "c2c:allowed"
    calls: list[tuple[str, str]] = []

    async def request(method: str, path: str, body=None):
        _ = body
        calls.append((method, path))
        return DeliveryStatus.DELIVERED, {}, None

    adapter._request_with_status = request
    started = await adapter._on_turn_stream(
        TurnStreamEvent(
            "preview:turn-1",
            TurnStreamEventKind.TURN_STARTED,
            TurnStartedPresentation("turn-1", "msg-1"),
        )
    )
    preview = await adapter._on_turn_stream(
        TurnStreamEvent(
            "preview:turn-1",
            TurnStreamEventKind.STREAM_DELTA,
            StreamDeltaPresentation("turn-1", 1, "x" * 120, ""),
        )
    )
    completed = await adapter._on_turn_stream(
        TurnStreamEvent(
            "preview:turn-1",
            TurnStreamEventKind.TURN_OUTPUT_COMPLETED,
            TurnOutputCompletedPresentation("turn-1", 2),
        )
    )

    assert started.status is DeliveryStatus.DELIVERED
    assert preview.status is DeliveryStatus.UNKNOWN
    assert completed.status is DeliveryStatus.UNKNOWN
    assert preview.error is not None and "缺少 stream message id" in preview.error
    assert [method for method, _path in calls] == ["POST", "POST"]
    assert "preview:turn-1" in adapter._live_states
    assert "preview:turn-1" in adapter._live_uncertain

    retry = await adapter._on_turn_stream(
        TurnStreamEvent(
            "preview:turn-1",
            TurnStreamEventKind.STREAM_DELTA,
            StreamDeltaPresentation("turn-1", 3, "retry", ""),
        )
    )
    assert retry.status is DeliveryStatus.UNKNOWN
    assert [method for method, _path in calls] == ["POST", "POST"]


@pytest.mark.asyncio
async def test_gateway_cancellation_reaps_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = module.build_qqbot_channel(_context())
    channel_module = sys.modules[type(adapter).__module__]
    heartbeat_started = asyncio.Event()
    heartbeat_cancelled = asyncio.Event()
    block_gateway = asyncio.Event()

    class WebSocket:
        def __init__(self) -> None:
            self.sent_ready = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self.sent_ready:
                self.sent_ready = True
                return json.dumps({"op": 10, "d": {"heartbeat_interval": 1000}})
            await block_gateway.wait()
            raise StopAsyncIteration

        async def send(self, _payload: str) -> None:
            return None

    async def heartbeat(*_args) -> None:
        heartbeat_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            heartbeat_cancelled.set()

    monkeypatch.setattr(channel_module.websockets, "connect", lambda _url: WebSocket())
    adapter._heartbeat = heartbeat
    gateway = asyncio.create_task(adapter._run_gateway("ws://test", "token"))
    await heartbeat_started.wait()
    gateway.cancel()
    with pytest.raises(asyncio.CancelledError):
        await gateway
    assert heartbeat_cancelled.is_set()


@pytest.mark.asyncio
async def test_provider_close_failure_is_retained_for_retry() -> None:
    class FlakyClient(FakeProviderClient):
        def __init__(self) -> None:
            super().__init__()
            self.attempts = 0

        async def aclose(self) -> None:
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("close failed")
            await super().aclose()

    factory = FakeProviderFactory()
    factory.client = FlakyClient()
    stream = FakeTurnStream()
    adapter = module.build_qqbot_channel(_context(factory=factory, stream=stream))

    async def gateway() -> None:
        await adapter._stopped.wait()

    adapter._gateway_loop = gateway
    adapter.attach_presentation(ChannelPresentationPorts(FakeControl(), stream))
    await adapter.start()
    first = await adapter.stop()
    assert not first.resources_closed
    assert any(item.resource == "provider-client" for item in first.failures)
    second = await adapter.stop()
    assert second.resources_closed
    assert factory.client.attempts == 2
