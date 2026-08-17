from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from agent.plugin_composition.channels import (
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
    def resolve(self, provider_identity: str) -> str | None:
        _ = provider_identity
        return None


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
    def __init__(self) -> None:
        self.subscription: FakeSubscription | None = None

    def subscribe(self, callback) -> FakeSubscription:
        self.subscription = FakeSubscription(callback)
        return self.subscription


def _context(
    *,
    factory: FakeProviderFactory | None = None,
    ingress: FakeIngress | None = None,
    control: FakeControl | None = None,
    stream: FakeTurnStream | None = None,
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
        identity=FakeIdentity(),
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
async def test_attachment_is_rejected_before_provider_effect() -> None:
    factory = FakeProviderFactory()
    adapter = module.build_qqbot_channel(_context(factory=factory))
    attachment = AttachmentRef(
        artifact_id="artifact-1",
        kind=AttachmentKind.FILE,
        filename="a.txt",
        media_type="text/plain",
        size_bytes=1,
        sha256="0" * 64,
    )
    receipt = await adapter.deliver(
        ProviderDeliveryRequest(
            "binding-1",
            "delivery-1",
            "c2c:alice",
            "body",
            (attachment,),
        )
    )
    assert receipt.status is DeliveryStatus.REJECTED
    assert factory.create_calls == 0


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
