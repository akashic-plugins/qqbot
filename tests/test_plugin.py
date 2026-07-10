from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_plugin_module():
    path = Path(__file__).parents[1] / "plugin.py"
    spec = importlib.util.spec_from_file_location(
        "test_qqbot_plugin",
        path,
        submodule_search_locations=[str(path.parent)],
    )
    if spec is None or spec.loader is None:
        raise ImportError(str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


module = _load_plugin_module()
QQBotConfigModel = module.QQBotConfigModel
QQBotPlugin = module.QQBotPlugin


def test_qqbot_plugin_without_config_returns_no_channels() -> None:
    plugin = QQBotPlugin()
    plugin.context = type("Ctx", (), {"config": None})()
    assert plugin.channels() == []


def test_qqbot_plugin_with_config_returns_channel() -> None:
    plugin = QQBotPlugin()
    plugin.context = type(
        "Ctx",
        (),
        {
            "config": QQBotConfigModel(
                app_id="app",
                client_secret="secret",
                allow_from=[],
                groups=[],
            )
        },
    )()
    assert len(plugin.channels()) == 1


@pytest.mark.asyncio
async def test_gateway_cancellation_reaps_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plugin = QQBotPlugin()
    plugin.context = type(
        "Ctx",
        (),
        {"config": QQBotConfigModel(app_id="app", client_secret="secret")},
    )()
    channel = plugin.channels()[0]
    channel_module = sys.modules[type(channel).__module__]
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
                return json.dumps(
                    {"op": 10, "d": {"heartbeat_interval": 1000}}
                )
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
    channel._heartbeat = heartbeat
    gateway = asyncio.create_task(channel._run_gateway("ws://test", "token"))
    await heartbeat_started.wait()
    gateway.cancel()

    with pytest.raises(asyncio.CancelledError):
        await gateway
    assert heartbeat_cancelled.is_set()


@pytest.mark.asyncio
async def test_channel_can_start_stop_twice() -> None:
    plugin = QQBotPlugin()
    plugin.context = type(
        "Ctx",
        (),
        {"config": QQBotConfigModel(app_id="app", client_secret="secret")},
    )()
    channel = plugin.channels()[0]
    starts = 0

    async def gateway_loop() -> None:
        nonlocal starts
        starts += 1
        await asyncio.Event().wait()

    channel._gateway_loop = gateway_loop
    registry = SimpleNamespace(
        on=lambda *_args: object(),
        register_channel=lambda *_args, **_kwargs: object(),
        subscribe_outbound=lambda *_args: object(),
    )
    context = SimpleNamespace(
        bus=registry,
        event_bus=registry,
        push_tool=registry,
        interrupt_controller=None,
    )

    await channel.start(context)
    await asyncio.sleep(0)
    await channel.stop()
    await channel.start(context)
    await asyncio.sleep(0)
    await channel.stop()

    assert starts == 2
    assert channel._task is None
