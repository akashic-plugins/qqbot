from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, cast

import pytest

from agent.plugins import channel_generation_host
from agent.plugins.manager import PluginManager
from bus.event_bus import EventBus


ROOT = Path(__file__).parents[1]


class FakeProviderClient:
    def __init__(self) -> None:
        self.closed = 0

    def credential(self, ref: Any) -> str:
        if ref.path == ("appId",):
            return "formal-app-id"
        if ref.path == ("clientSecret",):
            return "formal-client-secret"
        raise KeyError(ref.path)

    async def aclose(self) -> None:
        self.closed += 1


class FakeProviderFactory:
    def __init__(self) -> None:
        self.client = FakeProviderClient()
        self.create_calls = 0
        self.close_calls = 0

    async def create(self, credentials: object) -> FakeProviderClient:
        del credentials
        self.create_calls += 1
        return self.client

    async def aclose(self) -> None:
        self.close_calls += 1


def _stage(tmp_path: Path) -> tuple[Path, Path]:
    plugin_root = tmp_path / "plugins" / "qqbot"
    plugin_root.mkdir(parents=True)
    for filename in (
        "plugin.py",
        "channel.py",
        "config.py",
        "akashic.plugin.toml",
    ):
        shutil.copy2(ROOT / filename, plugin_root / filename)
    workspace = tmp_path / "workspace"
    data_dir = workspace / "plugin-data" / "qqbot-builtin"
    data_dir.mkdir(parents=True)
    (data_dir / "config.local.toml").write_text(
        'appId = "formal-app-id"\n'
        'clientSecret = "formal-client-secret"\n'
        'allowFrom = ["allowed"]\n',
        encoding="utf-8",
    )
    return plugin_root, workspace


@pytest.mark.asyncio
async def test_manager_formal_candidate_discard_promote_and_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise QQBot through the real Manager and Channel Host without network."""

    # 1. Stage one static v3 artifact and replace only its external gateway loop.
    plugin_root, workspace = _stage(tmp_path)
    factory = FakeProviderFactory()
    original_resolver = channel_generation_host._resolve_sync_factory

    def resolve_factory(module: object, export: str) -> object:
        factory_callable = original_resolver(module, export)

        def wrapped(context: object) -> object:
            adapter = cast(Any, factory_callable(context))
            adapter._gateway_loop = lambda: adapter._stopped.wait()
            return adapter

        return wrapped

    monkeypatch.setattr(
        channel_generation_host,
        "_resolve_sync_factory",
        resolve_factory,
    )
    manager = PluginManager(
        plugin_dirs=[plugin_root.parent],
        event_bus=EventBus(),
        tool_registry=None,
        workspace=workspace,
        installed_cache_root=tmp_path / "home" / "cache",
    )
    manager.bind_channel_provider_factory_resolver(lambda snapshot: {"qqbot": factory})

    # 2. Formal boot owns one provider; candidate stays inert and secret-free.
    await manager.load_all()
    stable = manager.current_snapshot
    runtime = manager.active_channel_generation
    assert stable is not None and stable.state == "committed"
    assert runtime is not None and runtime.channel("qqbot").admission_open
    assert factory.create_calls == 1

    candidate = await manager.prepare_candidate("qqbot")
    assert candidate is not None and candidate.runtime_snapshot is not None
    assert manager.current_snapshot is stable
    assert factory.create_calls == 1
    assert candidate.validation_workspace is not None
    validation_root = candidate.validation_workspace.parent
    for path in validation_root.rglob("*"):
        if path.is_file() and not path.is_symlink():
            assert b"formal-client-secret" not in path.read_bytes()
    config_path = workspace / "plugin-data" / "qqbot-builtin" / "config.local.toml"
    original_config = config_path.read_bytes()
    await manager.discard_prepared("qqbot")
    assert manager.current_snapshot is stable
    assert factory.create_calls == 1
    assert config_path.read_bytes() == original_config

    # 3. Promotion rebuilds a formal binding; terminate drains every owned resource.
    candidate = await manager.prepare_candidate("qqbot")
    assert candidate is not None
    publication = await manager.publish_prepared("qqbot")
    assert publication["publication_state"] == "committed"
    assert manager.current_snapshot is not stable
    assert factory.create_calls == 2
    assert manager.active_channel_generation is not None
    assert manager.active_channel_generation.channel("qqbot").admission_open

    await manager.terminate_all()
    assert manager.active_channel_generation is None
    assert factory.close_calls == 2
    assert factory.client.closed == 2
