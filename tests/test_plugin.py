from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


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
