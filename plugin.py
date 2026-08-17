from __future__ import annotations

from agent.plugin_composition import (
    CHANNELS,
    ChannelCapability,
    ChannelDefinition,
    Context,
    InboundIdentity,
    PluginChannels,
)

from .channel import QQBotAdapter, build_qqbot_channel
from .config import QQBotConfig


api_version = 3
name = "qqbot"
version = "3.0.0"
desc = "官方 QQBot 私聊 v3 channel adapter"
author = "Akashic"
inject = (CHANNELS,)
Config = QQBotConfig


async def apply(ctx: Context, config: QQBotConfig) -> None:
    """Register the immutable QQBot channel definition in the exact Root."""

    channels: PluginChannels = ctx.require(CHANNELS)
    await channels.register(
        ctx,
        ChannelDefinition(
            name="qqbot",
            capabilities=frozenset(
                {
                    ChannelCapability.INBOUND,
                    ChannelCapability.OUTBOUND,
                    ChannelCapability.CONTROL,
                    ChannelCapability.TURN_STREAM,
                }
            ),
            factory_export="build_qqbot_channel",
            inbound_identity=InboundIdentity.PROVIDER_MESSAGE_ID,
            credential_paths=(
                "appId",
                "app_id",
                "clientSecret",
                "client_secret",
            ),
        ),
    )


__all__ = [
    "Config",
    "QQBotAdapter",
    "api_version",
    "apply",
    "author",
    "build_qqbot_channel",
    "desc",
    "inject",
    "name",
    "version",
]
