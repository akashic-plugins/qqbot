from __future__ import annotations

from typing import Annotated

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from agent.plugin_composition import CredentialRef


class QQBotGroupConfig(BaseModel):
    """Preserve the existing group configuration while group events stay disabled."""

    model_config = ConfigDict(extra="forbid", validate_by_alias=True, validate_by_name=True)

    group_openid: str = Field(
        default="",
        validation_alias=AliasChoices("group_openid", "groupOpenid"),
    )
    allow_from: tuple[str, ...] = Field(
        default=(),
        validation_alias=AliasChoices("allow_from", "allowFrom"),
    )
    require_at: bool = Field(
        default=True,
        validation_alias=AliasChoices("require_at", "requireAt"),
    )
    allow_proactive: bool = Field(
        default=False,
        validation_alias=AliasChoices("allow_proactive", "allowProactive"),
    )


class QQBotConfig(BaseModel):
    """Validate QQBot's redacted Core config projection."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        validate_by_alias=True,
        validate_by_name=False,
    )

    app_id: Annotated[
        CredentialRef | None,
        Field(validation_alias=AliasChoices("appId", "app_id")),
    ] = None
    client_secret: Annotated[
        CredentialRef | None,
        Field(validation_alias=AliasChoices("clientSecret", "client_secret")),
    ] = None
    allow_from: tuple[str, ...] = Field(
        default=(),
        validation_alias=AliasChoices("allow_from", "allowFrom"),
    )
    groups: tuple[QQBotGroupConfig, ...] = ()
