# QQBot

Akashic 官方 QQBot 私聊渠道。插件已迁移到 pure v3 Channel API：

- candidate 只注册静态 channel definition，不解密凭证、不建立网络连接；
- formal generation 通过 Core 提供的 exact binding 处理入站、`/stop`、临时预览和最终文本投递；
- 首批 v3 adapter 明确只支持文本，附件会在任何 provider 副作用前返回 `REJECTED`。
