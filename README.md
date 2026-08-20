# QQBot

Akashic 官方 QQBot 私聊渠道。插件已迁移到 pure v3 Channel API：

- candidate 只注册静态 channel definition，不解密凭证、不建立网络连接；
- formal generation 通过 Core 提供的 exact binding 处理入站、`/stop`、临时预览和最终文本投递；
- v3 adapter 支持 Core-owned 图片和文件附件：先 exact-ref/hash 校验并有界读取，再按文本、附件顺序走 QQ 富媒体上传；provider 效果使用 `DELIVERED`、`REJECTED`、`UNKNOWN` 三态。
- 入站附件通过 provider URL 下载并导入 Core artifact store，再一次性提交给 ingress；插件不读取 workspace 路径、不拥有附件持久化。
