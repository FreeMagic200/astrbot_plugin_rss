# astrbot-plugin-rss

✨ Get Everything You Want to Know / 获取你想知道的一切。✨

支持通过 RSSHub 路由和直接 URL 订阅 RSS 源，并定时获取最新的 RSS 内容。

## 功能

- 添加、列出和删除 RSSHub endpoint
- 通过 RSSHub 路由订阅 RSS 源
- 直接通过 URL 订阅 RSS 源
- 列出所有订阅
- 删除订阅
- 获取最新一条订阅内容

## 指令描述

### RSSHub 相关指令

- `/rss rsshub add <url>`: 添加一个 RSSHub endpoint
- `/rss rsshub list`: 列出所有 RSSHub endpoint
- `/rss rsshub remove <idx>`: 删除一个 RSSHub endpoint

### 订阅相关指令

- `/rss add <idx> <route> <cron_expr>`: 通过 RSSHub 路由给当前会话的增加一条订阅
- `/rss add-url <url> <cron_expr>`: 给当前会话直接增加一条自定义的订阅
- `/rss list`: 列出当前会话的所有订阅
- `/rss remove <idx>`: 删除当前会话指定序号的订阅
- `/rss get <idx>`: 获取当前会话的指定序号中最新一条的订阅内容

## Cron 表达式教程

Cron 表达式格式：`* * * * *`，分别表示分钟、小时、日、月、星期，`*` 表示任意值，支持范围和逗号分隔。例：

1. `0 0 * * *` 表示每天 0 点触发。
2. `0/5 * * * *` 表示每 5 分钟触发。
3. `0 9-18 * * *` 表示每天 9 点到 18 点触发。
4. `0 0 1,15 * *` 表示每月 1 号和 15 号 0 点触发。

星期的取值范围是 0-6，0 表示星期天。

## 安装

参考 AstrBot 安装插件方式。

## 使用

### 从 RSSHub 订阅内容

首先使用指令 `/rss rsshub add https://rsshub.app` 添加官方 RSSHub 订阅站。

然后使用指令 `/rss rsshub list` 查看刚刚添加的订阅站。

官方维护了很多可用的路由，涵盖了 Telegram Channel、Bilibili、金融信息、高校官网信息等等。可参考 RSSHub 官方维护的路由：https://docs.rsshub.app/zh/routes/popular

找到自己想订阅的内容，根据其中的 Route、Example、Parameters 组装成最终的路由，如 `/cls/telegraph`（只需要路由名即可，不要加前面的 `https://rsshub.app` ）

然后使用指令 `/rss add 0 /cls/telegraph 0 * * * *` 订阅消息，每小时拉取一次。第一个 0 表示使用的是 list 中第 0 个 RSSHub 站。

> 鼓励自己搭建 RSSHub 订阅站。


### 从自定义链接订阅内容

你可以使用指令 `/rss add-url <url> <cron_expr>` 订阅。

如 `/rss add-url https://blog.lwl.lol/index.xml 0 * * * *`。

请注意目前仅支持 RSS 2.0 格式。

## 配置

~~插件成功启动后，配置文件位于 `data/astrbot_plugin_rss_data.json`。~~

原配置文件，现已根据文档更新，请在Astrbot的插件管理中进行设置。

### 基础配置

`title_max_length`

- **描述:** 推送消息的标题最大长度。
- **类型:** 整数 (`int`)
- **默认值:** `30`

`description_max_length`

- **描述:** 推送消息的描述内容最大长度。
- **类型:** 整数 (`int`)
- **默认值:** `500`

`t2i` (Text to Image)

- **描述:** 是否将文字内容转换为图片进行发送。
- **类型:** 布尔值 (`bool`)
- **提示：**订阅中的图片内容会丢失
- **默认值:** `false`

`max_items_per_poll`

- **描述:** 每次从数据源获取的最大条目数。
- **类型:** 整数 (`int`)
- **提示:** 设置为 `-1` 表示不限制获取的条目数。
- **默认值:** `3`

`is_hide_url`

- **描述:** 是否在推送的消息中隐藏原始链接。
- **类型:** 布尔值 (`bool`)
- **提示:** 如果设置为 `true`，推送的消息中将不会显示链接，这有助于解决因发送链接可能导致的风控问题。
- **默认值:** `false`

`compose`

- **描述:** （仅限qq）是否将消息合并，以转发的方式组合发送。
- **类型:** 布尔值 (`bool`)
- **提示:** 如果设置为 `true`，会以转发的方式组合发送（建议开启，以规避qq的消息频率检测）。
- **默认值:** `true`



### 图片配置 

本部分包含与图片处理相关的配置。

`pic_config.is_read_pic`

- **描述:** 是否自动读取 RSS 链接中的图片。
- **类型:** 布尔值 (`bool`)
- **提示:** 如果设置为 `true`，程序会自动尝试获取 RSS 链接中的图片。
- **默认值:** `false`

`pic_config.is_adjust_pic`

- **描述:** 是否对读取到的图片进行防和谐处理。
- **类型:** 布尔值 (`bool`)
- **提示:** 如果设置为 `true`，程序会在读取到的图片四个角的像素点上添加随机像素，以尝试规避和谐。
- **默认值:** `false`

`pic_config.max_pic_item`

- **描述:** 每次处理图片的最大条目数。
- **类型:** 整数 (`int`)
- **提示:** 设置为 `-1` 表示不限制每次转换的图片条目数。
- **默认值:** `3`



## Q&A

Q： #13 bot会重复发送同一条消息多次，次数会不停增加。

A： 更新rss配置时，由于AsyncIOScheduler没正常删除旧任务导致的，重启Astrbot可以解决，在接下来版本中计划修复该问题。



## 限制

由于 QQ 官方对主动消息限制较为严重，因此主动推送不支持 qqofficial 消息平台。

## 贡献

欢迎提交 issue 和 pull request 来帮助改进这个项目。
