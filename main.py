import aiohttp
import asyncio
import re
import logging
from email.utils import parsedate_to_datetime
from lxml import etree

from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult, MessageChain
from astrbot.api.star import Context, Star, register
from astrbot.api import AstrBotConfig
import astrbot.api.message_components as Comp

from .data_handler import DataHandler
from .pic_handler import RssImageHandler
from .rss import RSSItem
from typing import List


@register(
    "astrbot_plugin_rss",
    "Soulter",
    "RSS订阅插件",
    "1.2.0",
    "https://github.com/Soulter/astrbot_plugin_rss",
)
class RssPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context)

        self.logger = logging.getLogger("astrbot")
        self.context = context
        self.config = config
        self.data_handler = DataHandler()

        # 提取scheme文件中的配置
        self.title_max_length = config.get("title_max_length")
        self.description_max_length = config.get("description_max_length")
        self.max_items_per_poll = config.get("max_items_per_poll")
        self.t2i = config.get("t2i")
        self.is_hide_url = config.get("is_hide_url")
        self.is_read_pic = config.get("pic_config").get("is_read_pic")
        self.is_adjust_pic = config.get("pic_config").get("is_adjust_pic")
        self.max_pic_item = config.get("pic_config").get("max_pic_item")
        self.is_compose = config.get("compose")

        self.pic_handler = RssImageHandler(self.is_adjust_pic)

        # 记录 (url, user) -> job_id 的映射，用于后续更新/删除
        self._job_ids: dict[str, str] = {}

    def _job_key(self, url: str, user: str) -> str:
        return f"{url}::{user}"

    @filter.on_astrbot_loaded()
    async def on_astrbot_loaded(self):
        """AstrBot 初始化完成后自动注册所有订阅的定时任务"""
        await self._refresh_all_jobs()

    async def terminate(self):
        """插件卸载时删除所有已注册的定时任务"""
        for job_id in list(self._job_ids.values()):
            try:
                await self.context.cron_manager.delete_job(job_id)
            except Exception as e:
                self.logger.warning(f"RSS 删除定时任务失败: {e}")
        self._job_ids.clear()
        self.logger.info("RSS 所有定时任务已清理")

    async def _refresh_all_jobs(self):
        """清除并重新注册所有订阅的定时任务"""
        # 先删除旧任务
        for job_id in list(self._job_ids.values()):
            try:
                await self.context.cron_manager.delete_job(job_id)
            except Exception:
                pass
        self._job_ids.clear()

        self.logger.info("刷新定时任务")
        for url, info in self.data_handler.data.items():
            if url in ("rsshub_endpoints", "settings"):
                continue
            for user, sub_info in info["subscribers"].items():
                await self._register_job(url, user, sub_info["cron_expr"])

    async def _register_job(self, url: str, user: str, cron_expr: str):
        """为单个订阅注册定时任务"""
        key = self._job_key(url, user)
        job = await self.context.cron_manager.add_basic_job(
            name=f"rss_{key}",
            cron_expression=cron_expr,
            handler=self.cron_task_callback,
            payload={"url": url, "user": user},
            persistent=False,
        )
        self._job_ids[key] = job.job_id
        self.logger.info(f"RSS 定时任务已注册: {url} - {user} (job_id={job.job_id})")

    async def _unregister_job(self, url: str, user: str):
        """删除单个订阅的定时任务"""
        key = self._job_key(url, user)
        job_id = self._job_ids.pop(key, None)
        if job_id:
            try:
                await self.context.cron_manager.delete_job(job_id)
                self.logger.info(f"RSS 定时任务已删除: {url} - {user}")
            except Exception as e:
                self.logger.warning(f"RSS 删除定时任务失败 ({url} - {user}): {e}")

    async def parse_channel_info(self, url):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        connector = aiohttp.TCPConnector(ssl=False)
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        try:
            async with aiohttp.ClientSession(
                trust_env=True,
                connector=connector,
                timeout=timeout,
                headers=headers,
            ) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        self.logger.error(f"rss: 无法正常打开站点 {url}")
                        return None
                    text = await resp.read()
                    return text
        except asyncio.TimeoutError:
            self.logger.error(f"rss: 请求站点 {url} 超时")
            return None
        except aiohttp.ClientError as e:
            self.logger.error(f"rss: 请求站点 {url} 网络错误: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"rss: 请求站点 {url} 发生未知错误: {str(e)}")
            return None

    async def cron_task_callback(self, url: str, user: str):
        """定时任务回调"""

        if url not in self.data_handler.data:
            return
        if user not in self.data_handler.data[url]["subscribers"]:
            return

        self.logger.info(f"RSS 定时任务触发: {url} - {user}")
        last_update = self.data_handler.data[url]["subscribers"][user]["last_update"]
        latest_link = self.data_handler.data[url]["subscribers"][user]["latest_link"]
        max_items_per_poll = self.max_items_per_poll
        # 拉取 RSS
        rss_items = await self.poll_rss(
            url,
            num=max_items_per_poll,
            after_timestamp=last_update,
            after_link=latest_link,
        )
        max_ts = last_update

        # 分解 unified_msg_origin
        platform_name, message_type, session_id = user.split(":", 2)

        # 分平台处理消息
        if platform_name == "aiocqhttp" and self.is_compose:
            nodes = []
            for item in rss_items:
                comps = await self._get_chain_components(item)
                node = Comp.Node(
                    uin=0,
                    name="Astrbot",
                    content=comps,
                )
                nodes.append(node)
                max_ts = max(max_ts, item.pubDate_timestamp)

            if len(nodes) > 0:
                msc = MessageChain(chain=nodes, use_t2i_=self.t2i)
                await self.context.send_message(user, msc)
        else:
            for item in rss_items:
                comps = await self._get_chain_components(item)
                msc = MessageChain(chain=comps, use_t2i_=self.t2i)
                await self.context.send_message(user, msc)
                max_ts = max(max_ts, item.pubDate_timestamp)

        # 更新最后更新时间和最新链接
        if rss_items:
            self.data_handler.data[url]["subscribers"][user]["last_update"] = max_ts
            self.data_handler.data[url]["subscribers"][user]["latest_link"] = rss_items[0].link
            self.data_handler.save_data()
            self.logger.info(f"RSS 定时任务 {url} 推送成功 - {user}")
        else:
            self.logger.info(f"RSS 定时任务 {url} 无消息更新 - {user}")

    async def poll_rss(
        self,
        url: str,
        num: int = -1,
        after_timestamp: int = 0,
        after_link: str = "",
    ) -> List[RSSItem]:
        """从站点拉取RSS信息"""
        text = await self.parse_channel_info(url)
        if text is None:
            self.logger.error(f"rss: 无法解析站点 {url} 的RSS信息")
            return []
        root = etree.fromstring(text)
        items = root.xpath("//item")

        cnt = 0
        rss_items = []

        for item in items:
            try:
                chan_title = (
                    self.data_handler.data[url]["info"]["title"]
                    if url in self.data_handler.data
                    else "未知频道"
                )

                title_nodes = item.xpath("title")
                title = title_nodes[0].text if title_nodes and title_nodes[0].text else "（无标题）"
                if len(title) > self.title_max_length:
                    title = title[: self.title_max_length] + "..."

                link_nodes = item.xpath("link")
                link = link_nodes[0].text if link_nodes and link_nodes[0].text else ""
                if link and not re.match(r"^https?://", link):
                    link = self.data_handler.get_root_url(url) + link

                desc_nodes = item.xpath("description")
                raw_description = desc_nodes[0].text if desc_nodes and desc_nodes[0].text else ""

                pic_url_list = self.data_handler.strip_html_pic(raw_description)
                description = self.data_handler.strip_html(raw_description)

                if len(description) > self.description_max_length:
                    description = description[: self.description_max_length] + "..."

                pub_date_nodes = item.xpath("pubDate")
                if pub_date_nodes and pub_date_nodes[0].text:
                    pub_date = pub_date_nodes[0].text
                    try:
                        pub_date_timestamp = int(parsedate_to_datetime(pub_date).timestamp())
                    except Exception:
                        pub_date_timestamp = 0

                    if pub_date_timestamp > after_timestamp:
                        rss_items.append(
                            RSSItem(
                                chan_title,
                                title,
                                link,
                                description,
                                pub_date,
                                pub_date_timestamp,
                                pic_url_list,
                            )
                        )
                        cnt += 1
                        if num != -1 and cnt >= num:
                            break
                    else:
                        break
                else:
                    # 根据 link 判断是否为新内容
                    if link != after_link:
                        rss_items.append(
                            RSSItem(chan_title, title, link, description, "", 0, pic_url_list)
                        )
                        cnt += 1
                        if num != -1 and cnt >= num:
                            break
                    else:
                        break

            except Exception as e:
                self.logger.error(f"rss: 解析Rss条目 {url} 失败: {str(e)}")
                break

        return rss_items

    def parse_rss_url(self, url: str) -> str:
        """解析RSS URL，确保以http或https开头"""
        if not re.match(r"^https?://", url):
            url = "https://" + url.lstrip("/")
        return url

    async def _add_url(self, url: str, cron_expr: str, message: AstrMessageEvent):
        """内部方法：添加URL订阅的共用逻辑"""
        user = message.unified_msg_origin
        if url in self.data_handler.data:
            latest_items = await self.poll_rss(url)
            last_update = latest_items[0].pubDate_timestamp if latest_items else 0
            latest_link = latest_items[0].link if latest_items else ""
            self.data_handler.data[url]["subscribers"][user] = {
                "cron_expr": cron_expr,
                "last_update": last_update,
                "latest_link": latest_link,
            }
        else:
            try:
                text = await self.parse_channel_info(url)
                if text is None:
                    return message.plain_result("无法连接到该 RSS 地址，请检查 URL 是否正确")
                title, desc = self.data_handler.parse_channel_text_info(text)
                latest_items = await self.poll_rss(url)
            except Exception as e:
                return message.plain_result(f"解析频道信息失败: {str(e)}")

            last_update = latest_items[0].pubDate_timestamp if latest_items else 0
            latest_link = latest_items[0].link if latest_items else ""
            self.data_handler.data[url] = {
                "subscribers": {
                    user: {
                        "cron_expr": cron_expr,
                        "last_update": last_update,
                        "latest_link": latest_link,
                    }
                },
                "info": {
                    "title": title,
                    "description": desc,
                },
            }
        self.data_handler.save_data()
        return self.data_handler.data[url]["info"]

    async def _get_chain_components(self, item: RSSItem):
        """组装消息链"""
        comps = []

        # 文本部分
        text_parts = [
            f"频道 {item.chan_title} 最新 Feed",
            f"标题: {item.title}"
        ]

        if not self.is_hide_url:
            text_parts.append(f"链接: {item.link}")

        if item.description:
            text_parts.append(item.description)

        comps.append(Comp.Plain("\n".join(text_parts) + "\n"))

        # 图片部分
        if self.is_read_pic and item.pic_urls:
            max_pic = len(item.pic_urls) if self.max_pic_item == -1 else self.max_pic_item

            for pic_url in item.pic_urls[:max_pic]:
                base64str = await self.pic_handler.modify_corner_pixel_to_base64(pic_url)

                if base64str:
                    comps.append(Comp.Image.fromBase64(base64str))
                else:
                    comps.append(Comp.Plain("图片链接读取失败\n"))

        return comps

    def _is_url_or_ip(self, text: str) -> bool:
        """判断一个字符串是否为网址（http/https 开头）或 IP 地址。"""
        url_pattern = r"^(?:http|https)://.+$"
        ip_pattern = r"^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
        return bool(re.match(url_pattern, text) or re.match(ip_pattern, text))

    @filter.command_group("rss", alias={"RSS"})
    def rss(self):
        """RSS订阅插件

        可以订阅和管理多个RSS源，支持cron表达式设置更新频率

        cron 表达式格式：
        * * * * *，分别表示分钟 小时 日 月 星期，* 表示任意值，支持范围和逗号分隔。例：
        1. 0 0 * * * 表示每天 0 点触发。
        2. 0/5 * * * * 表示每 5 分钟触发。
        3. 0 9-18 * * * 表示每天 9 点到 18 点触发。
        4. 0 0 1,15 * * 表示每月 1 号和 15 号 0 点触发。
        星期的取值范围是 0-6，0 表示星期天。
        """
        pass

    @rss.group("rsshub")
    def rsshub(self, event: AstrMessageEvent):
        """RSSHub相关操作

        可以添加、查看、删除RSSHub的端点
        """
        pass

    @rsshub.command("add")
    async def rsshub_add(self, event: AstrMessageEvent, url: str):
        """添加一个RSSHub端点

        Args:
            url: RSSHub服务器地址，例如：https://rsshub.app
        """
        if url.endswith("/"):
            url = url[:-1]
        if not self._is_url_or_ip(url):
            yield event.plain_result("请输入正确的URL")
            return
        elif url in self.data_handler.data["rsshub_endpoints"]:
            yield event.plain_result("该RSSHub端点已存在")
            return
        else:
            self.data_handler.data["rsshub_endpoints"].append(url)
            self.data_handler.save_data()
            yield event.plain_result("添加成功")

    @rsshub.command("list")
    async def rsshub_list(self, event: AstrMessageEvent):
        """列出所有已添加的RSSHub端点"""
        ret = "当前Bot添加的rsshub endpoint：\n"
        yield event.plain_result(
            ret
            + "\n".join(
                [
                    f"{i}: {x}"
                    for i, x in enumerate(self.data_handler.data["rsshub_endpoints"])
                ]
            )
        )

    @rsshub.command("remove")
    async def rsshub_remove(self, event: AstrMessageEvent, idx: int):
        """删除一个RSSHub端点

        Args:
            idx: 要删除的端点索引，可通过list命令查看
        """
        if idx < 0 or idx >= len(self.data_handler.data["rsshub_endpoints"]):
            yield event.plain_result("索引越界")
            return
        else:
            self.data_handler.data["rsshub_endpoints"].pop(idx)
            self.data_handler.save_data()
            yield event.plain_result("删除成功")

    @rss.command("add")
    async def add_command(
        self,
        event: AstrMessageEvent,
        idx: int,
        route: str,
        minute: str,
        hour: str,
        day: str,
        month: str,
        day_of_week: str,
    ):
        """通过RSSHub路由添加订阅

        Args:
            idx: RSSHub端点索引，可通过/rss rsshub list查看
            route: RSSHub路由，需以/开头
            minute: Cron表达式分钟字段
            hour: Cron表达式小时字段
            day: Cron表达式日期字段
            month: Cron表达式月份字段
            day_of_week: Cron表达式星期字段
        """
        if idx < 0 or idx >= len(self.data_handler.data["rsshub_endpoints"]):
            yield event.plain_result(
                "索引越界, 请使用 /rss rsshub list 查看已经添加的 rsshub endpoint"
            )
            return
        if not route.startswith("/"):
            yield event.plain_result("路由必须以 / 开头")
            return

        url = self.data_handler.data["rsshub_endpoints"][idx] + route
        cron_expr = f"{minute} {hour} {day} {month} {day_of_week}"

        ret = await self._add_url(url, cron_expr, event)
        if isinstance(ret, MessageEventResult):
            yield ret
            return
        else:
            chan_title = ret["title"]
            chan_desc = ret["description"]

        user = event.unified_msg_origin
        await self._unregister_job(url, user)
        await self._register_job(url, user, cron_expr)

        yield event.plain_result(
            f"添加成功。频道信息：\n标题: {chan_title}\n描述: {chan_desc}"
        )

    @rss.command("add-url")
    async def add_url_command(
        self,
        event: AstrMessageEvent,
        url: str,
        minute: str,
        hour: str,
        day: str,
        month: str,
        day_of_week: str,
    ):
        """直接通过Feed URL添加订阅

        Args:
            url: RSS Feed的完整URL
            minute: Cron表达式分钟字段
            hour: Cron表达式小时字段
            day: Cron表达式日期字段
            month: Cron表达式月份字段
            day_of_week: Cron表达式星期字段
        """
        url = self.parse_rss_url(url)
        cron_expr = f"{minute} {hour} {day} {month} {day_of_week}"
        ret = await self._add_url(url, cron_expr, event)
        if isinstance(ret, MessageEventResult):
            yield ret
            return
        else:
            chan_title = ret["title"]
            chan_desc = ret["description"]

        user = event.unified_msg_origin
        await self._unregister_job(url, user)
        await self._register_job(url, user, cron_expr)

        yield event.plain_result(
            f"添加成功。频道信息：\n标题: {chan_title}\n描述: {chan_desc}"
        )

    @rss.command("list")
    async def list_command(self, event: AstrMessageEvent):
        """列出当前所有订阅的RSS频道"""
        user = event.unified_msg_origin
        subs_urls = self.data_handler.get_subs_channel_url(user)
        if not subs_urls:
            yield event.plain_result("当前没有订阅任何频道")
            return
        lines = ["当前订阅的频道："]
        for cnt, url in enumerate(subs_urls):
            info = self.data_handler.data[url]["info"]
            sub_info = self.data_handler.data[url]["subscribers"][user]
            cron_expr = sub_info.get("cron_expr", "未知")
            lines.append(
                f"{cnt}. {info['title']}\n"
                f"   描述: {info['description']}\n"
                f"   定时: {cron_expr}\n"
                f"   URL: {url}"
            )
        yield event.plain_result("\n".join(lines))

    @rss.command("remove")
    async def remove_command(self, event: AstrMessageEvent, idx: int):
        """删除一个RSS订阅

        Args:
            idx: 要删除的订阅索引，可通过/rss list查看
        """
        user = event.unified_msg_origin
        subs_urls = self.data_handler.get_subs_channel_url(user)
        if idx < 0 or idx >= len(subs_urls):
            yield event.plain_result("索引越界, 请使用 /rss list 查看已经添加的订阅")
            return
        url = subs_urls[idx]
        self.data_handler.data[url]["subscribers"].pop(user)
        self.data_handler.save_data()

        await self._unregister_job(url, user)
        yield event.plain_result("删除成功")

    @rss.command("update")
    async def update_command(
        self,
        event: AstrMessageEvent,
        idx: int,
        minute: str,
        hour: str,
        day: str,
        month: str,
        day_of_week: str,
    ):
        """更新一个订阅的推送频率

        Args:
            idx: 要更新的订阅索引，可通过/rss list查看
            minute: 新的Cron表达式分钟字段
            hour: 新的Cron表达式小时字段
            day: 新的Cron表达式日期字段
            month: 新的Cron表达式月份字段
            day_of_week: 新的Cron表达式星期字段
        """
        user = event.unified_msg_origin
        subs_urls = self.data_handler.get_subs_channel_url(user)
        if idx < 0 or idx >= len(subs_urls):
            yield event.plain_result("索引越界, 请使用 /rss list 查看已经添加的订阅")
            return
        url = subs_urls[idx]
        new_cron = f"{minute} {hour} {day} {month} {day_of_week}"
        self.data_handler.data[url]["subscribers"][user]["cron_expr"] = new_cron
        self.data_handler.save_data()

        await self._unregister_job(url, user)
        await self._register_job(url, user, new_cron)

        chan_title = self.data_handler.data[url]["info"]["title"]
        yield event.plain_result(f"已将「{chan_title}」的推送频率更新为: {new_cron}")

    @rss.command("get")
    async def get_command(self, event: AstrMessageEvent, idx: int):
        """获取指定订阅的最新内容

        Args:
            idx: 要查看的订阅索引，可通过/rss list查看
        """
        subs_urls = self.data_handler.get_subs_channel_url(event.unified_msg_origin)
        if idx < 0 or idx >= len(subs_urls):
            yield event.plain_result("索引越界, 请使用 /rss list 查看已经添加的订阅")
            return
        url = subs_urls[idx]
        rss_items = await self.poll_rss(url)
        if not rss_items:
            yield event.plain_result("没有新的订阅内容")
            return
        item = rss_items[0]
        platform_name, message_type, session_id = event.unified_msg_origin.split(":", 2)
        comps = await self._get_chain_components(item)
        if platform_name == "aiocqhttp" and self.is_compose:
            node = Comp.Node(
                uin=0,
                name="Astrbot",
                content=comps,
            )
            yield event.chain_result([node]).use_t2i(self.t2i)
        else:
            yield event.chain_result(comps).use_t2i(self.t2i)
