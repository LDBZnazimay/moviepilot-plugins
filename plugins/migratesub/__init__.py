import json
from typing import Any, Dict, Optional, Tuple, List, Type
from sqlalchemy.ext.declarative import DeclarativeMeta
import datetime
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy.orm import Session
from threading import Event
import requests
from app.db import db_query, db_update
from app.db.models.subscribehistory import SubscribeHistory
from app.db.site_oper import SiteOper
from app.db.subscribe_oper import SubscribeOper
from app.db.models.site import Site
from app.db.models.subscribe import Subscribe
from app.plugins import _PluginBase
from app.core.config import settings
from app import schemas
from app.log import logger
from app.utils.http import RequestUtils


class SubscribeHistoryOper:
    """
    订阅历史表操作
    """

    @staticmethod
    @db_query
    def get_list_all(db: Session):
        result = db.query(SubscribeHistory).all()
        return list(result)

    @staticmethod
    @db_query
    def is_exists(
        db: Session,
        tmdbid: Optional[int] = None,
        doubanid: Optional[str] = None,
        season: Optional[int] = None,
    ):
        if tmdbid:
            if season:
                return (
                    db.query(SubscribeHistory)
                    .filter(
                        SubscribeHistory.tmdbid == tmdbid,
                        SubscribeHistory.season == season,
                    )
                    .first()
                )
            return (
                db.query(SubscribeHistory)
                .filter(SubscribeHistory.tmdbid == tmdbid)
                .first()
            )
        elif doubanid:
            return (
                db.query(SubscribeHistory)
                .filter(SubscribeHistory.doubanid == doubanid)
                .first()
            )
        return None


class SqlOper:
    @staticmethod
    @db_update
    def update_str_note_to_json(db: Session, Table):
        """
        将table中note字段的字符串转为json
        """
        # 检查Table是否存在note字段
        if not hasattr(Table, "note"):
            logger.debug(f"Table {Table.__name__} 不存在 'note' 字段")
            return

        # 查询所有 note 字段不为空且为字符串的记录
        filtered_records = (
            db.query(Table)
            .filter(
                Table.note.isnot(None),
                Table.note.startswith('"'),
            )
            .all()
        )
        logger.debug(f"filtered_records  len: {len(filtered_records)}")
        for record in filtered_records:
            # 更新订阅表中的note字段
            is_success, __data = MigrateSub.str_json_loads(record.note)
            if is_success:
                db.query(Table).filter(Table.id == record.id).update({"note": __data})

    def note_str_to_json(
        self,
        db: Session,
    ):
        """
        对多个表中的 note 字段进行更新转换
        """
        tables = [Subscribe, Site]
        for table in tables:
            self.update_str_note_to_json(db, table)


class MigrateSub(_PluginBase):
    # 插件名称
    plugin_name = "迁移订阅"
    # 插件描述
    plugin_desc = "迁移原MP的订阅配置到新MP"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/boeto/MoviePilot-Plugins/main/icons/MigrateSub.png"
    # 插件版本
    plugin_version = "0.0.8"
    # 插件作者
    plugin_author = "boeto"
    # 作者主页
    author_url = "https://github.com/boeto/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "migratesub_"
    # 加载顺序
    plugin_order = 6
    # 可使用的用户级别
    auth_level = 2

    # 退出事件
    _event = Event()

    # 私有属性
    _plugin_id = "MigrateSub"
    _msg_install = "请确保原MP已**安装并启用**此插件。如果原MP是V1版本，在安装插件后，原MP需要**重启一次**让API生效"

    _scheduler = None
    _subscribeoper: SubscribeOper
    _siteOper: SiteOper
    _sqlOper: SqlOper

    _migrate_from_url: str = ""
    _migrate_api_token: str = ""

    _enabled: bool = False
    _onlyonce: bool = False
    _is_with_sites: bool = False
    _is_with_sub_history: bool = False
    _is_with_fix_note_str_json: bool = False

    def init_plugin(self, config: dict[str, Any] | None = None):
        logger.debug(f"初始化插件 {self.plugin_name}: {config}")
        self.__setup(config)
        # if hasattr(settings, "VERSION_FLAG"):
        #     version = settings.VERSION_FLAG  # V2
        # else:
        #     version = "v1"

        # if version == "v2":
        #     self.setup_v2()
        # else:
        #     self.setup_v1()

    def __setup(self, config: dict[str, Any] | None = None):
        # 初始化逻辑
        self._subscribeoper = SubscribeOper()
        self._siteOper = SiteOper()
        self._sqlOper = SqlOper()

        if config:
            self._migrate_api_token = config.get("migrate_api_token", "")
            self._migrate_from_url = config.get("migrate_from_url", "")

            self._enabled = config.get("enabled", False)
            self._onlyonce = config.get("onlyonce", False)
            self._is_with_sites = config.get("is_with_sites", False)
            self._is_with_sub_history = config.get("is_with_sub_history", False)
            self._is_with_fix_note_str_json = config.get(
                "is_with_fix_note_str_json", False
            )

        # 停止现有任务
        self.stop_service()

        # 启动服务
        if self._enabled or self._onlyonce:
            if self._onlyonce:
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                logger.info(f"{self.plugin_name}服务启动, 立即运行一次")
                self._scheduler.add_job(
                    func=self.__start_migrate,
                    trigger="date",
                    run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ))
                    + datetime.timedelta(seconds=3),
                )

                if self._scheduler.get_jobs():
                    # 启动服务
                    self._scheduler.print_jobs()
                    self._scheduler.start()

            if self._onlyonce:
                # 关闭一次性开关
                self.__update_onlyonce(False)

    def __update_config(self):
        """
        更新配置
        """
        __config = {
            "migrate_api_token": self._migrate_api_token,
            "migrate_from_url": self._migrate_from_url.rstrip("/"),
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "is_with_sites": self._is_with_sites,
            "is_with_sub_history": self._is_with_sub_history,
            "is_with_fix_note_str_json": self._is_with_fix_note_str_json,
        }
        logger.debug(f"更新配置 {__config}")
        self.update_config(__config)

    def __start_migrate(self):
        """
        启动迁移
        """
        if self._is_with_fix_note_str_json:
            logger.info("将数据库表中 note 字符串转为json格式...")
            self._sqlOper.note_str_to_json(self._subscribeoper._db)
            # 关闭一次性开关
            self._is_with_fix_note_str_json = False
            self.__update_config()
            logger.info("转换完成，结束处理")
            return

        if not self._migrate_api_token:
            logger.error("未设置迁移Token，结束迁移")
            return
        if not self._migrate_from_url:
            logger.error("未设置迁移url，结束迁移")
            return

        self.__migrate_sub()

        logger.debug(f"self._is_with_sites:{self._is_with_sites}")
        if self._is_with_sites:
            self.__migrate_sites()

        logger.debug(f"self._is_with_sub_history:{self._is_with_sub_history}")
        if self._is_with_sub_history:
            self.__migrate_sub_history()

        logger.info("全部迁移结束")

    def __migrate_sub(self):
        logger.info("开始获取订阅 ...")
        ret_sub_list = self.__get_migrate_sub_list()

        if not ret_sub_list:
            logger.warn("没有从原MP获取到订阅列表，结束订阅迁移")
        else:
            logger.info("获取到原MP订阅列表，开始添加订阅")
            add_count = 0
            # deal_count = 0

            for item in ret_sub_list:
                # 新增订阅
                (isAdded, msg) = self.__add_sub(item)
                if isAdded:
                    add_count += 1
                logger.info(msg)

            logger.info("订阅迁移完成，共添加 %s 条" % add_count)

    def __migrate_sub_history(self):
        logger.info("开始获取订阅历史 ...")
        ret_sub_history = self.__get_migrate_sub_history()

        if not ret_sub_history:
            logger.warn("没有从原MP获取到订阅历史，结束订阅历史迁移")
            return
        else:
            logger.info("获取到原MP订阅历史，开始添加订阅历史")
            add_count = 0
            # deal_count = 0
            for item in ret_sub_history:
                # 新增订阅历史
                (isAdded, msg) = self.__add_sub_history(item)
                # deal_count += 1
                if isAdded:
                    add_count += 1
                logger.info(msg)

                # if deal_count == 20:
                #     break
            logger.info("订阅历史迁移完成，共添加 %s 条" % add_count)

        # 关闭一次性开关
        self._is_with_sub_history = False
        self.__update_config()

    def __migrate_sites(self):
        logger.info("开始获取站点管理 ...")
        ret_sites = self.__get_migrate_sites()
        logger.info("获取站点管理完成")

        if not ret_sites:
            logger.warn("没有从原MP到站点管理信息，结束站点管理迁移")
            return
        else:
            # 清空站点管理
            logger.info("重置新MP站点管理...")
            Site.reset(self._siteOper._db)

            site_count = 0
            # 新增站点
            for item in ret_sites:
                logger.info(f"开始迁移站点：{item.get('name')}")

                if "note" in item:
                    is_success, __data = MigrateSub.str_json_loads(item.get("note"))
                    if is_success:
                        item["note"] = __data
                    # item.pop("id")
                site = Site(**item)
                site.create(self._siteOper._db)
                site_count += 1

            logger.info("站点迁移完成，共添加 %s 条" % site_count)

        # 关闭一次性开关
        self._is_with_sites = False
        self.__update_config()

    def __update_onlyonce(self, enabled: bool):
        self._onlyonce = enabled
        self.__update_config()

    def setup_v2(self):
        # V2版本特有的初始化逻辑
        pass

    def setup_v1(self):
        # V1版本特有的初始化逻辑
        pass

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command():
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        """
        获取插件API
        [{
            "path": "/xx",
            "endpoint": self.xxx,
            "methods": ["GET", "POST"],
            "summary": "API说明"
        }]
        """
        return [
            {
                "path": "/sites",
                "endpoint": self.get_sites_list,
                "methods": ["GET"],
                "summary": "获取所有站点管理",
            },
            {
                "path": "/sub-history",
                "endpoint": self.get_sub_history,
                "methods": ["GET"],
                "summary": "获取所有订阅历史",
            },
        ]

    def _validate_token(self, migrate_api_token: str) -> Any:
        """
        验证 API 密钥
        """
        if migrate_api_token != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        return None

    def get_sub_history(self, migrate_api_token: str):
        """
        获取所有订阅历史
        """
        logger.debug("获取所有订阅历史...")
        validation_response = self._validate_token(migrate_api_token)
        if validation_response:
            return validation_response
        return SubscribeHistoryOper.get_list_all(self._subscribeoper._db)

    def get_sites_list(self, migrate_api_token: str):
        """
        获取所有站点列表
        """
        logger.debug("获取所有站点列表...")
        validation_response = self._validate_token(migrate_api_token)
        if validation_response:
            return validation_response
        return self._siteOper.list()

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "migrate_from_url",
                                            "label": "原MP地址: 例如 http://mp.com:3001",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "migrate_api_token",
                                            "label": "原MP API Token",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 6, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "is_with_sites",
                                            "label": "迁移站点管理一次",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "is_with_sub_history",
                                            "label": "迁移订阅历史一次",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "is_with_fix_note_str_json",
                                            "label": "字符串修正一次",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 6, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "onlyonce",
                                            "label": "立即运行一次",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                        },
                                        "content": [
                                            {
                                                "component": "span",
                                                "text": "将原MP迁移订阅到此MP，需要填写原MP Url地址和原MP API Token（当然你原MP肯定是需要同时运行）。开启插件并立即运行一次将会迁移订阅列表。是否需要开启“迁移订阅站点管理”选项，请认真阅读下面的说明。",
                                            }
                                        ],
                                    },
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                        },
                                        "content": [
                                            {
                                                "component": "span",
                                                "text": "迁移订阅站点管理”选项须知：不开启迁移站点管理，迁移的订阅中将不保留“订阅站点”选项。开启后会重置新MP“站点管理”中已存在的站点！会重置新MP“站点管理”中已存在的站点！会重置新MP“站点管理”中已存在的站点！这样才能匹配上订阅中的“订阅站点”选项。请按需开启",
                                            }
                                        ],
                                    },
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                        },
                                        "content": [
                                            {
                                                "component": "span",
                                                "text": f"新MP开启“迁移订阅历史/迁移订阅站点管理”选项：{self._msg_install}。不需要填写或开启其他选项。运行一次后关闭，如果需要再次运行，请手动开启",
                                            }
                                        ],
                                    },
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                        },
                                        "content": [
                                            {
                                                "component": "span",
                                                "text": "“字符串修正一次”选项：仅0.0.7版本前（不包含0.0.7）迁移过数据的用户，需要开启此选项立即运行一次，解决note字段未格式化转换的问题",
                                            }
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "is_with_sub_history": False,
            "is_with_sites": False,
            "is_with_fix_note_str_json": False,
            "migrate_from_url": "",
            "migrate_api_token": "",
        }

    def get_page(self):
        pass

    def stop_service(self):
        """
        停止服务
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as e:
            print(str(e))

    @staticmethod
    def str_json_loads(data: Any) -> tuple[bool, Any]:
        if isinstance(data, str):
            try:
                data_json = json.loads(data)
                return True, data_json
            except json.JSONDecodeError:
                logger.debug(f"{data} 是一个字符串，但不是有效的 JSON")
                return False, data
        else:
            return False, data

    def __add_sub(self, item: dict) -> tuple[bool, str]:
        """
        添加订阅
        """
        item_name = item.get("name", "")
        item_year = item.get("year", "")

        item_name_year = f"{item_name} ({item_year})"

        tmdbid = item.get("tmdbid", None)
        doubanid = str(item.get("doubanid", None))
        season = item.get("season", None)

        if not item_name:
            item_info_id = None
            if tmdbid or doubanid:
                item_info_id = tmdbid if tmdbid else doubanid
            if item_info_id is None:
                logger.debug(f"无法添加，没有获取到 item 信息：{item}")
                logger.error(
                    f"无法添加，没有找到 item 信息：name：{item_name}，tmdbid: {tmdbid} 或 doubanid: {doubanid}"
                )
                return (False, "缺少必要信息，无法添加订阅")

            logger.error(f"{item_info_id} 无法添加，没有获取到影片名字")
            return (False, f"{item_info_id} 无法添加，没有获取到影片名字")

        if not tmdbid and not doubanid:
            logger.error(f"{item_name_year} 无法添加，没有获取到tmdbid或doubanid")
            return (False, f"{item_name_year} 无法添加，没有获取到tmdbid或doubanid")

        is_sub_exists = self._subscribeoper.exists(
            tmdbid=tmdbid, doubanid=doubanid, season=season
        )

        # 去除Subscribe 没有的字段
        kwargs = {k: v for k, v in item.items() if hasattr(Subscribe, k)}

        # 未启用站点迁移则去掉订阅站点管理
        if "sites" in kwargs and not self._is_with_sites:
            kwargs.pop("sites", None)

        # 移除特定字段
        fields_to_remove = [
            "id",
        ]
        for _field in fields_to_remove:
            if _field in kwargs:
                kwargs.pop(_field, None)

        fields_to_json = [
            # "sites",
            "note",
        ]
        for _field in fields_to_json:
            if _field in kwargs:
                is_success, __data = MigrateSub.str_json_loads(kwargs.get(_field))
                if is_success:
                    kwargs[_field] = __data

        if not is_sub_exists:
            logger.info(f"{item_name_year} 订阅不存在，开始添加订阅")
            sub = Subscribe(
                **kwargs,
            )
            sub.create(self._subscribeoper._db)
            return (True, f"{item_name_year}  添加订阅成功")
        else:
            logger.info(f"{item_name_year} 订阅已存在")
            return (False, "订阅已存在，跳过")

    def __add_sub_history(self, item: dict):
        """
        添加完成订阅历史
        """
        item_name_year = f"{item.get('name', '')} ({item.get('year', '')})"
        is_sub_history_exists = SubscribeHistoryOper.is_exists(
            self._subscribeoper._db,
            tmdbid=item.get("tmdbid", None),
            doubanid=item.get("doubanid", None),
            season=item.get("season", None),
        )
        if is_sub_history_exists:
            return (False, f"{item_name_year} 订阅历史已存在，跳过")
        else:
            # 去除kwargs中 SubscribeHistory 没有的字段
            kwargs = {k: v for k, v in item.items() if hasattr(SubscribeHistory, k)}

            # 去掉主键
            if "id" in kwargs:
                kwargs.pop("id", None)

            # 未启用站点迁移则去掉订阅站点管理
            if "sites" in kwargs:
                if not self._is_with_sites:
                    kwargs.pop("sites", None)
                else:
                    # 将字符串转为json
                    is_success, __data = MigrateSub.str_json_loads(item.get("sites"))
                    if is_success:
                        kwargs["sites"] = __data

            subHistory = SubscribeHistory(**kwargs)
            subHistory.create(self._subscribeoper._db)

            return (True, f"{item_name_year}  添加订阅历史成功")

    def __get_migrate_plugin_api_url(self, endpoint: str) -> str:
        """
        获取插件API URL
        """
        return f"{self._migrate_from_url}/api/v1/plugin/{self._plugin_id}/{endpoint}?migrate_api_token={self._migrate_api_token}"

    def __get_migrate_endpoint_api_url(self, endpoint: str):
        """
        获取插件API URL
        """
        return f"{self._migrate_from_url}/api/v1/{endpoint}?token={self._migrate_api_token}"

    def __get_migrate_info(self, migrate_url: str):
        """
        从原MP API URL获取信息
        """
        logger.info(f"开始从原MP获取数据，【请求URL】：{migrate_url}")

        try:
            res = RequestUtils().request(method="get", url=migrate_url)
            if not res:
                logger.error(
                    "没有获取到原MP信息，检查原MP地址和API Token是否正确，检查浏览器打开【请求URL】查看是能获取到数据"
                )
                if self._is_with_sites or self._is_with_sub_history:
                    logger.error(f"{self._msg_install}")
                return None
            res.raise_for_status()  # 检查响应状态码，如果不是 2xx，会抛出 HTTPError 异常
            resData = res.json()

            if isinstance(resData, dict):
                if resData.get("success", "") is False:
                    logger.error(f"获取原MP信息失败：{resData.get('message','')}")
                    return None

                if resData.get("detail", "") == "Not Found":
                    logger.error("请检查【请求URL】是否能获取到数据")
                    if self._is_with_sites or self._is_with_sub_history:
                        logger.error(f"{self._msg_install}")
                    return

            if isinstance(resData, list) and len(resData) == 0:
                logger.info(f"没有需要添加的迁移信息：{resData}")
                return

            return resData
        except requests.exceptions.RequestException as err:
            logger.error(f"请求错误发生: {err}")  # 打印所有请求错误
        return None

    def __get_migrate_sub_list(self):
        """
        获取订阅列表
        """
        url = self.__get_migrate_endpoint_api_url("subscribe/list")
        return self.__get_migrate_info(url)

    def __get_migrate_sites(self):
        """
        获取所有站点列表
        """
        url = self.__get_migrate_plugin_api_url("sites")
        return self.__get_migrate_info(url)

    def __get_migrate_sub_history(self):
        """
        获取订阅历史
        """
        url = self.__get_migrate_plugin_api_url("sub-history")
        return self.__get_migrate_info(url)
