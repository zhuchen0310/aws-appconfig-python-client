#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2020/8/10 17:39
import boto3
import ujson
import logging
import os
import threading
import time
import uuid


logger = logging.getLogger(__name__)


class AppConfigClient(object):
    """
    aws appconfig 连接器
    支持长轮询 和 本地缓存
    """
    _instance_lock = threading.Lock()
    _client_instance = {}

    def __init__(self, application: str='test', environment: str='dev', client_id: str=str(uuid.uuid4()),
                 cycle_time: int=60, cache_file_path=None):
        """
        :param application: 应用名称
        :param environment: 环境
        :param client_id: 自动生成的客户端id
        :param cycle_time: 轮训间隔(秒/S)
        :param cache_file_path: local cache file store path
        """
        self.client = boto3.client("appconfig")
        self.stopped = False
        self.application = application
        self.environment = environment
        self.client_id = client_id
        self._cycle_time = cycle_time
        self._stopping = False
        self._configuration_version = {"TestConfig": None}    # 首次访问 返回的版本号, 有值返回表示数据已更新
        self._cache = {}    # 本地缓存的数据 {"TestConfig": {"name": "test"}}
        if cache_file_path is None:
            self._cache_file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config')
        else:
            self._cache_file_path = cache_file_path
        self._path_checker()
        self.start()

    def __new__(cls, *args, **kwargs):
        # 单例
        service_key = f'{kwargs["application"]}:{kwargs["environment"]}'
        if service_key not in AppConfigClient._client_instance:
            with AppConfigClient._instance_lock:
                if service_key not in AppConfigClient._client_instance:
                    AppConfigClient._client_instance[service_key] = object.__new__(cls)
        return AppConfigClient._client_instance[service_key]

    def get_value(self, key="name", default_val=None, configuration='TestConfig'):
        """
        get the configuration value
        :param key:
        :param default_val: None
        :param configuration: 配置文件名称
        :return:
        """
        if configuration not in self._cache:
            self._cache[configuration] = {}
            self._configuration_version[configuration] = None
            logger.info(f"Add Configuration '{configuration}' to local cache")
            self._long_poll()

        if key in self._cache[configuration]:
            ret = self._cache[configuration][key]
        else:
            ret = default_val
        return ret

    def start(self, use_event_let=False, event_let_monkey_patch=False, catch_signals=True):
        """
        Start the long polling loop. Two modes are provided:
        1: thread mode (default), create a worker thread to do the loop. Call self.stop() to quit the loop
        2: event_let mode (recommended), no need to call the .stop() since it is async
        First do a blocking long poll to populate the local cache, otherwise we may get racing problems
        :param use_event_let:
        :param event_let_monkey_patch:
        :param catch_signals:
        :return:
        """
        if len(self._cache) == 0:
            self._long_poll()
        if use_event_let:
            import eventlet
            if event_let_monkey_patch:
                eventlet.monkey_patch()
            eventlet.spawn(self._listener)
        else:
            if catch_signals:
                import signal
                signal.signal(signal.SIGINT, self._signal_handler)
                signal.signal(signal.SIGTERM, self._signal_handler)
                signal.signal(signal.SIGABRT, self._signal_handler)
            t = threading.Thread(target=self._listener)
            t.setDaemon(True)
            t.start()

    def stop(self):
        """
        stop the client
        :return:
        """
        self._stopping = True
        logger.info("Stopping listener...")

    def _signal_handler(self):
        logger.info('You pressed Ctrl+C!')
        self._stopping = True

    def _path_checker(self):
        """
        create configuration cache file directory if not exits
        :return:
        """
        if not os.path.isdir(self._cache_file_path):
            os.mkdir(self._cache_file_path)

    def _update_local_cache(self, data, configuration='TestConfig'):
        """
        if local cache file exits, update the content
        if local cache file not exits, create a version
        :param data: new configuration content
        :param configuration::s
        :return:
        """
        new_string = ujson.dumps(data)
        cache_file_path = os.path.join(self._cache_file_path, f'{self.application}_configuration_{configuration}.txt')
        with open(cache_file_path, 'w') as f:
            f.write(new_string)

    def _get_local_cache(self, configuration='TestConfig'):
        """
        get configuration from local cache file
        if local cache file not exits than return empty dict
        :param configuration:
        :return:
        """
        cache_file_path = os.path.join(self._cache_file_path, f'{self.application}_configuration_{configuration}.txt')
        if os.path.isfile(cache_file_path):
            with open(cache_file_path, 'r') as f:
                result = ujson.loads(f.readline())
            return result
        return {}

    def _long_poll(self):
        """
        长链轮训
        只有在version 版本更改后才会更新数据
        :return:
        """
        for configuration, version in self._configuration_version.items():
            logger.debug(f'开始轮训: {self.application}:{self.environment}:{configuration}')
            try:
                if version:
                    response = self.client.get_configuration(
                        Application=self.application, Environment=self.environment, Configuration=configuration,
                        ClientId=self.client_id, ClientConfigurationVersion=version)
                else:
                    response = self.client.get_configuration(
                        Application=self.application, Environment=self.environment, Configuration=configuration,
                        ClientId=self.client_id)
                data = ujson.loads(response["Content"].read())
                client_configuration_version = response["ConfigurationVersion"]
                if not version:
                    self._configuration_version[configuration] = client_configuration_version
                    self._cache[configuration] = data
                    logger.info(f'Created local cache for configuration {configuration}')
                    self._update_local_cache(data, configuration)
                elif client_configuration_version != version:
                    self._configuration_version[configuration] = client_configuration_version
                    self._cache[configuration] = data
                    logger.info(f'Updated local cache for configuration {configuration}')
                    self._update_local_cache(data, configuration)
                if not data:
                    logger.warning(f"{self.application}:{self.environment}:{configuration}没有配置!!!")
                    continue
            except:
                continue
        self._load_local_cache_file()

    def _load_local_cache_file(self):
        """
        load all cached files from local path
        is only used while apollo server is unreachable
        :return:
        """
        for file in os.listdir(self._cache_file_path):
            file_path = os.path.join(self._cache_file_path, file)
            if os.path.isfile(file_path):
                configuration = file.split('.')[0].split('_')[-1]
                with open(file_path) as f:
                    self._cache[configuration] = ujson.loads(f.read())
        return True

    def _listener(self):
        """

        :return:
        """
        logger.info('Entering listener loop...')
        while not self._stopping:
            self._long_poll()
            time.sleep(self._cycle_time)

        logger.info("Listener stopped!")
        self.stopped = True
