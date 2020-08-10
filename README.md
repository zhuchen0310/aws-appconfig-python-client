# aws-appconfig-python-client
用户 aws appconfig 做配置中心, Python3 获取配置

## 安装


```shell script
python setup.py develop
```

## 使用

```python
from appconfig_client import AppConfigClient
client = AppConfigClient(application='kalo', environment='dev')
client.get_value(key="name", default_val="", configuration='TestConfig')
```

