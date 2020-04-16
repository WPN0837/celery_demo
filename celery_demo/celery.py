from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from django.conf import settings

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'celery_demo.settings')

app = Celery(
    'celery_demo',
)

# 从环境变量加载配置
app.config_from_envvar('DJANGO_SETTINGS_MODULE')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

from celery.loaders.app import AppLoader
