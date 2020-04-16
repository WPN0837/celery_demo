import socket
import time
import threading
from celery import platforms
from celery.schedules import crontab
from celery.apps.beat import Beat
from celery.utils.log import get_logger
from celery_demo import celery_app

logger = get_logger('celery.beat')
flag = False


class MyBeat(Beat):
    '''
    继承Beat 添加一个获取service的方法
    '''
    def start_scheduler(self):
        if self.pidfile:
            platforms.create_pidlock(self.pidfile)
        # 修改了获取service的方式
        service = self.get_service()

        print(self.banner(service))

        self.setup_logging()
        if self.socket_timeout:
            logger.debug('Setting default socket timeout to %r',
                         self.socket_timeout)
            socket.setdefaulttimeout(self.socket_timeout)
        try:
            self.install_sync_handler(service)
            service.start()
        except Exception as exc:
            logger.critical('beat raised exception %s: %r',
                            exc.__class__, exc,
                            exc_info=True)
            raise

    def get_service(self):
        '''
        这个是自定义的 目的是为了把service暴露出来，方便对service的scheduler操作，因为定时任务信息都存放在service.scheduler里
        :return:
        '''
        service = getattr(self, "service", None)
        if service is None:
            service = self.Service(
                app=self.app,
                max_interval=self.max_interval,
                scheduler_cls=self.scheduler_cls,
                schedule_filename=self.schedule,
            )
            setattr(self, "service", service)
        return self.service


beat = MyBeat(max_interval=10, app=celery_app, socket_timeout=30, pidfile=None, no_color=None,
              loglevel='INFO', logfile=None, schedule=None, scheduler='celery.beat.PersistentScheduler',
              scheduler_cls=None,  # XXX use scheduler
              redirect_stdouts=None,
              redirect_stdouts_level=None)


# 设置主动启动beat是为了避免使用celery -A celery_demo worker 命令重复启动worker
def run():
    '''
    启动Beat
    :return:
    '''
    beat.run()


def new_thread():
    '''
    创建一个线程启动Beat 最多只能创建一个
    :return:
    '''
    global flag
    if not flag:
        t = threading.Thread(target=run, daemon=True)
        t.start()
        # 启动成功2s后才能操作定时任务 否则可能会报错
        time.sleep(2)
        flag = True


def add_cron_task(task_name: str, cron_task: str, minute='*', hour='*', day_of_week='*', day_of_month='*',
                  month_of_year='*', **kwargs):
    '''
    创建或更新定时任务
    :param task_name: 定时任务名称
    :param cron_task: task名称
    :param minute: 以下是时间
    :param hour:
    :param day_of_week:
    :param day_of_month:
    :param month_of_year:
    :param kwargs:
    :return:
    '''
    service = beat.get_service()
    scheduler = service.scheduler
    entries = dict()
    entries[task_name] = {
        'task': cron_task,
        'schedule': crontab(minute=minute, hour=hour, day_of_week=day_of_week, day_of_month=day_of_month,
                            month_of_year=month_of_year, **kwargs),
        'options': {'expires': 3600}}
    scheduler.update_from_dict(entries)


def del_cron_task(task_name: str):
    '''
    删除定时任务
    :param task_name:
    :return:
    '''
    service = beat.get_service()
    scheduler = service.scheduler
    if scheduler.schedule.get(task_name, None) is not None:
        del scheduler.schedule[task_name]


def get_cron_task():
    '''
    获取当前所有定时任务的配置
    :return:
    '''
    service = beat.get_service()
    scheduler = service.scheduler
    ret = [{k: {"task": v.task, "crontab": v.schedule}} for k, v in scheduler.schedule.items()]
    return ret
