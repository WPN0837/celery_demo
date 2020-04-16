from celery.task import periodic_task, task
from celery.utils.log import get_logger

logger = get_logger('celery.beat')


@task(name="cronApp.async_task_1")
def task_1():
    '''
    任务1
    :return:
    '''
    logger.info("task_1 start...")
    print("正在处理任务...")
    logger.info("task_1 end...")
    return "SUCCESS"


@task(name="cronApp.async_task_2")
def task_2():
    '''
    任务2
    :return:
    '''
    logger.info("task_2 start...")
    print("正在处理任务...")
    logger.info("task_2 end...")
    return "SUCCESS"
