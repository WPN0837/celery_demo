from django.shortcuts import HttpResponse
from celery_demo.cron_util import add_cron_task, del_cron_task, get_cron_task, new_thread
import json

# tasks.py里的函数名称与task名称映射
tasks = dict(
    task_1="cronApp.async_task_1",
    task_2="cronApp.async_task_2"
)


def add_or_update_task(request):
    '''
    创建或更新任务
    :param request:
    :return:
    '''
    task_name = request.GET.get("task_name", None)
    task = request.GET.get("task", None)
    m = request.GET.get("m", "*")
    h = request.GET.get("h", "*")
    if tasks is None or task is None or task not in tasks:
        return HttpResponse("ERROR")
    add_cron_task(task_name, tasks[task], minute=m, hour=h)
    return HttpResponse("SUCCESS")


def del_task(request):
    '''
    删除已设置的任务
    :param request:
    :return:
    '''
    task_name = request.GET.get("task_name", "")
    if not task_name:
        return HttpResponse("ERROR")
    del_cron_task(task_name)
    return HttpResponse("SUCCESS")


def get_cron_tasks(request):
    '''
    获取已设置的任务
    :param request:
    :return:
    '''
    result = get_cron_task()
    return HttpResponse(result)


def get_tasks(request):
    '''
    创建一个Beat 并返回所有可创建的任务目录
    :param request:
    :return:
    '''
    new_thread()
    return HttpResponse(json.dumps(tasks))
