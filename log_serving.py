import requests
import traceback
import json
from datetime import datetime, timedelta
import pytz
from apscheduler.schedulers.blocking import BlockingScheduler
import threading
from common.system_util import SystemUtil

py_config = SystemUtil.get_py_config()
mlc_url = f"http://{py_config['mlc']['host']}:{py_config['mlc']['port']}"
nginx_host, nginx_port = py_config['nginx']['host'], py_config['nginx']['port']
module_name, sys_id = "exem_aiops_anls_log", py_config['sys_id']

def serving_one_target(standard_datetime, sys_id, target_id):
    data = {
        "id1": module_name,
        "standard_datetime": standard_datetime,
        "uid": "74d7c705-8ad4-4a70-82b5-7d5b9102415d",
        "header": {
            "sys_id": sys_id,
            "target_id": target_id,
            "inst_type": "log",
            "business_list": [],
            "time": standard_datetime,
            'module': module_name,
        },
        "body": []
    }
    requests.post(f'http://{nginx_host}:{nginx_port}/serving/anls-log/log', data=json.dumps(data))
    print(f"success threading. http://{nginx_host}:{nginx_port}/serving/anls-log/log, target_id: {target_id}")

def start_threading():
    standard_datetime = (datetime.now() - timedelta(minutes=2)).strftime('%Y-%m-%d %H:%M:%S')
    print(f"==================== start::serving_all_target::time: {standard_datetime} ====================")

    try:
        ''' api call to mlc '''
        res = requests.get(f'{mlc_url}/mlc/serving/log-serving-info')

        if res.status_code == 200:
            target_list = json.loads(res.content)
            print(target_list)

            if target_list:  # 서빙할 대상이 있다면 request post
                for target_id in target_list:
                    print(f"threading start. {standard_datetime}, {nginx_port}, {target_id}")
                    log_thread = threading.Thread(target=serving_one_target, args=(standard_datetime, sys_id, target_id))
                    log_thread.start()
        else:
            print(f"request fail. status_code : {res.status_code}")
    except Exception as e:
        print(f"error during query from mlc. : {e}")

def main_process():
    print(f"==================== log serving module started. {(datetime.now()).strftime('%Y-%m-%d %H:%M:%S')} ====================")
    timezone = pytz.timezone('Asia/Seoul')
    scheduler = BlockingScheduler()
    scheduler.add_job(start_threading, "cron", minute="*", second="0", timezone=timezone)
    try:
        scheduler.start()
    except Exception as e:
        print(f"{e}\n\n{traceback.format_exc()}")


if __name__ == "__main__":
    main_process()