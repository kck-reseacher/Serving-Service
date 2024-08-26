from typing import List, Set, Dict
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from common.system_util import SystemUtil


def get_server_run_configuration():
    """
    서비스 운영에 필요한 시스템 환경 변수를 리턴하는 함수
    :return: 환경 변수 ex) 로그 경로
    """
    os_env = SystemUtil.get_environment_variable()

    return os_env["AIMODULE_LOG_PATH"]


def message_filtering(messages, module: str, redis_serving_targets: list):
    """
    kafka에서 컨슈밍한 메시지를 서빙 Request(POST) 포맷(JSON)으로 필터링하는 함수
    :param messages: double-ended queue 형태의 자료구조, max length가 60인 queue (deque)
    :param module: XAIOps 기능명, ex) exem_aiops_anls_inst(이상탐지) (String)
    :param redis_serving_targets: Redis에서 관리하는 서빙 대상 정보 (List)
    :return: List 객체, 서빙 Request Data, ex) [1201 target Serving Req(dict), 1202 target Serving Req(dict)]
    """
    output_data = []
    target_ids = set()
    for entry in messages:
        for value in entry['values']:
            target_ids.add(value['target_id'])

    serving_targets = [target for target in redis_serving_targets if target in target_ids]

    for target_id in serving_targets:
        body = []
        for entry in messages:
            for value in entry['values']:
                if value['target_id'] == target_id:
                    time_data = {'time': entry['time']}
                    time_data.update(value['data'])
                    body.append(time_data)

        if body:
            target_entry = {
                "id1": module,
                "standard_datetime": messages[-1]['time'],
                "header": {
                    "sys_id": messages[-1]['sys_id'],
                    "target_id": target_id,
                    "inst_type": messages[-1]['inst_type'],
                    "business_list": []
                },
                "body": body
            }
            output_data.append(target_entry)

    return output_data


def message_filtering_for_system(messages, module, redis_serving_targets):
    """
    kafka에서 컨슈밍한 메시지를 서빙 Request(POST) 포맷(JSON)으로 필터링하는 함수
    :param messages: double-ended queue 형태의 자료구조, max length가 60인 queue (deque)
    :param module: XAIOps 기능명, ex) exem_aiops_anls_inst(이상탐지) (String)
    :param redis_serving_targets: Redis에서 관리하는 서빙 대상 정보 (List)
    :return: Dict 객체, 부하예측(RMC) 서빙 Request Data
    """
    target_ids = set()
    for entry in messages:
        for value in entry['values']:
            target_ids.add(value['target_id'])

    serving_targets = [target for target in redis_serving_targets if target in target_ids]

    body = []
    for target_id in serving_targets:
        for entry in messages:
            for value in entry['values']:
                if value['target_id'] == target_id:
                    time_data = {'time': entry['time']}
                    time_data['target_id'] = target_id
                    time_data.update(value['data'])
                    body.append(time_data)

    if body:
        target_entry = {
            "id1": module,
            "standard_datetime": messages[-1]['time'],
            "header": {
                "sys_id": messages[-1]['sys_id'],
                "target_id": 'all',
                "inst_type": messages[-1]['inst_type'],
                "business_list": []
            },
            "body": body
        }

    return target_entry


def send_request(data: Dict) -> str:
    """
    서빙 데이터를 MLS Endpoint로 요청하고 추론 결과를 리턴하는 함수
    :param data: Serving Request Data
    :return: 서빙 추론 결과
    """
    payload = json.dumps(data)
    headers = {
        'Content-Type': 'application/json'
    }

    inst_type = data["header"]["inst_type"]
    if data["id1"] == "exem_aiops_anls_inst":
        url = f"http://0.0.0.0:20000/serving/anomaly-load/{inst_type}"
        response = requests.request("POST", url, headers=headers, data=payload)

    elif data["id1"] == "exem_aiops_fcst_tsmixer":
        url = f"http://0.0.0.0:20000/serving/fcst-tsmixer/{inst_type}"
        response = requests.request("POST", url, headers=headers, data=payload)

    elif data["id1"] == "exem_aiops_load_fcst":
        url = f"http://0.0.0.0:20000/serving/load-fcst/{inst_type}"
        response = requests.request("POST", url, headers=headers, data=payload)

    elif data["id1"] == "exem_aiops_event_fcst":
        url = f"http://0.0.0.0:20000/serving/event-fcst/{inst_type}"
        response = requests.request("POST", url, headers=headers, data=payload)

    data = response.json()
    if 'body' in data.keys():
        return response.json()


max_workers = 10


def serving_request(req_list: list):
    """
    서빙 요청 (HTTP)을 비동기로 처리하는 함수
    :param req_list: 서빙 데이터 List
    :return: 서빙 추론 결과를 한번에 담은 list 객체
    """
    responses = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(send_request, serv_data) for serv_data in req_list]

        for future in as_completed(futures):
            try:
                response = future.result()
                if response is not None:
                    responses.append(response)
            except Exception as e:
                print(f"Exception : {e}")

    return responses
