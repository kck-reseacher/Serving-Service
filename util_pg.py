import traceback
from typing import List, Set, Dict
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from common.system_util import SystemUtil

config = SystemUtil.get_py_config()
# SRS를 container로 운영 시 bridge network ip로 수정
nginx_ip = config["nginx"]["host"]
nginx_port = config["nginx"]["port"]
max_workers = 10
timeout = 5


def get_server_run_configuration():
    """
    서비스 운영에 필요한 시스템 환경 변수를 리턴하는 함수
    :return: 환경 변수 ex) 로그 경로
    """
    os_env = SystemUtil.get_environment_variable()

    return os_env["AIMODULE_LOG_PATH"]


async def message_filtering(
        messages,
        module: str,
        redis_serving_targets: list,
        sys_id: int,
        inst_type: str,
        group_type: str = None,
        group_id: str = None
):
    """
    kafka에서 컨슈밍한 메시지를 서빙 Request(POST) 포맷(JSON)으로 필터링하는 함수
    :param messages: double-ended queue 형태의 자료구조, max length가 60인 queue (deque)
    :param module: XAIOps 기능명, ex) exem_aiops_anls_inst(이상탐지) (String)
    :param redis_serving_targets: Redis에서 관리하는 서빙 대상 정보 (List)
    :param sys_id : 시스템 아이디 ex) 개발서버 = 102
    :param inst_type : 인스턴스 타입 ex) was, db, os
    :param group_type
    :param group_id
    :return: List 객체, 서빙 Request Data, ex) [1201 target Serving Req(dict), 1202 target Serving Req(dict)]
    """
    output_data = []
    target_ids = set()

    try:
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
                        "sys_id": sys_id,
                        "target_id": target_id,
                        "inst_type": inst_type,
                        "business_list": []
                    },
                    "body": body
                }
                if group_type is not None:
                    target_entry['header']['group_type'] = group_type
                    target_entry['header']['group_id'] = group_id
                output_data.append(target_entry)

        return output_data
    except:
        return None


async def message_filtering_for_system(messages, module, redis_serving_targets, sys_id, inst_type, product_type=None):
    """
    kafka에서 컨슈밍한 메시지를 서빙 Request(POST) 포맷(JSON)으로 필터링하는 함수
    :param messages: double-ended queue 형태의 자료구조, max length가 60인 queue (deque)
    :param module: XAIOps 기능명, ex) exem_aiops_anls_inst(이상탐지) (String)
    :param redis_serving_targets: Redis에서 관리하는 서빙 대상 정보 (List)
    :param sys_id : 시스템 아이디 ex) 개발서버 = 102
    :param inst_type : 인스턴스 타입 ex) was, db, os

    :return: Dict 객체, 부하예측(RMC) 서빙 Request Data
    """
    target_ids = set()
    try:
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
                    "sys_id": sys_id,
                    "target_id": 'all' if product_type is None else product_type,
                    "inst_type": inst_type,
                    "business_list": []
                },
                "body": body
            }

        return target_entry
    except:
        return None


def send_request(data: Dict, logger) -> str:
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
    target_id = data["header"]["target_id"]
    if data["id1"] == "exem_aiops_anls_inst":
        url = f"http://{nginx_ip}:{nginx_port}/serving/anomaly-load/{inst_type}"

    elif data["id1"] == "exem_aiops_fcst_tsmixer":
        url = f"http://{nginx_ip}:{nginx_port}/serving/fcst-tsmixer/{inst_type}"

    elif data["id1"] == "exem_aiops_load_fcst":
        url = f"http://{nginx_ip}:{nginx_port}/serving/load-fcst/{inst_type}"

    elif data["id1"] == "exem_aiops_event_fcst":
        url = f"http://{nginx_ip}:{nginx_port}/serving/event-fcst/{inst_type}"

    logger.info(f"{data['standard_datetime']}, url: {url}, target_id: {target_id}, Serving request")
    response = requests.request("POST", url, headers=headers, data=payload, timeout=timeout)
    logger.info(
        f"{data['standard_datetime']}, url: {url}, target_id: {target_id}, Serving response: {response.status_code}")

    if 'body' in response.json():
        for key, sub_dict in response.json().get('body', {}).items():
            if isinstance(sub_dict, dict) and 'errno' in sub_dict:
                logger.info(f"[{inst_type}_{target_id}] {response.json()}")
            else:
                return response.json()
    else:
        logger.info(f"[{inst_type}_{target_id}] {response.json()}")
        return None


async def serving_request(req_list: list, logger) -> list:
    """
   서빙 요청 (HTTP)을 비동기로 처리하는 함수
   :param req_list: 서빙 데이터 List
   :return: 서빙 추론 결과를 한번에 담은 list 객체
   """
    responses = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(send_request, serv_data, logger) for serv_data in req_list]

        for future in as_completed(futures):
            try:
                response = future.result()
                if response is not None:
                    responses.append(response)
            except Exception:
                tb = traceback.format_exc()
                logger.info(f"serving_request Exception: {tb}")

    return responses


def serving_request_sync(req_list: list, logger) -> list:
    """
   서빙 요청 (HTTP)을 비동기로 처리하는 함수
   :param req_list: 서빙 데이터 List
   :return: 서빙 추론 결과를 한번에 담은 list 객체
   """
    responses = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(send_request, serv_data, logger) for serv_data in req_list]

        for future in as_completed(futures):
            try:
                response = future.result()
                if response is not None:
                    responses.append(response)
            except Exception:
                tb = traceback.format_exc()
                logger.info(f"serving_request_sync Exception: {tb}")

    return responses

def message_filtering_sync(
        messages,
        module: str,
        redis_serving_targets: list,
        sys_id: int,
        inst_type: str,
        group_type: str = None,
        group_id: str = None
):
    """
    kafka에서 컨슈밍한 메시지를 서빙 Request(POST) 포맷(JSON)으로 필터링하는 함수
    :param messages: double-ended queue 형태의 자료구조, max length가 60인 queue (deque)
    :param module: XAIOps 기능명, ex) exem_aiops_anls_inst(이상탐지) (String)
    :param redis_serving_targets: Redis에서 관리하는 서빙 대상 정보 (List)
    :param sys_id : 시스템 아이디 ex) 개발서버 = 102
    :param inst_type : 인스턴스 타입 ex) was, db, os
    :param group_type
    :param group_id
    :return: List 객체, 서빙 Request Data, ex) [1201 target Serving Req(dict), 1202 target Serving Req(dict)]
    """
    output_data = []
    target_ids = set()

    try:
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
                        "sys_id": sys_id,
                        "target_id": target_id,
                        "inst_type": inst_type,
                        "business_list": []
                    },
                    "body": body
                }
                if group_type is not None:
                    target_entry['header']['group_type'] = group_type
                    target_entry['header']['group_id'] = group_id
                output_data.append(target_entry)

        return output_data
    except:
        return None
