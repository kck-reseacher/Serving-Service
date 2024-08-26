import redis
import json
import re
import atexit
from common.system_util import SystemUtil


py_config = SystemUtil.get_py_config()
client = redis.Redis(host=py_config['redis_server']['be']['host'], port=py_config['redis_server']['be']['port'])
atexit.register(client.close)


async def get_serving_targets(module: str, inst_type: str):
    """
    싱글 타겟 모델에 대한 서빙 대상 정보를 Redis server에서 가져오는 함수

    :param module : XAIOps 기능 명, ex) 이상탐지: exem_aiops_anls_inst, 부하예측: exem_aiops_fcst_tsmixer
    :param inst_type : XAIOps 인스턴스 타입, ex) 'WAS', 'DB'
    :return = 서빙 대상 타겟 리스트, ex) ['1201', '1202', '1301', ...]
    """
    try:
        redis_hash = client.hgetall('RedisServingTargets::mlc')
        redis_hash_keylist = list(redis_hash.keys())
        redis_key_list = [item for item in redis_hash_keylist if f"{module}".encode('utf-8') in item and f"{inst_type}".encode('utf-8') in item]

        serving_target_list = []
        for key in redis_key_list:
            serving_target_list.append(json.loads(redis_hash[key].decode('utf-8'))['target_id'])

        return serving_target_list
    except:
        return None


async def get_serving_targets_load_fcst(inst_type, product_type=None):
    """
    시스템 모델에 대한 서빙 대상 정보를 Redis server에서 가져오는 함수

    :param inst_type = XAIOps 인스턴스 타입, ex) 'WAS', 'DB'
    :param product_type = DB 인스턴스 타입에 대한 제품 타입, ex) 'ORACLE', 'TIBERO'
    :return = 서빙 대상 타겟 리스트, ex) ['1201', '1202', '1301', ...]
    """
    try:
        if inst_type in ['was', 'os', 'web', 'tp', 'network', 'service']:
            redis_string = client.get(f'Collect::LoadFcstTargetList:{inst_type}:all')

        if inst_type == 'db':
            redis_string = client.get(f'Collect::LoadFcstTargetList:{inst_type}:{product_type}')

        return json.loads(redis_string.decode('utf-8')) if redis_string is not None else None
    except:
        return None


async def get_rediskey_eventfcst(inst_type):
    """
    이슈예측 관련 redis key를 리턴하는 함수

    :param inst_type: ex) 'was', 'db', 'os'
    :return: ex) ['Collect::TargetGroups::instanceGroup:175:exem_aiops_event_fcst']
    """
    if inst_type in ['was', 'db', 'web', 'tp']:
        key_list = client.keys(f"Collect::TargetGroups::instanceGroup:*")
        eventfcst_keylist = [key.decode('utf-8') for key in key_list]
    if inst_type in ['os']:
        key_list = client.keys(f"Collect::TargetGroups::hostGroup:*")
        eventfcst_keylist = [key.decode('utf-8') for key in key_list]
    if inst_type in ['service']:
        key_list = client.keys(f"Collect::TargetGroups::codeGroup:*")
        eventfcst_keylist = [key.decode('utf-8') for key in key_list]

    return eventfcst_keylist


async def get_serving_targets_eventfcst(rediskey, inst_type):
    """
    이슈예측 그룹의 서빙 대상을 리턴하는 함수

    :param rediskey: ex) 'Collect::TargetGroups::instanceGroup:175:exem_aiops_event_fcst'
    :param inst_type: ex) 'was', 'db', 'os'
    :return: 이슈예측 특정 그룹 및 특정 인스턴스 타입의 서빙 대상, ex) ['1201', '1202']
    """
    redis_string = client.get(rediskey)
    target_dict = json.loads(redis_string.decode('utf-8')) if redis_string is not None else None

    target_list = None
    if target_dict is not None:
        if inst_type in target_dict.keys():
            target_list = target_dict[inst_type]
            return target_list if len(target_list) > 0 else None

    return target_list


async def get_grouptarget_eventfcst(rediskey):
    """
    이슈 예측 redis key를 파싱하여 group type과 id를 반환하는 함수

    :param rediskey: ex) 'Collect::TargetGroups::instanceGroup:175:exem_aiops_event_fcst'
    :return: 이슈예측 group의 type과 id 반환, ex) 'instanceGroup', '175'
    """
    match = re.search(r'(\w+Group):(\d+)', rediskey)
    group_type = match.group(1)
    group_id = match.group(2)
    return group_type, group_id


def get_rediskey_eventfcst_sync(inst_type):
    """
    이슈예측 관련 redis key를 리턴하는 함수

    :param inst_type: ex) 'was', 'db', 'os'
    :return: ex) ['Collect::TargetGroups::instanceGroup:175:exem_aiops_event_fcst']
    """
    if inst_type in ['was', 'db', 'web', 'tp']:
        key_list = client.keys(f"Collect::TargetGroups::instanceGroup:*")
        eventfcst_keylist = [key.decode('utf-8') for key in key_list]
    if inst_type in ['os']:
        key_list = client.keys(f"Collect::TargetGroups::hostGroup:*")
        eventfcst_keylist = [key.decode('utf-8') for key in key_list]
    if inst_type in ['service']:
        key_list = client.keys(f"Collect::TargetGroups::codeGroup:*")
        eventfcst_keylist = [key.decode('utf-8') for key in key_list]

    return eventfcst_keylist


def get_serving_targets_eventfcst_sync(rediskey, inst_type):
    """
    이슈예측 그룹의 서빙 대상을 리턴하는 함수

    :param rediskey: ex) 'Collect::TargetGroups::instanceGroup:175:exem_aiops_event_fcst'
    :param inst_type: ex) 'was', 'db', 'os'
    :return: 이슈예측 특정 그룹 및 특정 인스턴스 타입의 서빙 대상, ex) ['1201', '1202']
    """
    redis_string = client.get(rediskey)
    target_dict = json.loads(redis_string.decode('utf-8')) if redis_string is not None else None

    target_list = None
    if target_dict is not None:
        if inst_type in target_dict.keys():
            target_list = target_dict[inst_type]
            return target_list if len(target_list) > 0 else None

    return target_list


def get_grouptarget_eventfcst_sync(rediskey):
    """
    이슈 예측 redis key를 파싱하여 group type과 id를 반환하는 함수

    :param rediskey: ex) 'Collect::TargetGroups::instanceGroup:175:exem_aiops_event_fcst'
    :return: 이슈예측 group의 type과 id 반환, ex) 'instanceGroup', '175'
    """
    match = re.search(r'(\w+Group):(\d+)', rediskey)
    group_type = match.group(1)
    group_id = match.group(2)
    return group_type, group_id