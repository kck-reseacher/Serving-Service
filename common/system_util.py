import os
import sys
import json
from pathlib import Path
from common.constants import SystemConstants as sc

class SystemUtil:

    @staticmethod
    def get_run_function_name() -> str:
        name = ""
        try:
            name = sys._getframe().f_code.co_name
        except SystemError:
            return name
        return name

    @staticmethod
    def get_class_name(cls) -> str:
        return type(cls).__name__

    @staticmethod
    def get_environment_variable():
        os_env = dict()

        # AIMODULE_LOG_PATH
        log_path = os.environ.get(sc.AIMODULE_LOG_PATH)
        if log_path is None:
            print("plz export AIMODULE_LOG_PATH")
            log_path = os.path.dirname(os.path.abspath(__file__))
        else:
            os_env[sc.AIMODULE_LOG_PATH] = log_path

        # MLOPS_SRS_PATH
        srs_path = os.environ.get(sc.MLOPS_SRS_PATH)
        if srs_path is None:
            print("plz export MLOPS_SRS_PATH")
            srs_path = os.path.dirname(os.path.abspath(__file__))
        else:
            os_env[sc.MLOPS_SRS_PATH] = srs_path

        # AIMODULE_SERVER_ENV
        server_env = os.environ.get(sc.AIMODULE_SERVER_ENV)
        if server_env is None:
            print("plz export AIMODULE_SERVER_ENV")
            py_path = os.path.dirname(os.path.abspath(__file__))
        else:
            os_env[sc.AIMODULE_SERVER_ENV] = server_env

        return os_env

    @staticmethod
    def get_py_config():
        """
        파이썬 config 파일을 로드하여 반환하는 함수

        :return: 파이썬 config (Dict) ex) postgres, redis 접속 정보 등
        """
        os_env = SystemUtil.get_environment_variable()
        server_env = os_env[sc.AIMODULE_SERVER_ENV]
        py_config_path = os_env[sc.MLOPS_SRS_PATH] + sc.CONFIG_FILE_PATH
        if server_env:
            config_file_name = sc.CONFIG_FILE_PREFIX + str(server_env) + sc.CONFIG_FILE_SUFFIX
        else:
            print("plz export AIMODULE_SERVER_ENV")

        py_config = json.loads((Path(py_config_path) / config_file_name).read_text())

        return py_config