import copy
import os
import logging.config
import json
from pathlib import Path
from datetime import datetime

from common.constants import SystemConstants as sc

class Logger:

    def __init__(self):
        py_path = os.environ.get(sc.MLOPS_SRS_PATH)
        self.py_logger_path = py_path + sc.LOGGER_FILE_PATH

        # os 환경 변수 AIMODULE_SERVER_ENV, 환경 변수 없으면 dev
        self.logger_env = os.environ.get(sc.AIMODULE_LOGGER_ENV)
        if self.logger_env is None:
            self.logger_env = "dev"

        logger_file_name = sc.LOGGER_FILE_PREFIX + str(self.logger_env) + sc.LOGGER_FILE_SUFFIX

        self.logger_config = json.load(open(Path(self.py_logger_path) / logger_file_name))
        self.stat_logger_config = copy.deepcopy(self.logger_config)

    def get_default_logger(self, logdir, service_name, error_log_dict=None, train_flag=False):

        Path(logdir).mkdir(exist_ok=True, parents=True)

        if train_flag:
            filename = str(Path(logdir) / "train.log")
        else:
            filename = str(Path(logdir) / f"{service_name}.log")

        self.logger_config["handlers"]["file"]["filename"] = filename

        handlers = list()
        handlers.append("file")

        # error_log_dict 없으면 생성 안하게 설정
        if error_log_dict is not None:
            Path(error_log_dict["log_dir"]).mkdir(exist_ok=True, parents=True)
            error_filename = str(Path(error_log_dict["log_dir"]) / f"{error_log_dict['file_name']}.log")
            self.logger_config["handlers"]["error"]["filename"] = error_filename
            handlers.append("error")

        self.logger_config["loggers"][f"{service_name}"] = dict()
        self.logger_config["loggers"][f"{service_name}"]["handlers"] = handlers

        logging.config.dictConfig(self.logger_config)
        logger = logging.getLogger(service_name)

        return logger


