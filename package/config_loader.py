import collections
import os
import yaml
import json
from typing import List, Union
from package.config_models import Config
from package.logging_helper import logger

with open(r"D:\report_r_steven\config.json", "r") as f:
    config = json.load(f)

def get_config(config_folder: str, report_name: str) -> Union[Config, str]:
    """ Get config by report_name or path_config.
    First step - checking server file by report_name, if it is not exist - return local version by path_config
    :param config_folder - local path for reports config.yml
    :param report_name - name of report, will be use for getting config from server
    :return Config object (or None) and errors text.
    """
    # TODO add fetching config from config api
    logger.info("load conf from local {}".format(config_folder))
    conf, err = _load_conf_by_path(config_folder)

    return conf, '' if conf is not None else err


def _load_conf_by_path(path_config) -> Union[Config, str]:
    """
    Read .yml config from local path
    :param path_config: relative\full path to config
    :return: Config object (or None) and errors text
    """
    try:
        conf_files = [file_name for file_name in os.listdir(path_config) if '.yml' in file_name or '.yaml' in file_name]

        print(f"path_config: {os.listdir(path_config)}")

        yml_configs = {}
        for file_name in conf_files:
            with open("{}/{}".format(path_config, file_name), "r") as stream:
                yml_configs = deep_update(yml_configs, yaml.safe_load(stream))
        # print(yml_configs)
        return Config(yml_configs), ''
    except Exception as e:
        return None, 'Exception in _load_conf_by_path: {}'.format(e)


def deep_update(first_d: dict, second_d: dict) -> dict:
    """
    Completely copies the second dictionary into the first
    :param first_d: dictionary to which the contents will be copied
    :param second_d: the dictionary from which the contents will be copied
    :return: Full copied dictionary from second_d to first_d
    """
    for k, v in second_d.items():
        if isinstance(v, dict):
            first_d[k] = deep_update(first_d.get(k, {}), v)
        else:
            first_d[k] = v
    return first_d