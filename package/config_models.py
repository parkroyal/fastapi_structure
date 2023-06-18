from typing import Any, List, Dict

class Config:
    """
    Contains parsed yaml config
    """
    def __init__(self, config_yaml: Any) -> None:
        # self.query: Dict[str, TableConfig] = {}
    
        self._parse_conf(config_yaml)

    def _parse_conf(self, conf_yaml: Any) -> None:
        """
        Parses yaml config and init python structures
        :param conf_yaml: config
        :return: None
        """
        for conf_name, conf_dict in conf_yaml.items():
            print(conf_name)
            if conf_name == 'sources':
                self._parse_sources_conf(conf_dict)
            else:
                self._parse_else_conf(conf_dict)

    def _parse_sources_conf(self, conf_yaml: dict):
        """
        Parses "sources" config params
        :param conf_yaml: config
        :return: None
        """
        for conf_name, conf_dict in conf_yaml.items():
            if conf_name == 'relational_db':
                self.querys = _get_key_2_conf(conf_dict, QueryConfig)

    def _parse_else_conf(self, conf_yaml: dict):
        """
        :param conf_yaml: config
        :return: dict
        """
        # for conf_name, conf_dict in conf_yaml.items():
        self.config = conf_yaml

class QueryConfig:
    """
    Parses table config. Example:
        user_table:
          db: 'datatp'
          schema: 'detail'
          connector_type: 'mysql_db'
          query: 'select * from [schema].[name] where {condition}'
    """
    db: str
    connector_type: str
    schema: str
    query: str

    def __init__(self, conf: Dict):
        self.db = conf.get('db', '')
        self.connector_type = conf.get('connector_type', '')
        self.schema = conf.get('schema', '')
        self.query = conf.get('query', '') 


def _get_key_2_conf(conf_dict: dict, class_name: Any) -> Dict[str, Any]:
    """
    Parses deep yaml structures into key-class_object structure
    :param conf_dict: structures config
    :param class_name: structure, that describes in config
    :return: key-class_object
    """
    key_2_conf_obj = {}
    for key, conf in conf_dict.items():
        key_2_conf_obj[key] = class_name(conf)
    return key_2_conf_obj