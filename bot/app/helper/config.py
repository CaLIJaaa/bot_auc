import yaml
import os

configPath = os.path.join(os.getcwd(), 'app', 'data', 'config.yaml')

class Config(object):
    def __init__(self) -> None:
        with open(configPath, encoding='utf-8') as file:
            self._yaml = yaml.safe_load(file)

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Config, cls).__new__(cls)
        return cls.instance

    def get_value(self, key: str) -> any:
        try:
            return self._yaml[key]
        except:
            return None
        
    def set_value(self, key: str, value: str) -> None:
        try:
            self._yaml[key] = value
            with open(configPath, 'w', encoding='utf-8') as file:
                yaml.dump(self._yaml, file, default_flow_style=False)
        except:
            return 
        return 