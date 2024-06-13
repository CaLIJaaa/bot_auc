import json
from app.DB.DB import User
from app.helper.config import Config
import os
def get_msg_lang(msg_name: str, user_id: int) -> str:
    """
    Принимает на вход название сообщения и язык, возвращает сообщение
    """
    dirname = os.path.dirname(__file__)
    with open(os.path.join(dirname, '../data/languages.json'), 'r') as f:
        messages = json.loads(f.read())
        return messages[msg_name][User.get_user_by_tg_id(user_id)[0]['lang']]
    return 

def get_msg_rules_lang(user_id: int) -> str:
    """
    Принимает на вход название сообщения и язык, возвращает правила
    """
    conf = Config()
    return conf.get_value('SETTINGS')[User.get_user_by_tg_id(user_id)[0]['lang']]