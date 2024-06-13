import pymysql.cursors
from app.helper.config import Config
import logging
import sys
import os

structurePath = os.path.join(os.getcwd(), 'app', 'DB', 'sql', 'create_structure.sql')

conf = Config()
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

conn = pymysql.connect(host=conf.get_value('HOST'),
                             user=conf.get_value('USER'),
                             password=conf.get_value('PASSWORD'),
                             cursorclass=pymysql.cursors.DictCursor)
cursor = conn.cursor()
cursor.execute(f'CREATE DATABASE IF NOT EXISTS `{conf.get_value('DATABASE')}`;')
conn = pymysql.connect(host=conf.get_value('HOST'),
                             user=conf.get_value('USER'),
                             password=conf.get_value('PASSWORD'),
                             database=conf.get_value('DATABASE'),
                             cursorclass=pymysql.cursors.DictCursor)
cursor = conn.cursor()
with open(structurePath, 'r') as sql_file:
    sql_script = sql_file.read()
    for sql in sql_script.split(';'):
        try:
            cursor.execute(sql)
        except BaseException as e:
            print(e)
    conn.commit()
    logging.info("Скрипт SQL успешно выполнен")