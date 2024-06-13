from aiogram import Router, F
from aiogram.filters import Command
from aiogram.filters.callback_data import CallbackData, CallbackQuery
from aiogram.types import Message, ReplyKeyboardRemove, FSInputFile
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

import asyncio
from queue import Queue
from threading import Thread

import app.keyboards.for_user as kb_usr
import app.callbackdata.custom as cbd
import app.messages.for_user as msg
import app.messages.for_admin as msg_adm
from app.DB.DB import User, Auction, Bid
import pymysql.cursors
from app.helper.config import Config
import time

conf = Config()
router = Router()

class AuctionControllerDB():
    def _get_connection_cursor():
        conn = pymysql.connect(host=conf.get_value('HOST'),
                             user=conf.get_value('USER'),
                             password=conf.get_value('PASSWORD'),
                             database=conf.get_value('DATABASE'),
                             cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()
        return conn, cursor
    
    def get_min_time_to_update():
        """
        Возвращает минимальное время до обновления аукциона
        """
        conn, cursor = AuctionControllerDB._get_connection_cursor()
        cursor.execute("SELECT MIN(`time_start` - CURRENT_TIMESTAMP() + TIME_TO_SEC(`time_leinght`)) as `result` FROM `auction` WHERE `status`='opened';")
        return cursor.fetchall()[0]['result']
    
    def close_auction():
        """
        Закрывает все аукционы
        """
        auctions = []
        try:
            conn, cursor = AuctionControllerDB._get_connection_cursor()
            cursor.execute("SELECT `id` FROM `auction` WHERE (`time_start` - CURRENT_TIMESTAMP() + TIME_TO_SEC(`time_leinght`)) < 0 AND `status`='opened';")
            auctions = cursor.fetchall()
            cursor.execute("UPDATE `auction` SET `status`  = 'closed' WHERE (`time_start` - CURRENT_TIMESTAMP() + TIME_TO_SEC(`time_leinght`)) < 0 AND `status` = 'opened';")
            
            conn.commit()
        except BaseException:
            pass
        finally:
            cursor.close()
            conn.close()
        return auctions
    
    def get_losers_by_auction_id(auction_id: int):
        """
        Возвращает список пользователей, которые не смогли победить в аукционе
        """
        losers = []
        try:
            conn, cursor = AuctionControllerDB._get_connection_cursor()
            cursor.execute("SELECT DISTINCT `user_id`, user.tg_id FROM `bid` LEFT JOIN user ON user.id = `user_id` WHERE `auction_id` = %s AND \
                           `user_id` != (SELECT `user_id` FROM `bid` WHERE `auction_id` = %s ORDER BY `money` DESC LIMIT 1);;", (str(auction_id), str(auction_id)))
            losers = cursor.fetchall()
        except BaseException:
            pass
        finally:
            cursor.close()
            conn.close()
        return losers
    
    def get_winner_by_auction_id(auction_id: int):
        """
        Возвращает пользователя, который победил в аукционе
        """
        winner = []
        money = 0
        try:
            conn, cursor = AuctionControllerDB._get_connection_cursor()
            cursor.execute("SELECT * FROM `user` WHERE `id` = (SELECT `user_id` FROM `bid` WHERE `auction_id` = %s ORDER BY `money` DESC LIMIT 1);", (str(auction_id), ))
            winner = cursor.fetchall()
            cursor.execute("SELECT `money` FROM `bid` WHERE `auction_id` = %s ORDER BY `money` DESC LIMIT 1;", (str(auction_id), ))
            money = cursor.fetchall()[0]['money']
        except BaseException:
            pass
        finally:
            cursor.close()
            conn.close()
        return winner, money
    
def controller(bot, loop):
    while True:
        time_to_close = AuctionControllerDB.get_min_time_to_update()
        time_to_close = time_to_close if time_to_close else 0
        auctions = AuctionControllerDB.close_auction()
        if len(auctions) > 0:    
            between_callback(auctions, bot, loop=loop)
        if time_to_close < 10:
            time.sleep(1)
        else: 
            time.sleep(10)

def between_callback(*args, loop):
    asyncio.set_event_loop(loop)

    send_fut = asyncio.run_coroutine_threadsafe(send_notification(*args), loop)
    send_fut.result()

async def send_notification(auction, bot):
    if conf.get_value('LOOSE_NOTIFICATION'):
        users = AuctionControllerDB.get_losers_by_auction_id(auction[0]['id'])
        auction_name = Auction.get_auction_by_id(auction[0]['id'])
        auction_name = auction_name[0]['name'] if len(auction_name) > 0 else ''
        for user in users:
            try:
                await bot.send_message(user['tg_id'], 
                                    msg.losers_msg(user['tg_id'], auction_name),
                                    reply_markup=kb_usr.get_back_kb(user['tg_id']))
            except BaseException:
                pass

    if conf.get_value('WIN_NOTIFICATION'):
        user, money = AuctionControllerDB.get_winner_by_auction_id(auction[0]['id'])
        if len(user) > 0:
            try:
                await bot.send_message(user[0]['tg_id'], 
                                    msg.winners_msg(user[0]['tg_id'], money),
                                    reply_markup=kb_usr.get_back_kb(user[0]['tg_id']))
                admins = User.get_admins()
                for admin in admins:
                    try:
                        await bot.send_message(admin['tg_id'], 
                                    msg_adm.winner_msg(admin['tg_id'], money, user[0]['tg_link']))
                    except BaseException:
                        pass
        
            except BaseException:
                pass