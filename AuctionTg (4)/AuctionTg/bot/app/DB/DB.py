import pymysql.cursors
from app.helper.config import Config

conf = Config()

class User():
    def _get_connection_cursor():
        conn = pymysql.connect(host=conf.get_value('HOST'),
                             user=conf.get_value('USER'),
                             password=conf.get_value('PASSWORD'),
                             database=conf.get_value('DATABASE'),
                             cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()
        return conn, cursor
    
    def is_user_admin(tg_id: int) -> bool:
        """
        `true` if the user is admin

        `false` if the user is not admin
        """
        conn, cursor = User._get_connection_cursor()
        cursor.execute("SELECT * FROM `user` WHERE `tg_id` = %s AND `status` = %s;", (str(tg_id), 'admin'))
        if len(cursor.fetchall()) != 0:
            return True
        return False

    def get_admins():
        '''
        Возвращает все admins из таблицы `user` 
        '''
        conn, cursor = User._get_connection_cursor()
        cursor.execute("SELECT * FROM `user` WHERE `status` = %s;", ('admin', ))
        return cursor.fetchall()
    
    def get_banned():
        '''
        Возвращает все admins из таблицы `user` 
        '''
        conn, cursor = User._get_connection_cursor()
        cursor.execute("SELECT * FROM `user` WHERE `status` = %s;", ('banned', ))
        return cursor.fetchall()
    
    def get_users():
        '''
        Возвращает всех из таблицы `user` 
        '''
        conn, cursor = User._get_connection_cursor()
        cursor.execute("SELECT * FROM `user` WHERE `status`!= %s;", ('banned', ))
        return cursor.fetchall()
    
    def get_users_tg_id():
        '''
        Возвращает всех из таблицы `user` 
        '''
        conn, cursor = User._get_connection_cursor()
        cursor.execute("SELECT id, tg_id FROM `user` WHERE `status`!= %s;", ('banned', ))
        return cursor.fetchall()

    def get_user_by_tg_id(tg_id):
        '''
        Возвращает все значения из таблицы `user`
        '''
        conn, cursor = User._get_connection_cursor()
        cursor.execute("SELECT * FROM `user` WHERE `tg_id` = %s AND `status` != %s;", (str(tg_id), 'banned'))
        return cursor.fetchall()
    
    def get_user_by_id(id):
        '''
        Возвращает все значения из таблицы `user`
        '''
        conn, cursor = User._get_connection_cursor()
        cursor.execute("SELECT * FROM `user` WHERE `id` = %s AND `status` != %s;", (str(id), 'banned'))
        return cursor.fetchall()

    def create_user(
            tg_id: int,
            tg_link: str,
            lang: str,
            status: str = 'user'
    ) -> None:
        '''
        Добавляет нового пользователя в таблицу `user`
        '''
        conn, cursor = User._get_connection_cursor()
        try:
            cursor.execute("INSERT INTO `user` (`tg_id`, `tg_link`, `lang`, `status`) VALUES (%s, %s, %s, %s);",
                        (tg_id, tg_link, lang, status))
        except BaseException:
            pass
        finally:
            conn.commit()
            cursor.close()
            conn.close()
        return
    
    def change_language(tg_id: int, lang_code: str = None) -> None:
        '''
        Изменяет язык пользователя
        '''
        conn, cursor = User._get_connection_cursor()
        try:
            if lang_code:
                new_lang = lang_code
            else:
                user = User.get_user_by_tg_id(tg_id)
                lang = user[0]['lang']
                new_lang = 'ru' if lang == 'en' else 'en'
            cursor.execute("UPDATE `user` SET `lang` = %s WHERE `tg_id` = %s;", (new_lang, tg_id))
        except BaseException:
            pass
        finally:
            conn.commit()
            cursor.close()
            conn.close()
        return
    
    def status_change(tg_id: int, id: int, status: str) -> None:
        '''
        Изменяет статус пользователя
        '''
        conn, cursor = User._get_connection_cursor()
        try:
            cursor.execute("UPDATE `user` SET `status` = %s WHERE `id` = %s;", (status, str(id)))
        except BaseException:
            pass
        finally:
            conn.commit()
            cursor.close()
            conn.close()
        return
    
    def status_change_by_nickname(tg_id: int, nickname: str, status: str) -> None:
        '''
        Изменяет статус пользователя
        '''
        conn, cursor = User._get_connection_cursor()
        try:
            cursor.execute("UPDATE `user` SET `status` = %s WHERE `tg_link` = %s;", (status, nickname))
            cursor.execute("SELECT * FROM `user` WHERE `tg_link` = %s;", (nickname, ))
            user = cursor.fetchall()
        except BaseException:
            pass
        finally:
            conn.commit()
            cursor.close()
            conn.close()
        return user

class Auction():
    def _get_connection_cursor():
        conn = pymysql.connect(host=conf.get_value('HOST'),
                             user=conf.get_value('USER'),
                             password=conf.get_value('PASSWORD'),
                             database=conf.get_value('DATABASE'),
                             cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()
        return conn, cursor
    
    def get_opened_auctions():
        """
        Возвращает все аукционы в статусе `opened`
        """
        conn, cursor = Auction._get_connection_cursor()
        cursor.execute("SELECT * FROM `auction` WHERE `status` = %s;", ('opened', ))
        return cursor.fetchall()
    
    def get_opened_auctions_by_type(type: str):
        """
        Возвращает все аукционы в статусе `opened`
        """
        conn, cursor = Auction._get_connection_cursor()
        cursor.execute("SELECT * FROM `auction` WHERE `status` = %s AND `type` = %s;", ('opened', str(type)))
        return cursor.fetchall()
    
    def get_time_auctions_by_id(auction_id: int):
        """
        Возвращает время до конца из таблицы `auction` по его `id`
        """
        conn, cursor = Auction._get_connection_cursor()
        cursor.execute("SELECT SEC_TO_TIME(TIMESTAMPDIFF(SECOND, CURRENT_TIMESTAMP(), TIMESTAMPADD(SECOND, TIME_TO_SEC(time_leinght), time_start))) as left_time FROM `auction` WHERE `id` = %s;", (str(auction_id), ))
        return cursor.fetchall()
    
    def get_auction_by_id(auction_id: int):
        """
        Возвращает значения из таблицы `auction` по его `id`
        """
        conn, cursor = Auction._get_connection_cursor()
        cursor.execute("SELECT * FROM `auction` WHERE `id` = %s;", (str(auction_id), ))
        return cursor.fetchall()
    
    def get_opened_auction_by_id(auction_id: int):
        """
        Возвращает значения из таблицы `auction` по его `id` открытые
        """
        conn, cursor = Auction._get_connection_cursor()
        cursor.execute("SELECT * FROM `auction` WHERE `id` = %s AND `status` = %s;", (str(auction_id), 'opened'))
        return cursor.fetchall()
    
    def get_auction_by_id_with_bid(auction_id: int):
        """
        Возвращает значения из таблицы `auction` по его `id` со ставками
        """
        conn, cursor = Auction._get_connection_cursor()
        cursor.execute("SELECT * FROM auction a LEFT JOIN bid b ON a.id = b.auction_id WHERE a.id = %s ORDER BY money DESC;", (str(auction_id), ))
        return cursor.fetchall()
    
    def get_auction_by_user_id(tg_id: int):
        """
        Возвращает значения из `auction` по `tg_id` пользователя
        """
        conn, cursor = Auction._get_connection_cursor()
        cursor.execute("SELECT * FROM `user` WHERE `tg_id` = %s;", (str(tg_id), ))
        user_id = cursor.fetchall()[0]['id']
        cursor.execute("SELECT * FROM `auction` WHERE `status` != %s;", ('deleted', ))
        return cursor.fetchall()
    
    def get_auction_by_status_pay():
        """
        Возвращает значения из `auction` по `tg_id` пользователя
        """
        conn, cursor = Auction._get_connection_cursor()
        # cursor.execute("SELECT * FROM `user` WHERE `tg_id` = %s;", (str(tg_id), ))
        # user_id = cursor.fetchall()[0]['id']
        cursor.execute("SELECT * FROM `auction` WHERE `statusPay` = %s;", ('active', ))
        return cursor.fetchall()
    
    def delete_auction(auction_id: int):
        """
        Удаляет аукцион по его `id`
        """
        try:
            conn, cursor = Auction._get_connection_cursor()
            cursor.execute("UPDATE `auction` SET `status` = 'deleted' WHERE `id` = %s;", (str(auction_id), ))
            conn.commit()
        except BaseException:
            pass
        finally:
            cursor.close()
            conn.close()
        return
    
    def open_auction_by_id(auction_id: int) -> None:
        """
        Открывает аукцион по его `id`
        """
        try:
            conn, cursor = Auction._get_connection_cursor()
            cursor.execute("update auction a set status = 'opened', time_start = CURRENT_TIMESTAMP(), time_update = CURRENT_TIMESTAMP() \
                            where a.id = %s and \
                            (select COUNT(*) FROM (SELECT * FROM auction) AS B  WHERE B.type = (SELECT type FROM (SELECT * FROM auction) as C WHERE id = %s) AND B.status = %s) < 5", 
                            (str(auction_id), str(auction_id), 'opened'))
            conn.commit()
        except BaseException:
            pass
        finally:
            cursor.close()
            conn.close()
        return
    
    def create_auction(
                       created_by: int,
                       picture: str,
                       name: str,
                       type: str,
                       volume: float,
                       abv: float,
                       country_ru: str,
                       country_en: str,
                       brand: str,
                       produser: str,
                       description_ru: str,
                       description_en: str,
                       price: float,
                       time_leinght
    ) -> int:
        auction_id = None
        try:
            print(country_en, country_ru, description_en, description_ru)
            user = User.get_user_by_tg_id(created_by)
            created_by = user[0]['id'] if len(user)!= 0 else 0
            conn, cursor = Auction._get_connection_cursor()
            cursor.execute("INSERT INTO `auction` (`created_by`, `picture`, `name`, \
                           `type`, `volume`, `abv`, `country_ru`, `country_en`, `brand`, `produser`, \
                           `description_ru`, `description_en`, `price`, `time_leinght`, `status`) \
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                            (created_by,
                             picture,
                             name,
                             type,
                             volume,
                             abv,
                             country_ru,
                             country_en,
                             brand,
                             produser,
                             description_ru,
                             description_en,
                             price,
                             time_leinght,
                             'created'))
            conn.commit()
            auction_id = cursor.lastrowid
        except BaseException as e:
            print(e)
        finally:
            cursor.close()
            conn.close()
        return auction_id
    
    def update_auction(auction_id: int,
                       to_update: str,
                       value: str) -> int:
        """
        Обновляет аукцион по его `id`
        """
        try:
            conn, cursor = Auction._get_connection_cursor()
            match to_update:
                case 'picture':
                    to_update = 'picture'
                case 'description':
                    to_update = 'description'
                case 'price':
                    to_update = 'price'
                case 'time_leinght':
                    to_update = 'time_leinght'
                case 'statusPay':
                    to_update = 'statusPay'
                case _:
                    return
            cursor.execute("UPDATE `auction` SET `" + to_update + "` = %s WHERE `id` = %s;",
                            (value, auction_id))
            conn.commit()
        except BaseException:
            pass
        finally:
            cursor.close()
            conn.close()
        return 
    
    def update_payment_auction(
        auction_id: int,
        invoice_id: int,
        asset: str,
        amount: str,
        payment_url: str
    ) -> int:
        """
        Обновляет аукцион по его `id` (только поля для оплаты)
        """
        try:
            conn, cursor = Auction._get_connection_cursor()

            cursor.execute(
                "UPDATE `auction` SET `invoice_id` = %s, `asset` = %s, `amount` = %s, `payment_url` = %s WHERE `id` = %s;",
                (invoice_id, asset, amount, payment_url, auction_id)
            )
            conn.commit()
        except BaseException:
            pass
        finally:
            cursor.close()
            conn.close()
        return 
    
    def update_status_auction(
        auction_id: int,
        invoice_id: int,
        asset: str,
        statusPay: str,
        payment_url: str
    ) -> int:
        """
        Обновляет аукцион по его `id` (только поля для оплаты)
        """
        try:
            conn, cursor = Auction._get_connection_cursor()

            cursor.execute(
                "UPDATE `auction` SET `invoice_id` = %s, `asset` = %s, `statusPay` = %s, `payment_url` = %s WHERE `id` = %s;",
                (invoice_id, asset, statusPay, payment_url, auction_id)
            )
            conn.commit()
        except BaseException:
            pass
        finally:
            cursor.close()
            conn.close()
        return 
    
    def expired_auction():
        """
        Закрывает все аукционы
        """
        auctions = []
        try:
            conn, cursor = Auction._get_connection_cursor()
            cursor.execute("SELECT `id` FROM `auction` WHERE TIMESTAMPADD(SECOND, TIME_TO_SEC(time_leinght), time_start) < NOW() AND `status`='closed';")
            auctions = cursor.fetchall()
            cursor.execute("UPDATE `auction` SET `statusPay`  = 'closed' WHERE TIMESTAMPADD(SECOND, TIME_TO_SEC(time_leinght), time_start) < NOW() AND `status` = 'closed';")
            
            conn.commit()
        except BaseException:
            pass
        finally:
            cursor.close()
            conn.close()
        return auctions

    
class Bid():
    def _get_connection_cursor():
        conn = pymysql.connect(host=conf.get_value('HOST'),
                             user=conf.get_value('USER'),
                             password=conf.get_value('PASSWORD'),
                             database=conf.get_value('DATABASE'),
                             cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()
        return conn, cursor
    
    def add_bid(auction_id: int, tg_id: int, money: int) -> bool:
        """
        Добавляет новую ставку в таблицу `bid`
        """
        conn, cursor = Bid._get_connection_cursor()
        try:
            cursor.execute("SELECT TIMESTAMPADD(SECOND, TIME_TO_SEC(time_leinght), time_start) > NOW() as `result` FROM `auction` WHERE `id` = %s;", (str(auction_id), ))
            results = cursor.fetchall()
            if len(results) > 0 and results[0]['result'] != 0:
                cursor.execute("INSERT INTO `bid` (`auction_id`, `user_id`, `money`, `time_bid`) VALUES (%s, (SELECT `id` FROM `user` WHERE `tg_id` = %s), \
                                IFNULL((SELECT MAX(money ) FROM (SELECT * FROM `bid`) as b WHERE b.auction_id = %s), (SELECT a.price FROM auction a WHERE a.id=%s))+%s, CURRENT_TIMESTAMP());",
                            (str(auction_id), str(tg_id), str(auction_id), str(auction_id), money))
                cursor.execute("SELECT TIMESTAMPDIFF(SECOND, CURRENT_TIMESTAMP(), TIMESTAMPADD(SECOND, TIME_TO_SEC(time_leinght), time_start)) as `result` FROM `auction` WHERE `id` = %s;", (str(auction_id), ))
                try:
                    time = cursor.fetchall()[0]['result']
                    if time < conf.get_value("ANTISNIPER_FIX"):
                        cursor.execute("UPDATE `auction` SET `time_leinght` = ADDTIME(`time_leinght`, SEC_TO_TIME(%s)) WHERE `id` = %s;", (str(conf.get_value("ANTISNIPER")), str(auction_id), ))
                except BaseException:
                    pass
                conn.commit()
                done = True
        except BaseException:
            done = False
        finally:
            cursor.close()
            conn.close()
        return done
    
    def get_bids_by_auction_id(auction_id: int):
        """
        Возвращает все ставки по `auction_id`
        """
        conn, cursor = Bid._get_connection_cursor()
        cursor.execute("SELECT a.id, `money`, `tg_link` FROM `bid` a LEFT JOIN `user` b ON a.user_id = b.id WHERE `auction_id` = %s;", (str(auction_id), ))
        return cursor.fetchall()
    
    def get_bid_by_id(bid_id: int):
        """
        Возвращает ставку по `id`
        """
        conn, cursor = Bid._get_connection_cursor()
        cursor.execute("SELECT * FROM `bid` WHERE `id` = %s;", (str(bid_id), ))
        return cursor.fetchall()
    
    def delete_bid_by_id(bid_id: int):
        """
        Удаляет ставку по `id`
        """
        try:
            conn, cursor = Bid._get_connection_cursor()
            cursor.execute("DELETE FROM `bid` WHERE `id` = %s;", (str(bid_id), ))
            conn.commit()
        except BaseException:
            pass
        finally:
            cursor.close()
            conn.close()
        return
    
    def get_bid_by_tg_id(tg_id: int, auction_id: int):
        """
        Возвращает ставку по `tg_id` пользователя
        """
        conn, cursor = Bid._get_connection_cursor()
        cursor.execute("SELECT * FROM `bid` WHERE `user_id` = (SELECT `id` FROM `user` WHERE `tg_id` = %s) AND `auction_id` = %s ORDER BY `money` DESC;", (str(tg_id), str(auction_id)))
        return cursor.fetchall()