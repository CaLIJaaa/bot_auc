from app.helper.i18n import get_msg_lang, get_msg_rules_lang
from app.helper.config import Config
from app.DB.DB import Auction, Bid, User, Bank
import datetime

def lang_msg(tg_id: int = None):
    if tg_id is None:
        msg = 'Please, select a language'
    else:
        msg = get_msg_lang('lang_msg', tg_id)
    return msg

def start_msg(tg_id: int):
    msg = get_msg_lang('hello_msg', tg_id)
    return msg

def payment_msg(tg_id: int):
    msg = get_msg_lang('payment_msg', tg_id)
    return msg

def payment_msg_error(tg_id: int):
    msg = get_msg_lang('payment_msg_error', tg_id)
    return msg

def pay_way_msg(tg_id: int):
    msg = get_msg_lang('pay_way_msg', tg_id)
    return msg

def menu_msg(tg_id: int) -> str:
    msg = get_msg_lang('user_menu_msg', tg_id)
    return msg

def do_bid_msg(tg_id: int) -> str:
    msg = get_msg_lang('do_bid_msg', tg_id)
    return msg

def do_bid_msg_sure(tg_id: int, price: str) -> str:
    msg = (get_msg_lang('do_bid_msg_sure', tg_id) % (price))
    return msg

def do_bid_msg_cancel(tg_id: int) -> str:
    msg = get_msg_lang('do_bid_msg_cancel', tg_id)
    return msg

def do_bid_msg_success(tg_id: int, price: str) -> str:
    msg = (get_msg_lang('do_bid_msg_success', tg_id) % (price))
    return msg

def increace_bank_summ(tg_id: int) -> str:
    msg = get_msg_lang('increace_bank_summ', tg_id)
    return msg

def increace_bank_sure(tg_id: int, price: str) -> str:
    msg = (get_msg_lang('increace_bank_sure', tg_id) % (price))
    return msg

def increace_bank_low(tg_id: int, price: str) -> str:
    msg = (get_msg_lang('increace_bank_low', tg_id) % (price))
    return msg

def put_summ(tg_id: int) -> str:
    msg = get_msg_lang('put_summ', tg_id)
    return msg

def put_summ_success(tg_id: int) -> str:
    msg = get_msg_lang('put_summ_success', tg_id)
    return msg

def put_summ_error(tg_id: int) -> str:
    msg = get_msg_lang('put_summ_error', tg_id)
    return msg

def put_summ_empty(tg_id: int) -> str:
    msg = get_msg_lang('put_summ_empty', tg_id)
    return msg

def user_carency_msg(tg_id: int) -> str:
    msg = get_msg_lang('user_carency_msg', tg_id)
    return msg

def carency_msg() -> str:
    msg = 'Выберите валюту, в которой вам удобно учавствовать в аукционах\n*$ - оплата crypto\n  € - оплата фиатом'
    return msg

def active_auctions(tg_id: int, type: str) -> str:
    if len(Auction.get_opened_auctions_by_type(type)) == 0:
        msg = get_msg_lang('no_auctions_msg', tg_id)
    else:
        msg = get_msg_lang('yes_auctions_msg', tg_id)
    return msg

def paymentURL(tg_id: int, type: str) -> str:
    if type == 'TON':
        msg = get_msg_lang('no_auctions_msg', tg_id)
    elif type == 'BITCOIN':
        msg = get_msg_lang('yes_auctions_msg', tg_id)
    return msg

def msg_auction(tg_id: int, auction_id: str) -> str:
    auction = Auction.get_auction_by_id_with_bid(auction_id)[0]
    bank = Bank.get_bank_by_auction_and_user(int(auction_id), tg_id)
    print(bank[0]['balance'] if len(bank) != 0 else 0, auction_id, tg_id)
    conf = Config()
    user_bid = Bid.get_bid_by_tg_id(tg_id, auction_id)
    user_bid = user_bid[0]['money'] if len(user_bid) > 0 else '-'
    last_price = auction['money'] if auction['money'] else auction['price']
    userLang = User.get_user_by_tg_id(tg_id)[0]['lang']
    msg = (get_msg_lang('lot_msg', tg_id) % (auction['name'], 
                                            auction['type'], 
                                            (f'{auction['volume']} л' if userLang == 'ru' else f'{auction['volume'] * 100} cl') if auction['volume'] else '-', 
                                            (f'{auction['abv']}%') if auction['abv'] else '-', 
                                            (auction['country_ru'] if userLang == 'ru' else auction['country_en']) if auction['country_ru'] or auction['country_en'] else '-', 
                                            (auction['brand']) if auction['brand'] else '-', 
                                            (auction['produser']) if auction['produser'] else '-', 
                                            (auction['description_ru'] if userLang == 'ru' else auction['description_en']) if auction['description_en'] or auction['description_ru'] else '-', 
                                            last_price, 
                                            bank[0]['balance'] if len(bank) != 0 else 0,
                                            conf.get_value('CURRENCY')))
    if user_bid != '-':
        msg += (get_msg_lang('lot_your_bid_msg', tg_id) % (user_bid, 
                                                conf.get_value('CURRENCY')))
        
    bids = Bid.get_bids_by_auction_id(auction_id)[-3:]
    bids = bids if len(bids) > 0 else []
    bids.reverse()
    msg += '\n'.join([str(i + 1) + '. ' + el['tg_link'][:3] + ' : ' + str(el['money']) for i, el in enumerate(bids)])
    return msg

def time_msg(auction_id: int) -> str:
    msg = str(Auction.get_time_auctions_by_id(auction_id)[0]['left_time'])
    return msg

def rules_msg(tg_id: int) -> str:
    msg = get_msg_rules_lang(tg_id)
    return msg

def winners_msg(tg_id: int, money: int) -> str:
    msg = (get_msg_lang('user_win_msg', tg_id) % money)
    return msg

def choose_pay_way(tg_id: int) -> str:
    msg = get_msg_lang('choose_pay_way', tg_id)
    return msg

def successful_payment_msg(tg_id: int, price: str) -> str:
    msg = (get_msg_lang('user_successful_payment_msg', tg_id) % price)
    return msg

def losers_msg(tg_id: int, auction: str) -> str:
    msg = (get_msg_lang('user_loss_msg', tg_id) % auction)
    return msg

def notification_start_msg(tg_id: int, name: str) -> str:
    msg = (get_msg_lang('notification_start_msg', tg_id) % name)
    return msg

def error_msg(tg_id: int):
    msg = get_msg_lang('error_msg', tg_id)
    return msg