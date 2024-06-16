from app.helper.i18n import get_msg_lang, get_msg_rules_lang
from app.helper.config import Config
from app.DB.DB import Auction, Bid
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

def menu_msg(tg_id: int) -> str:
    msg = get_msg_lang('user_menu_msg', tg_id)
    return msg

def active_auctions(tg_id: int, type: str) -> str:
    if len(Auction.get_opened_auctions_by_type(type)) == 0:
        msg = get_msg_lang('no_auctions_msg', tg_id)
    else:
        msg = get_msg_lang('yes_auctions_msg', tg_id)
    return msg

def msg_auction(tg_id: int, auction_id: str) -> str:
    auction = Auction.get_auction_by_id_with_bid(auction_id)[0]
    conf = Config()
    user_bid = Bid.get_bid_by_tg_id(tg_id, auction_id)
    user_bid = user_bid[0]['money'] if len(user_bid) > 0 else '-'
    last_price = auction['money'] if auction['money'] else auction['price']
    msg = (get_msg_lang('lot_msg', tg_id) % (auction['name'], 
                                             auction['type'], 
                                             auction['volume'], 
                                             auction['abv'], 
                                             auction['country'], 
                                             auction['brand'], 
                                             auction['produser'], 
                                             auction['description'],
                                             last_price, 
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

def losers_msg(tg_id: int, auction: str) -> str:
    msg = (get_msg_lang('user_loss_msg', tg_id) % auction)
    return msg

def notification_start_msg(tg_id: int, name: str) -> str:
    msg = (get_msg_lang('notification_start_msg', tg_id) % name)
    return msg

def error_msg(tg_id: int):
    msg = get_msg_lang('error_msg', tg_id)
    return msg