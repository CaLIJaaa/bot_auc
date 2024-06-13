from app.helper.i18n import get_msg_lang
from app.helper.config import Config
from app.DB.DB import Auction, Bid, User

def menu_msg(tg_id: int):
    msg = get_msg_lang('admin_menu_action_msg', tg_id)
    return msg

def photo_set_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_create_pic_msg', tg_id)
    return msg

def name_set_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_create_name_msg', tg_id)
    return msg

def type_set_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_create_type_msg', tg_id)
    return msg

def volume_set_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_create_volume_msg', tg_id)
    return msg

def abv_set_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_create_abv_msg', tg_id)
    return msg

def country_set_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_create_country_msg', tg_id)
    return msg

def brand_set_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_create_brand_msg', tg_id)
    return msg

def produser_set_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_create_manufacturer_msg', tg_id)
    return msg

def description_set_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_create_description_msg', tg_id)
    return msg

def price_set_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_create_start_price_msg', tg_id)
    return msg

def time_leinght_set_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_create_duration_msg', tg_id)
    return msg

def auctions_msg(tg_id: int):
    msg = get_msg_lang('admin_menu_my_auc_decription_msg', tg_id)
    return msg

def settings_detail_step_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_settings_detail_step_msg', tg_id)
    return msg

def settings_detail_antisniper_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_settings_detail_antisniper_msg', tg_id)
    return msg

def settings_detail_currency_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_settings_detail_currency_msg', tg_id)
    return msg

def settings_detail_rules_msg(tg_id: int) -> str:
    conf = Config()
    msg = (get_msg_lang('admin_menu_settings_detail_rules_msg', tg_id) %
           (conf.get_value('SETTINGS')['ru'], ))
    return msg

def settings_detail_notification_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_settings_detail_notification_msg', tg_id)
    return msg

def get_admin_menu_settings_admin_nick_msg(tg_id: int, admin_id: int) -> str:
    admin = User.get_user_by_id(admin_id)[0]['tg_link']
    msg = (get_msg_lang('admin_menu_settings_admin_nick_msg', tg_id) % admin)
    return msg

def ge_admin_menu_settings_admin_delete_confirmation_msg(tg_id: int, admin_id: int) -> str:
    admin = User.get_user_by_id(admin_id)[0]['tg_link']
    msg = (get_msg_lang('admin_menu_settings_admin_delete_confirmation_msg', tg_id) % admin)
    return msg

def get_admin_menu_settings_make_admin_info_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_settings_make_admin_info_msg', tg_id)
    return msg

def get_admin_menu_settings_make_admin_done_msg(tg_id: int, admin: str) -> str:
    msg = (get_msg_lang('admin_menu_settings_make_admin_done_msg', tg_id) % admin)
    return msg

def get_admin_info_msg(user_id: int) -> str:
    msg = get_msg_lang('admin_info_msg', user_id)
    return msg

def get_admin_menu_settings_detail_notification_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_settings_detail_notification_msg', tg_id)
    return msg

def get_admin_menu_settings_make_bl_view_detail_msg(tg_id: int) -> str:
    users = User.get_banned()
    users = '\n'.join([str(i+1) + '. @' + user['tg_link'] for i, user in enumerate(users)])
    msg = (get_msg_lang('admin_menu_settings_make_bl_view_detail_msg', tg_id) % users)
    return msg

def get_admin_menu_settings_make_bl_add_info_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_settings_make_bl_add_info_msg', tg_id)
    return msg

def get_admin_menu_settings_make_bl_delete_info_msg(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_settings_make_bl_delete_info_msg', tg_id)
    return msg

def get_admin_menu_settings_make_bl_add_done_msg(tg_id: int, nickname: str) -> str:
    msg = (get_msg_lang('admin_menu_settings_make_bl_add_done_msg', tg_id) % nickname)
    return msg

def get_admin_menu_settings_make_bl_delete_done_msg(tg_id: int, nickname: str) -> str:
    msg = (get_msg_lang('admin_menu_settings_make_bl_delete_done_msg', tg_id) % nickname)
    return msg

def msg_auction(tg_id: int, auction_id: str) -> str:
    auction = Auction.get_auction_by_id_with_bid(auction_id)[0]
    conf = Config()
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
    return msg

def msg_bids(tg_id: int, auction_id: str) -> str:
    bids = Bid.get_bids_by_auction_id(auction_id)
    if len(bids) == 0:
        msg = get_msg_lang('admin_menu_bids_none_msg', tg_id)
    else:
        msg = ''
        max_len_name = max([len(i['tg_link']) for i in bids])
        for bid in bids:
            msg += '@' + bid['tg_link'] + ' '*(max_len_name-len(bid['tg_link'])) + ' : ' + str(bid['money']) + '\n'
    return msg[-4095:]

def msg_confirmation_delete(tg_id: int) -> str:
    msg = get_msg_lang('admin_menu_delete_confirmation_msg', tg_id)
    return msg

def winner_msg(tg_id: int, money: int, user_link: str) -> str:
    msg = (get_msg_lang('admin_notification_auction_end_msg', tg_id) % (str(money), user_link))
    return msg