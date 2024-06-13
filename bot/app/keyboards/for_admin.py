from aiogram.types import ReplyKeyboardMarkup
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
import app.callbackdata.custom as cbd
from app.helper.i18n import get_msg_lang
from app.helper.config import Config
from app.DB.DB import Auction, User, Bid

def get_admin_menu_kb(user_id: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура админ меню
    """
    keyboard = InlineKeyboardBuilder()
    keyboard.button(
        text = get_msg_lang('admin_menu_create_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_create')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_my_auc_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_my_auc')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_settings_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_settings')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_help_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_help')
    )
    keyboard.adjust(1)
    return keyboard.as_markup()

def get_type_auction_kb(user_id: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура с выбором типа напитка
    """
    keyboard = InlineKeyboardBuilder()
    keyboard.button(
        text = get_msg_lang('wine_msg', user_id),
        callback_data=cbd.CreationAuctionTypeCallback(type='wine')
    )
    keyboard.button(
        text = get_msg_lang('whiskey_msg', user_id),
        callback_data=cbd.CreationAuctionTypeCallback(type='whiskey')
    )
    return keyboard.as_markup()

def get_skip_auction_creation_kb(user_id: int, value) -> ReplyKeyboardMarkup:
    """
    Клавиатура с кнопкой `SKIP`
    """
    keyboard = InlineKeyboardBuilder()
    keyboard.button(
        text = 'SKIP',
        callback_data=cbd.SkipCallback(value=value)
    )
    return keyboard.as_markup()

def get_time_auction_creation_kb(user_id: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура с кнопками времени
    """
    keyboard = InlineKeyboardBuilder()
    keyboard.button(
        text = get_msg_lang('admin_menu_create_duration_1_msg', user_id),
        callback_data=cbd.SkipCallback(value='1000')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_create_duration_2_msg', user_id),
        callback_data=cbd.SkipCallback(value='1500')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_create_duration_3_msg', user_id),
        callback_data=cbd.SkipCallback(value='3000')
    )

    keyboard.button(
        text = get_msg_lang('admin_menu_create_duration_4_msg', user_id),
        callback_data=cbd.SkipCallback(value='10000')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_create_duration_5_msg', user_id),
        callback_data=cbd.SkipCallback(value='30000')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_create_duration_6_msg', user_id),
        callback_data=cbd.SkipCallback(value='60000')
    )

    keyboard.button(
        text = get_msg_lang('admin_menu_create_duration_7_msg', user_id),
        callback_data=cbd.SkipCallback(value='240000')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_create_duration_8_msg', user_id),
        callback_data=cbd.SkipCallback(value='360000')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_create_duration_9_msg', user_id),
        callback_data=cbd.SkipCallback(value='720000')
    )
    keyboard.adjust(3)
    return keyboard.as_markup()

def get_settings_menu_kb(user_id: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура с настройками
    """
    keyboard = InlineKeyboardBuilder()
    conf = Config()
    keyboard.button(
        text = (get_msg_lang('admin_menu_settings_step_msg', user_id) % (
            conf.get_value('STEP')
        )),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_settings_step')
    )
    keyboard.button(
        text = (get_msg_lang('admin_menu_settings_antisniper_msg', user_id) % (
            conf.get_value('ANTISNIPER')
        )),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_settings_antisniper')
    )
    keyboard.button(
        text = (get_msg_lang('admin_menu_settings_currency_msg', user_id) % (
            conf.get_value('CURRENCY')
        )),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_settings_currency')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_settings_change_rules_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_settings_change_rules')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_settings_admins_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_settings_admins')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_bl_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='blacklist_menu')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_settings_payment_method_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_settings_payment_method')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_settings_notifications_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_settings_notifications')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_settings_language_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_settings_language')
    )
    keyboard.button(
        text = get_msg_lang('get_back_button', user_id),
        callback_data=cbd.AdminMenuCallback(page='menu')
    )
    keyboard.adjust(1)
    return keyboard.as_markup()

def get_admins_kb(user_id: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура с админами
    """
    keyboard = InlineKeyboardBuilder()
    admins = User.get_admins()
    for admin in admins:
        keyboard.button(
            text = '@' + admin['tg_link'],
            callback_data=cbd.DeleteAdminsCallback(page='confirmation', id=admin['id'])
        )

    keyboard.button(
        text = get_msg_lang('admin_menu_settings_make_admin_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='new_admin')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_back_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_settings')
    )
    keyboard.adjust(1)
    return keyboard.as_markup()

def get_admins_delete_kb(user_id: int, admin_id: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура с админом
    """
    keyboard = InlineKeyboardBuilder()
    keyboard.button(
        text = get_msg_lang('admin_menu_settings_admin_delete_msg', user_id),
        callback_data=cbd.DeleteAdminsCallback(page='get_conf', id=admin_id)
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_back_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_settings_admins')
    )
    keyboard.adjust(1)
    return keyboard.as_markup()

def get_admins_delete_confirmation_kb(user_id: int, admin_id: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура с подтвержением удаления админа
    """
    keyboard = InlineKeyboardBuilder()
    keyboard.button(
        text = get_msg_lang('admin_menu_yes_msg', user_id),
        callback_data=cbd.DeleteAdminsCallback(page='yes', id=admin_id)
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_no_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_settings_admins')
    )
    keyboard.adjust(1)
    return keyboard.as_markup()

def get_bl_kb(user_id: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура с ЧС
    """
    keyboard = InlineKeyboardBuilder()
    keyboard.button(
        text = get_msg_lang('admin_menu_settings_make_bl_view_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='view_bl')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_settings_make_bl_add_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='add_to_bl')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_settings_make_bl_delete_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='delete_from_bl')
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_back_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_settings')
    )
    keyboard.adjust(1)
    return keyboard.as_markup()

def admin_notifications_kb(user_id: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура с настройкой уведомлений
    """
    keyboard = InlineKeyboardBuilder()
    conf = Config()
    win = conf.get_value('WIN_NOTIFICATION')
    lose = conf.get_value('LOOSE_NOTIFICATION')
    win = '● ' if win else '○ '
    lose = '● ' if lose else '○ '

    keyboard.button(
        text = win + get_msg_lang('admin_menu_settings_detail_notification_win_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_settings_detail_notification_win')
    )
    keyboard.button(
        text = lose + get_msg_lang('admin_menu_settings_detail_notification_lose_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_settings_detail_notification_lose')
    )

    keyboard.button(
        text = get_msg_lang('admin_menu_back_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_settings')
    )
    keyboard.adjust(1)
    return keyboard.as_markup()

def get_auctions_kb(user_id: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура с аукционами админа
    """
    keyboard = InlineKeyboardBuilder()
    auctions = Auction.get_auction_by_user_id(user_id)
    auctions = auctions[-15:]
    for auction in auctions:
        emoj = ''
        match auction['status']:
            case 'created':
                emoj = '○ '
            case 'opened':
                emoj = '◍ '
            case 'closed':
                emoj = '● '
        keyboard.button(
            text = emoj + auction['name'],
            callback_data=cbd.AuctionAdminCallback(auction_id=auction['id'])
        )
    keyboard.button(
        text = get_msg_lang('get_back_button', user_id),
        callback_data=cbd.AdminMenuCallback(page='menu')
    )
    keyboard.adjust(1)
    return keyboard.as_markup()

def get_actions_admin_kb(user_id: int, auction_id: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура с действиями над аукционами админа
    """
    keyboard = InlineKeyboardBuilder()

    keyboard.button(
        text = get_msg_lang('admin_menu_publish_msg', user_id),
        callback_data=cbd.AuctionSettingsCallback(action='publish', auction_id=auction_id)
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_edit_msg', user_id),
        callback_data=cbd.AuctionSettingsCallback(action='edit', auction_id=auction_id)
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_bids_msg', user_id),
        callback_data=cbd.AuctionSettingsCallback(action='bids', auction_id=auction_id)
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_delete_msg', user_id),
        callback_data=cbd.AuctionSettingsCallback(action='delete', auction_id=auction_id)
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_back_msg', user_id),
        callback_data=cbd.AdminMenuCallback(page='admin_menu_my_auc')
    )
    keyboard.adjust(1)
    return keyboard.as_markup()

def get_edit_kb(user_id: str, auction_id: str) -> ReplyKeyboardMarkup:
    """
    Клавиатура для редактирования аукциона
    """
    keyboard = InlineKeyboardBuilder()

    keyboard.button(
        text = get_msg_lang('admin_menu_edit_picture_msg', user_id),
        callback_data=cbd.EditAuctionCallback(page='picture', auction_id=auction_id)
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_edit_description_msg', user_id),
        callback_data=cbd.EditAuctionCallback(page='description', auction_id=auction_id)
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_edit_price_msg', user_id),
        callback_data=cbd.EditAuctionCallback(page='price', auction_id=auction_id)
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_edit_duration_msg', user_id),
        callback_data=cbd.EditAuctionCallback(page='time_leinght', auction_id=auction_id)
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_back_msg', user_id),
        callback_data=cbd.AuctionAdminCallback(auction_id=auction_id)
    )
    keyboard.adjust(1)
    return keyboard.as_markup()

def get_confirmation_detele_auction(user_id: str, auction_id: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура с подтверждением удаления
    """
    keyboard = InlineKeyboardBuilder()

    keyboard.button(
        text = get_msg_lang('admin_menu_yes_msg', user_id),
        callback_data=cbd.AuctionSettingsCallback(action='delete_yes', auction_id=auction_id)
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_no_msg', user_id),
        callback_data=cbd.AuctionAdminCallback(auction_id=auction_id)
    )
    keyboard.adjust(2)
    return keyboard.as_markup()

def get_bids_list(user_id: int, auction_id: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура со ставками
    """
    keyboard = InlineKeyboardBuilder()
    bids = Bid.get_bids_by_auction_id(auction_id)
    bids = bids[-15:]
    for bid in bids:
        keyboard.button(
            text = '@' + bid['tg_link'] + ' : ' + str(bid['money']) + '\n',
            callback_data=cbd.BidSettingsCallback(bid_id=bid['id'], action='view', auction_id=auction_id)
        )
    keyboard.button(
        text = get_msg_lang('admin_menu_back_msg', user_id),
        callback_data=cbd.AuctionAdminCallback(auction_id=auction_id)
    )
    keyboard.adjust(1)
    return keyboard.as_markup()

def get_edit_bid(user_id: int, bid_id: int,  auction_id: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура с настройкой ставки
    """
    keyboard = InlineKeyboardBuilder()
    bid = Bid.get_bid_by_id(bid_id)
    bid_id = bid[0]['id'] if len(bid) > 0 else -1
    keyboard.button(
        text = get_msg_lang('admin_menu_bids_delete_msg', user_id),
        callback_data=cbd.BidSettingsCallback(bid_id=bid_id, action='delete', auction_id=auction_id)
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_bids_ban_delete_msg', user_id),
        callback_data=cbd.BidSettingsCallback(bid_id=bid_id, action='ban_delete', auction_id=auction_id)
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_back_msg', user_id),
        callback_data=cbd.AuctionSettingsCallback(action='bids', auction_id=auction_id)
    )
    keyboard.adjust(1)
    return keyboard.as_markup()

def admin_get_back_kb(user_id: int, page: str = None) -> ReplyKeyboardMarkup:
    """
    Клавиатура с кнопкой назад
    """
    keyboard = InlineKeyboardBuilder()
    if page:
        keyboard.button(
            text = get_msg_lang('admin_menu_back_msg', user_id),
            callback_data=cbd.AdminMenuCallback(page=page)
        )
    else:
        keyboard.button(
            text = get_msg_lang('get_back_button', user_id),
            callback_data=cbd.AdminMenuCallback(page='menu')
        )
    keyboard.adjust(1)
    return keyboard.as_markup()