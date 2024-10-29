from aiogram.types import ReplyKeyboardMarkup
from aiogram import types
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
import app.callbackdata.custom as cbd
from app.helper.i18n import get_msg_lang
from app.DB.DB import Auction
from app.helper.config import Config

def get_lang_kb() -> ReplyKeyboardMarkup:
    """
    Клавиатура с выбором языка
    """
    keyboard = InlineKeyboardBuilder()
    keyboard.button(
        text = 'En',
        callback_data=cbd.LangCallback(lang_code='en')
    )
    keyboard.button(
        text = 'Ru',
        callback_data=cbd.LangCallback(lang_code='ru')
    )
    return keyboard.as_markup()

def get_carency_kb(lang_code: str) -> ReplyKeyboardMarkup:
    """
    Клавиатура с выбором языка
    """
    keyboard = InlineKeyboardBuilder()
    keyboard.button(
        text = '$',
        callback_data=cbd.CarencyCallback(carency='$', lang_code=lang_code)
    )
    keyboard.button(
        text = '€',
        callback_data=cbd.CarencyCallback(carency='€', lang_code=lang_code)
    )
    return keyboard.as_markup()

def get_type_auction_kb(user_id: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура с выбором типа напитка
    """
    keyboard = InlineKeyboardBuilder()
    keyboard.button(
        text = get_msg_lang('wine_msg', user_id),
        callback_data=cbd.TypeAUCallback(auction_type='wine')
    )
    keyboard.button(
        text = get_msg_lang('whiskey_msg', user_id),
        callback_data=cbd.TypeAUCallback(auction_type='whiskey')
    )
    return keyboard.as_markup()

def get_payment_kb(user_id: int, payment_url: str) -> ReplyKeyboardMarkup:
    """
    Клавиатура с оплатой
    """
    keyboard = InlineKeyboardBuilder()
    keyboard.button(
        text = get_msg_lang('payment_msg', user_id),
        url=payment_url
    )
    return keyboard.as_markup()

def get_pay_way_kb(auction_id: int, user_id: int, amount: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура с выбором типа оплаты
    """
    keyboard = InlineKeyboardBuilder()
    keyboard.button(
        text = "USDT",
        callback_data=cbd.TypePayCallback(pay_type='USDT', auction_id=auction_id, user_id=user_id, amount=amount)
    )
    keyboard.button(
        text = "USDC",
        callback_data=cbd.TypePayCallback(pay_type='USDC', auction_id=auction_id, user_id=user_id, amount=amount)
    )
    keyboard.adjust(1)
    return keyboard.as_markup()

def get_auctions_kb(user_id: int, type: str) -> ReplyKeyboardMarkup:
    """
    Клавиатура с аукционами
    """
    keyboard = InlineKeyboardBuilder()
    auctions = Auction.get_opened_auctions_by_type(type)
    for auction in auctions:
        keyboard.button(
            text = auction['name'],
            callback_data=cbd.AuctionCallback(auction_id=auction['id'])
        )
    keyboard.button(
        text = get_msg_lang('get_back_button', user_id),
        callback_data=cbd.GetBackCallback(page='menu')
    )
    keyboard.adjust(1)
    return keyboard.as_markup()

def get_auction_detail_kb(user_id: int, auction_id: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура для аукциона
    """
    keyboard = InlineKeyboardBuilder()
    conf = Config()
    CURRENCY = conf.get_value('CURRENCY')
    STEP = conf.get_value('STEP').split(',')
    keyboard = InlineKeyboardBuilder()
    # for i in STEP:
    #     keyboard.button(
    #         text = '+' + i + CURRENCY,
    #         callback_data=cbd.BidMenuCallback(auction_id=auction_id, bid=int(i))
    #     )
    keyboard.row(
        types.InlineKeyboardButton(
            text = get_msg_lang('do_bid', user_id),
            callback_data=cbd.BankCallback(bank='do_bid', auction_id=auction_id, user_id=user_id).pack(),
        ),
        width=1
    )
    keyboard.adjust(1)
    keyboard.row(
        types.InlineKeyboardButton(
            text = get_msg_lang('increace_bank', user_id),
            callback_data=cbd.BankCallback(bank='increace_bank', auction_id=auction_id, user_id=user_id).pack()
        )
    )
    keyboard.row(
        types.InlineKeyboardButton(
            text = get_msg_lang('get_out', user_id),
            callback_data=cbd.BankCallback(bank='put_summ', auction_id=auction_id, user_id=user_id).pack()
        )
    )
    keyboard.row(
        types.InlineKeyboardButton(
            text = '⌛️',
            callback_data=cbd.AuctionHelpCallback(page='time', auction_id=auction_id).pack()
        ),
        types.InlineKeyboardButton(
            text = '🔄',
            callback_data=cbd.AuctionHelpCallback(page='update', auction_id=auction_id).pack()
        ),
        types.InlineKeyboardButton(
            text = '⁉️',
            callback_data=cbd.AuctionHelpCallback(page='rules', auction_id=auction_id).pack()
        )
    )
    keyboard.row(
        types.InlineKeyboardButton(
            text = get_msg_lang('get_back_button', user_id),
            callback_data=cbd.GetBackCallback(page='menu').pack()
        )
    )
    return keyboard.as_markup()

def get_confirmation_do_bid(user_id: str) -> ReplyKeyboardMarkup:
    """
    Клавиатура с подтверждением удаления
    """
    keyboard = InlineKeyboardBuilder()

    keyboard.button(
        text = get_msg_lang('admin_menu_yes_msg', user_id),
        callback_data=cbd.DoBidCallback(status="yes")
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_no_msg', user_id),
        callback_data=cbd.DoBidCallback(status="no")
    )
    keyboard.adjust(2)
    return keyboard.as_markup()

def get_confirmation_increace(user_id: str) -> ReplyKeyboardMarkup:
    """
    Клавиатура с подтверждением удаления
    """
    keyboard = InlineKeyboardBuilder()

    keyboard.button(
        text = get_msg_lang('admin_menu_yes_msg', user_id),
        callback_data=cbd.IncreaceCallback(status="yes")
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_no_msg', user_id),
        callback_data=cbd.IncreaceCallback(status="no")
    )
    keyboard.adjust(2)
    return keyboard.as_markup()

def get_confirmation_put_summ(user_id: int, auction_id: int) -> ReplyKeyboardMarkup:
    """
    Клавиатура с подтверждением удаления
    """
    keyboard = InlineKeyboardBuilder()

    keyboard.button(
        text = get_msg_lang('admin_menu_yes_msg', user_id),
        callback_data=cbd.PutSummCallback(status="yes", user_id=user_id, auction_id=auction_id)
    )
    keyboard.button(
        text = get_msg_lang('admin_menu_no_msg', user_id),
        callback_data=cbd.PutSummCallback(status="no", user_id=user_id, auction_id=auction_id)
    )
    keyboard.adjust(2)
    return keyboard.as_markup()

def get_back_kb(user_id: int) -> ReplyKeyboardMarkup:
    keyboard = InlineKeyboardBuilder()
    keyboard.button(
        text = get_msg_lang('get_back_button', user_id),
        callback_data=cbd.GetBackCallback(page='menu')
    )
    return keyboard.as_markup()