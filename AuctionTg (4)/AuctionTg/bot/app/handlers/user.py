from aiogram import Router, F
from aiogram.filters import Command
from aiogram.filters.callback_data import CallbackData, CallbackQuery
from aiogram.types import Message, ReplyKeyboardRemove, FSInputFile
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

import app.keyboards.for_user as kb_usr
import app.keyboards.for_admin as kb_adm
import app.callbackdata.custom as cbd
import app.messages.for_user as msg
import app.messages.for_admin as msg_adm
from app.DB.DB import User, Auction, Bid
from app.helper.config import Config
import os
from app.cryptoPay import cryptoPay
import _thread

router = Router()
crypto = cryptoPay.Crypto()

class get_lang(StatesGroup):
    lang_add = State()

_thread.start_new_thread(crypto.checkInvoices, ())

STATIC_PATH = os.path.join(os.path.dirname(__file__), '../static/')

@router.message(Command("start")) 
async def cmd_start(message: Message):
    try:
        if len(User.get_user_by_tg_id(message.from_user.id)) == 0:
            await message.answer(
                msg.lang_msg(),
                reply_markup=kb_usr.get_lang_kb()
            )
        else:
            await message.answer(
                msg.start_msg(message.from_user.id),
                reply_markup=kb_usr.get_type_auction_kb(message.from_user.id)
            )
    except BaseException:
        pass

@router.message(Command("payment")) 
async def cmd_start(message: Message):
    try:
        if len(User.get_user_by_tg_id(message.from_user.id)) == 0:
            await message.answer(
                msg.lang_msg(),
                reply_markup=kb_usr.get_lang_kb()
            )
        else:
            print(crypto.getInvoices().get('items')[0])
            await message.answer(
                msg.pay_way_msg(message.from_user.id),
                reply_markup=kb_usr.get_pay_way_kb()
            )
    except BaseException:
        pass

@router.callback_query(cbd.LangCallback.filter())
async def answer_after_lang(query: CallbackQuery, callback_data: cbd.LangCallback):
    try:
        if User.is_user_admin(query.from_user.id):
            User.change_language(query.from_user.id, callback_data.lang_code)
            conf = Config()
            photo = FSInputFile(os.path.join(STATIC_PATH, conf.get_value('LOGO')))
            await query.message.delete()
            await query.message.answer_photo(
                photo=photo,
                reply_markup=kb_adm.get_settings_menu_kb(query.from_user.id)
            )
        else:
            User.create_user(query.from_user.id, query.from_user.username, callback_data.lang_code)
            await query.message.edit_text(
                text=msg.menu_msg(query.from_user.id),
                reply_markup=kb_usr.get_type_auction_kb(query.from_user.id)
            )
    except BaseException:
        pass

@router.callback_query(cbd.TypeAUCallback.filter())
async def answer_auctions(query: CallbackQuery, callback_data: cbd.TypeAUCallback):
    try:
        await query.message.edit_text(
            text=msg.active_auctions(query.from_user.id, callback_data.auction_type),
            reply_markup=kb_usr.get_auctions_kb(query.from_user.id, callback_data.auction_type)
        )
    except BaseException:
        pass

@router.callback_query(cbd.TypePayCallback.filter())
async def answer_payment(query: CallbackQuery, callback_data: cbd.TypePayCallback):
    try:
        payment_url = crypto.createInvoice(callback_data.pay_type, callback_data.amount, callback_data.auction_id)
        if payment_url == None:
            await query.message.edit_text(
                text="Server error",
            )
            return
        
        await query.message.edit_text(
            text=msg.winners_msg(callback_data.user_id, callback_data.amount),
            reply_markup=kb_usr.get_payment_kb(query.from_user.id, payment_url=payment_url)
        )
    except BaseException:
        pass

@router.callback_query(cbd.AuctionCallback.filter())
async def answer_auction_detail(query: CallbackQuery, callback_data: cbd.AuctionCallback):
    try:
        if len(Auction.get_opened_auction_by_id(callback_data.auction_id)) != 0 and \
            Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'] == None: # Аукцион без фото

            await query.message.edit_text(
                text=msg.msg_auction(query.from_user.id, callback_data.auction_id),
                reply_markup=kb_usr.get_auction_detail_kb(query.from_user.id, callback_data.auction_id)
            )
        elif len(Auction.get_opened_auction_by_id(callback_data.auction_id)) != 0 and \
            Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'] != None: # Аукцион с фото

            photo = FSInputFile(
                os.path.join(STATIC_PATH, 
                             Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'])
            )
            await query.message.delete()
            await query.message.answer_photo(
                photo=photo,
                caption=msg.msg_auction(query.from_user.id, callback_data.auction_id),
                reply_markup=kb_usr.get_auction_detail_kb(query.from_user.id, callback_data.auction_id)
            )
        else: # Аукцион уже не активен
            await query.message.edit_text(
                text=msg.menu_msg(query.from_user.id),
                reply_markup=kb_usr.get_type_auction_kb(query.from_user.id)
            )
    except BaseException:
        pass

@router.callback_query(cbd.BidMenuCallback.filter())
async def answer_add_bill(query: CallbackQuery, callback_data: cbd.BidMenuCallback):
    try:
        msg_done = None
        if len(Auction.get_auction_by_id(callback_data.auction_id)) != 0:
            msg_done = Bid.add_bid(callback_data.auction_id, query.from_user.id, callback_data.bid)
            await query.answer(
                text=str(msg_done)
            )
        if msg_done:
            if len(Auction.get_auction_by_id(callback_data.auction_id)) != 0 and \
                Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'] == None: # Аукцион без фото

                await query.message.edit_text(
                    text=msg.msg_auction(query.from_user.id, callback_data.auction_id),
                    reply_markup=kb_usr.get_auction_detail_kb(query.from_user.id, callback_data.auction_id)
                )
            elif len(Auction.get_auction_by_id(callback_data.auction_id)) != 0 and \
                Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'] != None: # Аукцион с фото

                photo = FSInputFile(
                    os.path.join(STATIC_PATH, 
                                 Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'])
                )
                await query.message.delete()
                await query.message.answer_photo(
                    photo=photo,
                    caption=msg.msg_auction(query.from_user.id, callback_data.auction_id),
                    reply_markup=kb_usr.get_auction_detail_kb(query.from_user.id, callback_data.auction_id)
                )
            else: # Аукцион уже не активен
                await query.message.edit_text(
                    text=msg.menu_msg(query.from_user.id),
                    reply_markup=kb_usr.get_type_auction_kb(query.from_user.id)
                )
    except BaseException:
        pass

@router.callback_query(cbd.AuctionHelpCallback.filter(F.page == "time"))
async def get_back_menu(query: CallbackQuery, callback_data: cbd.AuctionHelpCallback):
    try:
        await query.answer(
            text=msg.time_msg(callback_data.auction_id)
        )
        await answer_auction_detail(query, callback_data)
    except BaseException:
        pass

@router.callback_query(cbd.AuctionHelpCallback.filter(F.page == "update"))
async def get_back_menu(query: CallbackQuery, callback_data: cbd.AuctionHelpCallback):
    try:
        await answer_auction_detail(query, callback_data)
    except BaseException:
        pass

@router.callback_query(cbd.AuctionHelpCallback.filter(F.page == "rules"))
async def get_back_menu(query: CallbackQuery, callback_data: cbd.AuctionHelpCallback):
    try:
        await query.answer(
            text=msg.rules_msg(query.from_user.id),
            show_alert=True
        )
    except BaseException:
        pass


@router.callback_query(cbd.GetBackCallback.filter(F.page == "menu"))
async def get_back_menu(query: CallbackQuery, callback_data: cbd.GetBackCallback):
    try:
        if query.message.content_type == 'text':
            await query.message.edit_text(
                text=msg.menu_msg(query.from_user.id),
                reply_markup=kb_usr.get_type_auction_kb(query.from_user.id)
            )
        else:
            await query.message.delete()
            await query.message.answer(
                text=msg.menu_msg(query.from_user.id),
                reply_markup=kb_usr.get_type_auction_kb(query.from_user.id)
            )
    except BaseException:
        pass