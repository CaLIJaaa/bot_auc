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
from app.DB.DB import User, Auction, Bid, Messages, Bank
from app.helper.config import Config
import os
from app.cryptoPay import cryptoPay

router = Router()
crypto = cryptoPay.Crypto()

class get_lang(StatesGroup):
    lang_add = State()

class Do_bid(StatesGroup):
    price = State()
    auction_id = State()
    user_id = State()

class Increace(StatesGroup):
    price = State()
    auction_id = State()
    user_id = State()

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
                reply_markup=kb_adm.get_settings_menu_kb(callback_data.lang_code)
            )
        else:
            await query.message.edit_text(
                text=msg.carency_msg(),
                reply_markup=kb_usr.get_carency_kb(callback_data.lang_code)
            )
    except BaseException:
        pass

@router.callback_query(cbd.CarencyCallback.filter())
async def answer_after_lang(query: CallbackQuery, callback_data: cbd.CarencyCallback):
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
            User.create_user(query.from_user.id, query.from_user.username, callback_data.lang_code, callback_data.carency)
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
async def answer_payment(query: CallbackQuery, callback_data: cbd.TypePayCallback, state: FSMContext):
    try:
        bank = Bank.get_bank_by_auction_and_user(callback_data.auction_id, callback_data.user_id)
        if bank[0]['statusPay'] == 'active':
            await query.message.edit_text(
                text=msg.payment_msg_error(callback_data.user_id),
                reply_markup=kb_usr.get_back_kb(query.from_user.id)
            )
            state.clear()
        else:
            payment_url = crypto.createInvoice(callback_data.pay_type, callback_data.amount, callback_data.auction_id, callback_data.user_id)
            if payment_url == None:
                await query.message.edit_text(
                    text="Server error",
                )
                return
            
            await query.message.edit_text(
                text=msg.payment_msg(callback_data.user_id),
                reply_markup=kb_usr.get_payment_kb(query.from_user.id, payment_url=payment_url)
            )
    except BaseException as e:
        print(e)

@router.callback_query(cbd.AuctionCallback.filter())
async def answer_auction_detail(query: CallbackQuery, callback_data: cbd.AuctionCallback):
    try:
        if len(Auction.get_opened_auction_by_id(callback_data.auction_id)) != 0 and \
            Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'] == None: # Аукцион без фото

            message = await query.message.edit_text(
                text=msg.msg_auction(query.from_user.id, callback_data.auction_id),
                reply_markup=kb_usr.get_auction_detail_kb(query.from_user.id, callback_data.auction_id)
            )
            user_message = Messages.get_message_by_auction_and_user(callback_data.auction_id, query.from_user.id)
            if not(user_message):
                Messages.add_message(callback_data.auction_id, query.from_user.id, message.message_id)
            else:
                Messages.update_message(callback_data.auction_id, query.from_user.id, message.message_id)
        elif len(Auction.get_opened_auction_by_id(callback_data.auction_id)) != 0 and \
            Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'] != None: # Аукцион с фото

            photo = FSInputFile(
                os.path.join(STATIC_PATH, 
                             Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'])
            )
            await query.message.delete()
            message = await query.message.answer_photo(
                photo=photo,
                caption=msg.msg_auction(query.from_user.id, callback_data.auction_id),
                reply_markup=kb_usr.get_auction_detail_kb(query.from_user.id, callback_data.auction_id)
            )
            print(message.message_id)
            user_message = Messages.get_message_by_auction_and_user(callback_data.auction_id, query.from_user.id)
            if not(user_message):
                Messages.add_message(callback_data.auction_id, query.from_user.id, message.message_id)
            else:
                Messages.update_message(callback_data.auction_id, query.from_user.id, message.message_id)
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

                message = await query.message.edit_text(
                    text=msg.msg_auction(query.from_user.id, callback_data.auction_id),
                    reply_markup=kb_usr.get_auction_detail_kb(query.from_user.id, callback_data.auction_id)
                )
                Messages.update_message(callback_data.auction_id, query.from_user.id, message.message_id)
            elif len(Auction.get_auction_by_id(callback_data.auction_id)) != 0 and \
                Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'] != None: # Аукцион с фото

                photo = FSInputFile(
                    os.path.join(STATIC_PATH, 
                                 Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'])
                )
                await query.message.delete()
                message = await query.message.answer_photo(
                    photo=photo,
                    caption=msg.msg_auction(query.from_user.id, callback_data.auction_id),
                    reply_markup=kb_usr.get_auction_detail_kb(query.from_user.id, callback_data.auction_id)
                )
                Messages.update_message(callback_data.auction_id, query.from_user.id, message.message_id)
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

@router.callback_query(cbd.BankCallback.filter(F.bank == "do_bid"))
async def do_bid(query: CallbackQuery, callback_data: cbd.BankCallback, state: FSMContext):
    try:
        if query.message.content_type == 'text':
            await query.message.edit_text(
                text=msg.do_bid_msg(query.from_user.id),
                reply_markup=kb_usr.get_back_kb(query.from_user.id)
            )
            await state.set_state(Do_bid.price)
            await state.update_data(user_id=callback_data.user_id)
            await state.update_data(auction_id=callback_data.auction_id)
        else:
            await query.message.delete()
            await query.message.answer(
                text=msg.do_bid_msg(query.from_user.id),
                reply_markup=kb_usr.get_back_kb(query.from_user.id)
            )
            await state.set_state(Do_bid.price)
            await state.update_data(user_id=callback_data.user_id)
            await state.update_data(auction_id=callback_data.auction_id)
    except BaseException:
        pass

@router.message(Do_bid.price, F.text)
async def photo_set_(message: Message, state: FSMContext):
    if message.text.isdigit():
        await message.answer(
            text=msg.do_bid_msg_sure(message.from_user.id, message.text),
            reply_markup=kb_usr.get_confirmation_do_bid(message.from_user.id)
        )
        await state.update_data(price=message.text)
    else:
        await message.delete()
        await message.answer(
            text=msg.menu_msg(message.from_user.id),
            reply_markup=kb_usr.get_type_auction_kb(message.from_user.id)
        )
        await state.clear()

@router.callback_query(cbd.DoBidCallback.filter())
async def do_bidCall(query: CallbackQuery, callback_data: cbd.DoBidCallback, state: FSMContext):
    try:
        if callback_data.status == 'yes':
            data = await state.get_data()
            bank = Bank.get_bank_by_auction_and_user(data['auction_id'], data['user_id'])
            if len(bank) == 0:
                print(bank, 'create')
                Bank.create_bank(data['auction_id'], data['user_id'])
                bank = Bank.get_bank_by_auction_and_user(data['auction_id'], data['user_id'])
            bank = bank[0]
            
            if int(bank['balance']) >= int(data['price']):
                bid = Bid.get_bid_by_tg_id(data['user_id'], data['auction_id'])
                if len(bid) == 0:
                    msg_done = Bid.add_bid(data['auction_id'], data['user_id'], int(data['price']))
                else:
                    msg_done = Bid.update_bid_by_tg_id(data['user_id'], data['auction_id'], int(data['price']))

                # bank_done = Bank.update_bank(data['auction_id'], data['user_id'], int(bank['balance']) - int(data['price']))
                if msg_done:
                    await query.answer(
                        text=str(msg_done)
                    )
                    await query.message.edit_text(
                        text=msg.do_bid_msg_success(data['user_id'], data['price']),
                        reply_markup=kb_usr.get_back_kb(data['user_id'])
                    )
            else:
                await query.message.edit_text(
                    text=msg.do_bid_msg_cancel(data['user_id']),
                    reply_markup=kb_usr.get_back_kb(data['user_id'])
                )
        else:
            await query.message.edit_text(
                text=msg.do_bid_msg(data['user_id']),
                reply_markup=kb_usr.get_back_kb(data['user_id'])
            )
        await state.clear()
        
    except BaseException as e:
        pass

# end do_bid start increace_bank

@router.callback_query(cbd.BankCallback.filter(F.bank == "increace_bank"))
async def get_back_menu(query: CallbackQuery, callback_data: cbd.BankCallback, state: FSMContext):
    try:
        print(callback_data)
        if query.message.content_type == 'text':
            await query.message.edit_text(
                text=msg.increace_bank_summ(query.from_user.id),
                reply_markup=kb_usr.get_back_kb(query.from_user.id)
            )
            await state.set_state(Increace.price)
            await state.update_data(user_id=callback_data.user_id)
            await state.update_data(auction_id=callback_data.auction_id)
        else:
            await query.message.delete()
            await query.message.answer(
                text=msg.increace_bank_summ(query.from_user.id),
                reply_markup=kb_usr.get_back_kb(query.from_user.id)
            )
            await state.set_state(Increace.price)
            await state.update_data(user_id=callback_data.user_id)
            await state.update_data(auction_id=callback_data.auction_id)
    except BaseException:
        pass

@router.message(Increace.price, F.text)
async def photo_set_(message: Message, state: FSMContext):
    if message.text.isdigit():
        data = await state.get_data()
        max_bid = Bid.get_max_bid_by_auction_id(data['auction_id'])
        max = 0 if not(max_bid) else max_bid
        if max >= int(message.text):
            await message.answer(
                text=msg.increace_bank_low(message.from_user.id, message.text),
                reply_markup=kb_usr.get_back_kb(message.from_user.id)
            )
            return
        await message.answer(
            text=msg.increace_bank_sure(message.from_user.id, message.text),
            reply_markup=kb_usr.get_confirmation_increace(message.from_user.id)
        )
        await state.update_data(price=message.text)
    else:
        await message.delete()
        await message.answer(
            text=msg.menu_msg(message.from_user.id),
            reply_markup=kb_usr.get_type_auction_kb(message.from_user.id)
        )
        await state.clear()

@router.callback_query(cbd.IncreaceCallback.filter())
async def do_bidCall(query: CallbackQuery, callback_data: cbd.IncreaceCallback, state: FSMContext):
    try:
        if callback_data.status == 'yes':
            data = await state.get_data()
            await query.message.edit_text(
                text=msg.pay_way_msg(data['user_id']),
                reply_markup=kb_usr.get_pay_way_kb(data['auction_id'], data['user_id'], data['price'])
            )
        else:
            await query.message.edit_text(
                text=msg.increace_bank_summ(data['user_id']),
                reply_markup=kb_usr.get_back_kb(data['user_id'])
            )
            await state.clear()
        
    except BaseException as e:
        pass

# end increace_bank start put_summ

@router.callback_query(cbd.BankCallback.filter(F.bank == "put_summ"))
async def get_back_menu(query: CallbackQuery, callback_data: cbd.BankCallback):
    try:
        print(callback_data.user_id)
        if query.message.content_type == 'text':
            await query.message.edit_text(
                text=msg.put_summ(callback_data.user_id),
                reply_markup=kb_usr.get_confirmation_put_summ(callback_data.user_id, callback_data.auction_id)
            )
        else:
            await query.message.delete()
            await query.message.answer(
                text=msg.put_summ(callback_data.user_id),
                reply_markup=kb_usr.get_confirmation_put_summ(callback_data.user_id, callback_data.auction_id)
            )
    except BaseException:
        pass

@router.callback_query(cbd.PutSummCallback.filter())
async def do_bidCall(query: CallbackQuery, callback_data: cbd.PutSummCallback, state: FSMContext):
    try:
        if callback_data.status == 'yes':
            bank = Bank.get_bank_by_auction_and_user(callback_data.auction_id, callback_data.user_id)
            print("bank")
            if len(bank) == 0 or bank[0]['balance'] == 0:
                await query.message.edit_text(
                    text=msg.put_summ_empty(query.from_user.id),
                    reply_markup=kb_usr.get_back_kb(query.from_user.id)
                )
                return

            status = crypto.transfer(bank[0]['asset'], bank[0]['balance'] - bank[0]['balance'] * 0.03, callback_data.auction_id, callback_data.user_id)
            print(status)
            if status != 'completed' or not(status):
                await query.message.edit_text(
                    text=msg.put_summ_error(callback_data.user_id),
                    reply_markup=kb_usr.get_back_kb(callback_data.user_id)
                )
                return
            
            await query.message.edit_text(
                text=msg.put_summ_success(callback_data.user_id),
                reply_markup=kb_usr.get_back_kb(callback_data.user_id)
            )
            Bid.delete_bid_by_user_and_auction(callback_data.user_id, callback_data.auction_id)
            Bank.update_bank(callback_data.auction_id, callback_data.user_id, 0)
        else:
            await query.message.edit_text(
                text=msg.menu_msg(callback_data.user_id),
                reply_markup=kb_usr.get_type_auction_kb(callback_data.user_id)
            )
            await state.clear()
        
    except BaseException as e:
        pass