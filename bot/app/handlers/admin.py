from aiogram import Router, F
from aiogram.filters import Command
from aiogram.filters.callback_data import CallbackData, CallbackQuery
from aiogram.types import Message, ReplyKeyboardRemove, FSInputFile
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import re
from hashlib import sha256

import app.keyboards.for_user as kb_usr
import app.keyboards.for_admin as kb_adm
import app.callbackdata.custom as cbd
import app.messages.for_user as msg_user
import app.messages.for_admin as msg
from app.DB.DB import User, Auction, Bid
from app.helper.config import Config

router = Router()

class CreateNewAuction(StatesGroup):
    photo_set = State()
    name_set = State()
    type_set = State()
    volume_set = State()
    abv_set = State()
    country_set = State()
    brand_set = State()
    produser_set = State()
    description_set = State()
    price_set = State()
    time_leinght_set = State()

@router.message(Command("menu")) 
async def cmd_start(message: Message):
    try:
        if User.is_user_admin(message.from_user.id):
            await message.answer(
                msg.menu_msg(message.from_user.id),
                reply_markup=kb_adm.get_admin_menu_kb(message.from_user.id)
            )
        else:
            await message.answer(
                msg_user.menu_msg(message.from_user.id),
                reply_markup=kb_usr.get_type_auction_kb(message.from_user.id)
            )
    except BaseException:
        pass
# =================================================================
@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "admin_menu_create"))
async def admin_menu_create(query: CallbackQuery, callback_data: cbd.AdminMenuCallback, state: FSMContext):
    if User.is_user_admin(query.from_user.id):
        await query.message.delete()
        await query.message.answer(
            msg.photo_set_msg(query.from_user.id)
        )
        await state.set_state(CreateNewAuction.photo_set)

@router.message(CreateNewAuction.photo_set, F.photo)
async def photo_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            file = message.photo[-1]
            file_id = file.file_id
            file_unique_id = file.file_unique_id
            file = await message.bot.get_file(file_id)
            file_path = file.file_path
            extention = file_path.split('.')[-1]
            await message.bot.download_file(file_path, f"bot/app/static/{file_unique_id}.{extention}")
            await state.update_data(picture=f"{file_unique_id}.{extention}")
            await message.answer(
                text=msg.name_set_msg(message.from_user.id)
            )
        await state.set_state(CreateNewAuction.name_set)
    except BaseException:
        pass

@router.message(CreateNewAuction.name_set, F.text)
async def name_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            await state.update_data(name=message.text[:100])

            await message.answer(
                text=msg.type_set_msg(message.from_user.id),
                reply_markup=kb_adm.get_type_auction_kb(message.from_user.id)
            )
        await state.set_state(CreateNewAuction.type_set)
    except BaseException:
        pass

@router.callback_query(CreateNewAuction.type_set, cbd.CreationAuctionTypeCallback.filter())
async def type_set_(query: CallbackQuery, state: FSMContext, callback_data: cbd.CreationAuctionTypeCallback):
    try:
        if User.is_user_admin(query.from_user.id):
            await state.update_data(type=callback_data.type)
            
            await query.message.edit_text(
                text=msg.volume_set_msg(query.from_user.id),
                reply_markup=kb_adm.get_skip_auction_creation_kb(query.from_user.id, 'volume')
            )
        await state.set_state(CreateNewAuction.volume_set)
    except BaseException:
        pass

@router.message(CreateNewAuction.volume_set, F.text)
async def volume_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            volume = re.findall(r'[0-9]{1,}', message.text)
            volume = int(volume[0]) if len(volume) > 0 else 0
            await state.update_data(volume=volume)
            await message.answer(
                text=msg.abv_set_msg(message.from_user.id),
                reply_markup=kb_adm.get_skip_auction_creation_kb(message.from_user.id, 'abv')
            )
        await state.set_state(CreateNewAuction.abv_set)
    except BaseException:
        pass

@router.message(CreateNewAuction.abv_set, F.text)
async def abv_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            volume = re.findall(r'[0-9]{1,}', message.text)
            volume = int(volume[0]) if len(volume) > 0 else 0
            await state.update_data(abv=volume)
            await message.answer(
                text=msg.country_set_msg(message.from_user.id),
                reply_markup=kb_adm.get_skip_auction_creation_kb(message.from_user.id, 'country')
            )
        await state.set_state(CreateNewAuction.country_set)
    except BaseException:
        pass

@router.message(CreateNewAuction.country_set, F.text)
async def country_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            await state.update_data(country=message.text[:100])
            await message.answer(
                text=msg.brand_set_msg(message.from_user.id),
                reply_markup=kb_adm.get_skip_auction_creation_kb(message.from_user.id, 'brand')
            )
        await state.set_state(CreateNewAuction.brand_set)
    except BaseException:
        pass

@router.message(CreateNewAuction.brand_set, F.text)
async def brand_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            await state.update_data(brand=message.text[:100])
            await message.answer(
                text=msg.produser_set_msg(message.from_user.id),
                reply_markup=kb_adm.get_skip_auction_creation_kb(message.from_user.id, 'produser')
            )
        await state.set_state(CreateNewAuction.produser_set)
    except BaseException:
        pass

@router.message(CreateNewAuction.produser_set, F.text)
async def produser_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            await state.update_data(produser=message.text[:100])
            await message.answer(
                text=msg.description_set_msg(message.from_user.id),
                reply_markup=kb_adm.get_skip_auction_creation_kb(message.from_user.id, 'description')
            )
        await state.set_state(CreateNewAuction.description_set)
    except BaseException:
        pass

@router.message(CreateNewAuction.description_set, F.text)
async def description_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            await state.update_data(description=message.text[:200])
            await message.answer(
                text=msg.price_set_msg(message.from_user.id)
            )
        await state.set_state(CreateNewAuction.price_set)
    except BaseException:
        pass

@router.message(CreateNewAuction.price_set, F.text)
async def price_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            price = re.findall(r'[0-9]{1,}', message.text)
            price = float(price[0]) if len(price) > 0 else 0.0
            await state.update_data(price=price)
            await message.answer(
                text=msg.time_leinght_set_msg(message.from_user.id),
                reply_markup=kb_adm.get_time_auction_creation_kb(message.from_user.id)
            )
        await state.set_state(CreateNewAuction.time_leinght_set)
    except BaseException:
        pass

@router.message(CreateNewAuction.time_leinght_set, F.text)
async def time_leinght_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            time_leinght = re.findall(r'[0-9]{1,}', message.text)
            time_leinght = time_leinght[0] if len(time_leinght) > 0 else 0
            time_leinght = int(time_leinght) * 10000 if int(time_leinght) < 800 else '800'
            await state.update_data(time_leinght=time_leinght)
            user_data = await state.get_data()
            id = Auction.create_auction(created_by=message.from_user.id, **user_data)

            if Auction.get_auction_by_id(id)[0]['picture'] == None: # Аукцион без фото


                await message.answer(
                    text=msg.msg_auction(message.from_user.id, id),
                    reply_markup=kb_adm.get_actions_admin_kb(message.from_user.id, id)
                )
            elif Auction.get_auction_by_id(id)[0]['picture'] != None: # Аукцион с фото

                photo = FSInputFile('bot/app/static/' + Auction.get_auction_by_id(id)[0]['picture'])

                await message.answer_photo(
                    photo=photo,
                    caption=msg.msg_auction(message.from_user.id, id),
                    reply_markup=kb_adm.get_actions_admin_kb(message.from_user.id, id)
                )
        await state.clear()
    except BaseException:
        pass

@router.callback_query(cbd.SkipCallback.filter())
async def type_set_(query: CallbackQuery, state: FSMContext, callback_data: cbd.SkipCallback):
    try:
        if User.is_user_admin(query.from_user.id):
            match callback_data.value:
                case 'volume': 
                    await state.update_data({callback_data.value: None})
                    await state.set_state(CreateNewAuction.abv_set)
                    await query.message.answer(
                        text=msg.abv_set_msg(query.from_user.id),
                        reply_markup=kb_adm.get_skip_auction_creation_kb(query.from_user.id, 'abv')
                    )
                case 'abv':
                    await state.update_data({callback_data.value: None})
                    await state.set_state(CreateNewAuction.country_set)
                    await query.message.answer(
                        text=msg.country_set_msg(query.from_user.id),
                        reply_markup=kb_adm.get_skip_auction_creation_kb(query.from_user.id, 'country')
                    )
                case 'country':
                    await state.update_data({callback_data.value: None})
                    await state.set_state(CreateNewAuction.brand_set)
                    await query.message.answer(
                        text=msg.brand_set_msg(query.from_user.id),
                        reply_markup=kb_adm.get_skip_auction_creation_kb(query.from_user.id, 'brand')
                    )
                case 'brand':
                    await state.update_data({callback_data.value: None})
                    await state.set_state(CreateNewAuction.produser_set)
                    await query.message.answer(
                        text=msg.produser_set_msg(query.from_user.id),
                        reply_markup=kb_adm.get_skip_auction_creation_kb(query.from_user.id, 'produser')
                    )
                case 'produser':
                    await state.update_data({callback_data.value: None})
                    await state.set_state(CreateNewAuction.description_set)
                    await query.message.answer(
                        text=msg.description_set_msg(query.from_user.id),
                        reply_markup=kb_adm.get_skip_auction_creation_kb(query.from_user.id, 'description')
                    )
                case 'description':
                    await state.update_data({callback_data.value: None})
                    await state.set_state(CreateNewAuction.price_set)
                    await query.message.answer(
                        text=msg.price_set_msg(query.from_user.id)
                    )
                case _:
                    await state.update_data(time_leinght=callback_data.value)
                    user_data = await state.get_data()
                    id = Auction.create_auction(created_by=query.from_user.id, **user_data)
                    if Auction.get_auction_by_id(id)[0]['picture'] == None: # Аукцион без фото

                        await query.message.answer(
                            text=msg.msg_auction(query.from_user.id, id),
                            reply_markup=kb_adm.get_actions_admin_kb(query.from_user.id, id)
                        )
                    elif Auction.get_auction_by_id(id)[0]['picture'] != None: # Аукцион с фото

                        photo = FSInputFile('bot/app/static/' + Auction.get_auction_by_id(id)[0]['picture'])

                        await query.message.answer_photo(
                            photo=photo,
                            caption=msg.msg_auction(query.from_user.id, id),
                            reply_markup=kb_adm.get_actions_admin_kb(query.from_user.id, id)
                        )
    except BaseException:
        pass

# ==============
@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "admin_menu_my_auc"))
async def admin_menu_my_auc(query: CallbackQuery, callback_data: cbd.AdminMenuCallback):
    if User.is_user_admin(query.from_user.id):
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            caption=msg.auctions_msg(query.from_user.id),
            reply_markup=kb_adm.get_auctions_kb(query.from_user.id)
        )

@router.callback_query(cbd.AuctionAdminCallback.filter())
async def answer_auction_detail(query: CallbackQuery, callback_data: cbd.AuctionAdminCallback):
    if User.is_user_admin(query.from_user.id):
        try:
            if Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'] == None: # Аукцион без фото

                await query.message.delete()
                await query.message.answer(
                    text=msg.msg_auction(query.from_user.id, callback_data.auction_id),
                    reply_markup=kb_adm.get_actions_admin_kb(query.from_user.id, callback_data.auction_id)
                )
            elif Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'] != None: # Аукцион с фото

                photo = FSInputFile('bot/app/static/' + Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'])
                await query.message.delete()
                await query.message.answer_photo(
                    photo=photo,
                    caption=msg.msg_auction(query.from_user.id, callback_data.auction_id),
                    reply_markup=kb_adm.get_actions_admin_kb(query.from_user.id, callback_data.auction_id)
                )
        except BaseException:
            pass

@router.callback_query(cbd.AuctionSettingsCallback.filter(F.action == "publish"))
async def delete_auction(query: CallbackQuery, callback_data: cbd.AuctionSettingsCallback):
    try:
        if User.is_user_admin(query.from_user.id):
            Auction.open_auction_by_id(callback_data.auction_id)
            await query.answer(
                text='Done'
            )
            auction = Auction.get_auction_by_id(callback_data.auction_id)
            auction = auction[0]['name'] if len(auction) > 0 else '-'
            users = User.get_users()
            for user in users:
                await query.bot.send_message(
                    chat_id=user['tg_id'],
                    text=msg_user.notification_start_msg(user['tg_id'], auction)
                )
                if len(Auction.get_opened_auction_by_id(callback_data.auction_id)) != 0 and \
                    Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'] != None: # Аукцион с фото

                    photo = FSInputFile('bot/app/static/' + Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'])
                    await query.bot.send_photo(
                        chat_id=user['tg_id'],
                        photo=photo,
                        caption=msg_user.msg_auction(query.from_user.id, callback_data.auction_id),
                        reply_markup=kb_usr.get_auction_detail_kb(query.from_user.id, callback_data.auction_id)
                    )
    except BaseException:
        await query.answer(
                text='Error'
            )
# ==== EDITING ======
@router.callback_query(cbd.AuctionSettingsCallback.filter(F.action == "edit"))
async def edit_auction(query: CallbackQuery, callback_data: cbd.AuctionSettingsCallback):
    try:
        if User.is_user_admin(query.from_user.id):
            if Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'] == None: # Аукцион без фото

                await query.message.delete()
                await query.message.answer(
                    text=msg.msg_auction(query.from_user.id, callback_data.auction_id),
                    reply_markup=kb_adm.get_edit_kb(query.from_user.id, callback_data.auction_id)
                )
            elif Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'] != None: # Аукцион с фото

                photo = FSInputFile('bot/app/static/' + Auction.get_auction_by_id(callback_data.auction_id)[0]['picture'])
                await query.message.delete()
                await query.message.answer_photo(
                    photo=photo,
                    caption=msg.msg_auction(query.from_user.id, callback_data.auction_id),
                    reply_markup=kb_adm.get_edit_kb(query.from_user.id, callback_data.auction_id)
                )
    except BaseException:
        pass

class UpdateAuction(StatesGroup):
    photo_set = State()
    description_set = State()
    price_set = State()
    time_leinght_set = State()

@router.callback_query(cbd.EditAuctionCallback.filter())
async def picture_edit(query: CallbackQuery, callback_data: cbd.EditAuctionCallback, state: FSMContext):
    if User.is_user_admin(query.from_user.id):
        await query.message.delete()
        match callback_data.page:
            case 'picture':
                await query.message.answer(
                    msg.photo_set_msg(query.from_user.id),
                    reply_markup=kb_adm.admin_get_back_kb(query.from_user.id)
                )
                await state.set_state(UpdateAuction.photo_set)

            case 'description':
                await query.message.answer(
                    msg.description_set_msg(query.from_user.id),
                    reply_markup=kb_adm.admin_get_back_kb(query.from_user.id)
                )
                await state.set_state(UpdateAuction.description_set)

            case 'price':
                await query.message.answer(
                    msg.price_set_msg(query.from_user.id),
                    reply_markup=kb_adm.admin_get_back_kb(query.from_user.id)
                )
                await state.set_state(UpdateAuction.price_set)
            
            case 'time_leinght':
                await query.message.answer(
                    msg.time_leinght_set_msg(query.from_user.id),
                    reply_markup=kb_adm.admin_get_back_kb(query.from_user.id)
                )
                await state.set_state(UpdateAuction.time_leinght_set)

        await state.update_data(auction_id=callback_data.auction_id)

@router.message(UpdateAuction.photo_set, F.photo)
async def photo_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            file = message.photo[-1]
            file_id = file.file_id
            file_unique_id = file.file_unique_id
            file = await message.bot.get_file(file_id)
            file_path = file.file_path
            extention = file_path.split('.')[-1]
            await message.bot.download_file(file_path, f"bot/app/static/{file_unique_id}.{extention}")
            data = await state.get_data()
            Auction.update_auction(data['auction_id'], 'picture', f"{file_unique_id}.{extention}")
            await cmd_start(message)
        await state.clear()
    except BaseException:
        pass

@router.message(UpdateAuction.description_set, F.text)
async def description_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            data = await state.get_data()
            Auction.update_auction(data['auction_id'], 'description', message.text[:200])
            await cmd_start(message)
        await state.clear()
    except BaseException:
        pass

@router.message(UpdateAuction.price_set, F.text)
async def price_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            price = re.findall(r'[0-9]{1,}', message.text)
            price = float(price[0]) if len(price) > 0 else 0.0
            data = await state.get_data()
            Auction.update_auction(data['auction_id'], 'price', price)
            await cmd_start(message)
        await state.clear()
    except BaseException:
        pass

@router.message(UpdateAuction.time_leinght_set, F.text)
async def time_leinght_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            time_leinght = re.findall(r'[0-9]{1,}', message.text)
            time_leinght = time_leinght[0] if len(time_leinght) > 0 else 0
            time_leinght = int(time_leinght) * 10000 if int(time_leinght) < 800 else '800'
            data = await state.get_data()
            Auction.update_auction(data['auction_id'], 'time_leinght', time_leinght)
            await cmd_start(message)
        await state.clear()
    except BaseException:
        pass

# ===== END EDITING =====

@router.callback_query(cbd.AuctionSettingsCallback.filter(F.action == "bids"))
async def get_bids_list(query: CallbackQuery, callback_data: cbd.AuctionSettingsCallback):
    try:
        if User.is_user_admin(query.from_user.id):
            conf = Config()
            photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
            await query.message.delete()
            await query.message.answer_photo(
                photo=photo,
                reply_markup=kb_adm.get_bids_list(query.from_user.id, callback_data.auction_id)
            )
    except BaseException:
        pass

@router.callback_query(cbd.BidSettingsCallback.filter(F.action == "view"))
async def get_bids_detail(query: CallbackQuery, callback_data: cbd.BidSettingsCallback):
    try:
        if User.is_user_admin(query.from_user.id):
            conf = Config()
            photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
            await query.message.delete()
            await query.message.answer_photo(
                photo=photo,
                reply_markup=kb_adm.get_edit_bid(query.from_user.id, callback_data.bid_id, callback_data.auction_id)
            )
    except BaseException:
        pass

@router.callback_query(cbd.BidSettingsCallback.filter(F.action == "delete"))
async def get_bids_delete(query: CallbackQuery, callback_data: cbd.BidSettingsCallback):
    try:
        if User.is_user_admin(query.from_user.id):
            Bid.delete_bid_by_id(callback_data.bid_id)
            conf = Config()
            photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
            await query.message.delete()
            await query.message.answer_photo(
                photo=photo,
                reply_markup=kb_adm.get_bids_list(query.from_user.id, callback_data.auction_id)
            )
    except BaseException:
        pass

@router.callback_query(cbd.BidSettingsCallback.filter(F.action == "ban_delete"))
async def get_bids_ban_delete(query: CallbackQuery, callback_data: cbd.BidSettingsCallback):
    try:
        if User.is_user_admin(query.from_user.id):
            username = Bid.get_bid_by_id(callback_data.bid_id)
            username = User.get_user_by_id(username[0]['user_id'])
            username = username[0]['tg_link'] if len(username) > 0 else ''
            Bid.delete_bid_by_id(callback_data.bid_id)
            User.status_change_by_nickname(query.from_user.id, str(username), 'banned')
            conf = Config()
            photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
            await query.message.delete()
            await query.message.answer_photo(
                photo=photo,
                reply_markup=kb_adm.get_bids_list(query.from_user.id, callback_data.auction_id)
            )
    except BaseException:
        pass

@router.callback_query(cbd.AuctionSettingsCallback.filter(F.action == "delete"))
async def delete_confirm(query: CallbackQuery, callback_data: cbd.AuctionSettingsCallback):
    try:
        if User.is_user_admin(query.from_user.id):
            if query.message.content_type == 'text':
                await query.message.edit_text(
                    msg.msg_confirmation_delete(query.from_user.id),
                    reply_markup=kb_adm.get_confirmation_detele_auction(query.from_user.id, callback_data.auction_id)
                )
            else:
                await query.message.delete()
                await query.message.answer(
                    text=msg.msg_confirmation_delete(query.from_user.id),
                    reply_markup=kb_adm.get_confirmation_detele_auction(query.from_user.id, callback_data.auction_id)
                )
    except BaseException:
        pass

@router.callback_query(cbd.AuctionSettingsCallback.filter(F.action == "delete_yes"))
async def delete_auction(query: CallbackQuery, callback_data: cbd.AuctionSettingsCallback):
    try:
        if User.is_user_admin(query.from_user.id):
            Auction.delete_auction(callback_data.auction_id)
            if query.message.content_type == 'text':
                await query.message.edit_text(
                    msg.menu_msg(query.from_user.id),
                    reply_markup=kb_adm.get_admin_menu_kb(query.from_user.id)
                )
            else:
                await query.message.delete()
                await query.message.answer(
                    text=msg.menu_msg(query.from_user.id),
                    reply_markup=kb_adm.get_admin_menu_kb(query.from_user.id)
                )
    except BaseException:
        pass

# ==============

@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "admin_menu_settings"))
async def admin_menu_settings(query: CallbackQuery, callback_data: cbd.AdminMenuCallback, state: FSMContext):
    await state.clear()
    if User.is_user_admin(query.from_user.id):
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            reply_markup=kb_adm.get_settings_menu_kb(query.from_user.id)
        )

class Settings(StatesGroup):
    step_set = State()
    antisniper_set = State()
    currency_set = State()
    rules_set = State()
    admin_set = State()
    ban_set = State()
    unban_set = State()

@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "admin_menu_settings_step"))
async def admin_menu_settings_detail_step(query: CallbackQuery, callback_data: cbd.AdminMenuCallback, state: FSMContext):
    await state.clear()
    if User.is_user_admin(query.from_user.id):
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            caption=msg.settings_detail_step_msg(query.from_user.id),
            reply_markup=kb_adm.admin_get_back_kb(query.from_user.id, 'admin_menu_settings')
        )
        await state.set_state(Settings.step_set)

@router.message(Settings.step_set)
async def set_step(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            conf = Config()
            steps = re.findall(r'[0-9]{1,}', message.text)
            if len(steps) > 0:
                conf.set_value('STEP', ','.join(steps))
            await message.delete()
            await message.answer(
                text=msg.menu_msg(message.from_user.id),
                reply_markup=kb_adm.get_admin_menu_kb(message.from_user.id)
            )
        await state.clear()
    except BaseException:
        pass


@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "admin_menu_settings_antisniper"))
async def admin_menu_settings_detail_antisniper(query: CallbackQuery, callback_data: cbd.AdminMenuCallback, state: FSMContext):
    await state.clear()
    if User.is_user_admin(query.from_user.id):
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            caption=msg.settings_detail_antisniper_msg(query.from_user.id),
            reply_markup=kb_adm.admin_get_back_kb(query.from_user.id, 'admin_menu_settings')
        )
        await state.set_state(Settings.antisniper_set)

@router.message(Settings.antisniper_set)
async def antisniper_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            conf = Config()
            value = re.findall(r'[0-9]{1,}', message.text)
            value = int(value[0]) if len(value) > 0 else 0
            value = value if value < 3600 else 3600
            conf.set_value('ANTISNIPER', value)
            await message.delete()
            await message.answer(
                text=msg.menu_msg(message.from_user.id),
                reply_markup=kb_adm.get_admin_menu_kb(message.from_user.id)
            )
        await state.clear()
    except BaseException:
        pass

@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "admin_menu_settings_currency"))
async def admin_menu_settings_detail_currency(query: CallbackQuery, callback_data: cbd.AdminMenuCallback, state: FSMContext):
    await state.clear()
    if User.is_user_admin(query.from_user.id):
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            caption=msg.settings_detail_currency_msg(query.from_user.id),
            reply_markup=kb_adm.admin_get_back_kb(query.from_user.id, 'admin_menu_settings')
        )
        await state.set_state(Settings.currency_set)

@router.message(Settings.currency_set)
async def currency_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            conf = Config()
            conf.set_value('CURRENCY', message.text)
            await message.delete()
            await message.answer(
                text=msg.menu_msg(message.from_user.id),
                reply_markup=kb_adm.get_admin_menu_kb(message.from_user.id)
            )
        await state.clear()
    except BaseException:
        pass

@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "admin_menu_settings_change_rules"))
async def admin_menu_settings_detail_rules(query: CallbackQuery, callback_data: cbd.AdminMenuCallback, state: FSMContext):
    await state.clear()
    if User.is_user_admin(query.from_user.id):
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            caption=msg.settings_detail_rules_msg(query.from_user.id),
            reply_markup=kb_adm.admin_get_back_kb(query.from_user.id, 'admin_menu_settings')
        )
        await state.set_state(Settings.rules_set)

@router.message(Settings.rules_set)
async def rules_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            conf = Config()
            rules = conf.get_value('SETTINGS')
            rules['ru'] = message.text
            conf.set_value('SETTINGS', rules)
            await message.delete()
            await message.answer(
                text=msg.menu_msg(message.from_user.id),
                reply_markup=kb_adm.get_admin_menu_kb(message.from_user.id)
            )
        await state.clear()
    except BaseException:
        pass

@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "admin_menu_settings_admins"))
async def admin_menu_settings_detail_rules(query: CallbackQuery, callback_data: cbd.AdminMenuCallback, state: FSMContext):
    if User.is_user_admin(query.from_user.id):
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            reply_markup=kb_adm.get_admins_kb(query.from_user.id)
        )

@router.callback_query(cbd.DeleteAdminsCallback.filter(F.page == "confirmation"))
async def admin_menu_settings_detail_rules(query: CallbackQuery, callback_data: cbd.DeleteAdminsCallback, state: FSMContext):
    if User.is_user_admin(query.from_user.id):
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            caption=msg.get_admin_menu_settings_admin_nick_msg(query.from_user.id, callback_data.id),
            reply_markup=kb_adm.get_admins_delete_kb(query.from_user.id, callback_data.id)
        )

@router.callback_query(cbd.DeleteAdminsCallback.filter(F.page == "get_conf"))
async def admin_menu_settings_detail_rules(query: CallbackQuery, callback_data: cbd.DeleteAdminsCallback, state: FSMContext):
    if User.is_user_admin(query.from_user.id):
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            caption=msg.ge_admin_menu_settings_admin_delete_confirmation_msg(query.from_user.id, callback_data.id),
            reply_markup=kb_adm.get_admins_delete_confirmation_kb(query.from_user.id, callback_data.id)
        )

@router.callback_query(cbd.DeleteAdminsCallback.filter(F.page == "yes"))
async def admin_menu_settings_detail_rules(query: CallbackQuery, callback_data: cbd.DeleteAdminsCallback, state: FSMContext):
    if User.is_user_admin(query.from_user.id):
        User.status_change(query.from_user.id, callback_data.id, 'user')
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            reply_markup=kb_adm.get_admins_kb(query.from_user.id)
        )

@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "new_admin"))
async def new_admin_(query: CallbackQuery, callback_data: cbd.AdminMenuCallback, state: FSMContext):
    await state.clear()
    if User.is_user_admin(query.from_user.id):
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            caption=msg.get_admin_menu_settings_make_admin_info_msg(query.from_user.id),
            reply_markup=kb_adm.admin_get_back_kb(query.from_user.id, 'admin_menu_settings')
        )
        await state.set_state(Settings.admin_set)

@router.message(Settings.admin_set)
async def admin_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            nickname = message.text.split('@')[-1].split(' ')[0]
            user = User.status_change_by_nickname(message.from_user.id, str(nickname), 'admin')
            user = user[0]['tg_id'] if len(user) != 0 else message.from_user.id
            await message.delete()
            await message.answer(
                text=msg.get_admin_menu_settings_make_admin_done_msg(message.from_user.id, nickname),
                reply_markup=kb_adm.admin_get_back_kb(message.from_user.id, 'new_admin')
            )
            await message.bot.send_message(
                chat_id=user,
                text=msg.get_admin_info_msg(user)
            )
        await state.clear()
    except BaseException:
        pass
# ======= BLACKLIST =====
@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "blacklist_menu"))
async def admin_menu_settings_bl(query: CallbackQuery, callback_data: cbd.AdminMenuCallback, state: FSMContext):
    await state.clear()
    if User.is_user_admin(query.from_user.id):
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            reply_markup=kb_adm.get_bl_kb(query.from_user.id)
        )

@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "view_bl"))
async def admin_menu_settings_detail_bl(query: CallbackQuery, callback_data: cbd.AdminMenuCallback, state: FSMContext):
    if User.is_user_admin(query.from_user.id):
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            caption=msg.get_admin_menu_settings_make_bl_view_detail_msg(query.from_user.id),
            reply_markup=kb_adm.admin_get_back_kb(query.from_user.id, 'blacklist_menu')
        )

@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "add_to_bl"))
async def admin_menu_settings_add_to_bl(query: CallbackQuery, callback_data: cbd.AdminMenuCallback, state: FSMContext):
    if User.is_user_admin(query.from_user.id):
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            caption=msg.get_admin_menu_settings_make_bl_add_info_msg(query.from_user.id),
            reply_markup=kb_adm.admin_get_back_kb(query.from_user.id, 'blacklist_menu')
        )
        await state.set_state(Settings.ban_set)

@router.message(Settings.ban_set)
async def ban_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            nickname = message.text.split('@')[-1].split(' ')[0]
            user = User.status_change_by_nickname(message.from_user.id, str(nickname), 'banned')
            await message.delete()
            await message.answer(
                text=msg.get_admin_menu_settings_make_bl_add_done_msg(message.from_user.id, nickname),
                reply_markup=kb_adm.admin_get_back_kb(message.from_user.id, 'blacklist_menu')
            )
        await state.clear()
    except BaseException:
        pass

@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "delete_from_bl"))
async def admin_menu_settings_add_to_bl(query: CallbackQuery, callback_data: cbd.AdminMenuCallback, state: FSMContext):
    if User.is_user_admin(query.from_user.id):
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            caption=msg.get_admin_menu_settings_make_bl_delete_info_msg(query.from_user.id),
            reply_markup=kb_adm.admin_get_back_kb(query.from_user.id)
        )
        await state.set_state(Settings.unban_set)

@router.message(Settings.unban_set)
async def unban_set_(message: Message, state: FSMContext):
    try:
        if User.is_user_admin(message.from_user.id):
            nickname = message.text.split('@')[-1].split(' ')[0]
            user = User.status_change_by_nickname(message.from_user.id, str(nickname), 'user')
            await message.delete()
            await message.answer(
                text=msg.get_admin_menu_settings_make_bl_delete_done_msg(message.from_user.id, nickname),
                reply_markup=kb_adm.admin_get_back_kb(message.from_user.id)
            )
        await state.clear()
    except BaseException:
        pass
# ======= BLACKLIST =====
# ======= NOTIFICATIONS =
@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "admin_menu_settings_notifications"))
async def admin_menu_settings(query: CallbackQuery, callback_data: cbd.AdminMenuCallback):
    if User.is_user_admin(query.from_user.id):
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            caption=msg.get_admin_menu_settings_detail_notification_msg(query.from_user.id),
            reply_markup=kb_adm.admin_notifications_kb(query.from_user.id)
        )

@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "admin_menu_settings_detail_notification_win"))
async def admin_menu_settings_win(query: CallbackQuery, callback_data: cbd.AdminMenuCallback):
    if User.is_user_admin(query.from_user.id):
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        conf.set_value('WIN_NOTIFICATION', (not conf.get_value('WIN_NOTIFICATION')))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            caption=msg.get_admin_menu_settings_detail_notification_msg(query.from_user.id),
            reply_markup=kb_adm.admin_notifications_kb(query.from_user.id)
        )

@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "admin_menu_settings_detail_notification_lose"))
async def admin_menu_settings_lose(query: CallbackQuery, callback_data: cbd.AdminMenuCallback):
    if User.is_user_admin(query.from_user.id):
        conf = Config()
        photo = FSInputFile('bot/app/static/' + conf.get_value('LOGO'))
        conf.set_value('LOOSE_NOTIFICATION', (not conf.get_value('LOOSE_NOTIFICATION')))
        await query.message.delete()
        await query.message.answer_photo(
            photo=photo,
            caption=msg.get_admin_menu_settings_detail_notification_msg(query.from_user.id),
            reply_markup=kb_adm.admin_notifications_kb(query.from_user.id)
        )

@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "admin_menu_settings_language"))
async def admin_menu_settings_change_lang(query: CallbackQuery, callback_data: cbd.AdminMenuCallback):
    if User.is_user_admin(query.from_user.id):
        await query.message.delete()
        await query.message.answer(
            text=msg_user.lang_msg(query.from_user.id),
            reply_markup=kb_usr.get_lang_kb()
        )

# ==============
@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "admin_menu_help"))
async def help_vid(query: CallbackQuery, callback_data: cbd.AdminMenuCallback):
    try:
        if User.is_user_admin(query.from_user.id):
            conf = Config()
            video = FSInputFile('bot/app/static/' + conf.get_value('HELP'))
            await query.message.answer_video(
                video=video
            )
    except BaseException:
        pass

@router.callback_query(cbd.AdminMenuCallback.filter(F.page == "menu"))
async def get_back_menu(query: CallbackQuery, callback_data: cbd.AdminMenuCallback, state: FSMContext):
    try:
        await state.clear()
        if User.is_user_admin(query.from_user.id):
            if query.message.content_type == 'text':
                await query.message.edit_text(
                    msg.menu_msg(query.from_user.id),
                    reply_markup=kb_adm.get_admin_menu_kb(query.from_user.id)
                )
            else:
                await query.message.delete()
                await query.message.answer(
                    text=msg.menu_msg(query.from_user.id),
                    reply_markup=kb_adm.get_admin_menu_kb(query.from_user.id)
                )
    except BaseException:
        pass