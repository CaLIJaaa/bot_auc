from aiogram import Router, F
from aiogram.filters import Command
from aiogram.filters.callback_data import CallbackData, CallbackQuery
from aiogram.types import Message, ReplyKeyboardRemove

from app.callbackdata.custom import MyCallback
import app.messages.for_user as msg

router = Router()

@router.message()
async def eroor_handler(message: Message) -> None:
    try:
        await message.answer(
            msg.error_msg(message.from_user.id)
        )
    except BaseException:
        pass