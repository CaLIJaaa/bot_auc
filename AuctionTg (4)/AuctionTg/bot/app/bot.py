import asyncio
from aiogram import Bot, Dispatcher
from app.handlers import admin, user, error
from app.helper import config
import logging
import sys
import app.DB
import asyncio
from queue import Queue
from threading import Thread
from app.controller_auctions.controller import controller


async def main():
    conf = config.Config()
    bot = Bot(token=conf.get_value('bot_token'))
    dp = Dispatcher()

    dp.include_routers(user.router, admin.router, error.router)
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    _thread = Thread(target=controller, args=(bot, asyncio.get_event_loop()))
    _thread.start()
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())