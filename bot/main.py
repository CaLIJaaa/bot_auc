import asyncio
from app import bot
import logging
import sys
import app.DB

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(bot.main())