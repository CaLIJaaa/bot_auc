from aiogram.filters.callback_data import CallbackData, CallbackQuery
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup

class MyCallback(CallbackData, prefix="my"):
    foo: str
    bar: int

class LangCallback(CallbackData, prefix="lang"):
    lang_code: str

class TypeAUCallback(CallbackData, prefix="typeAu"):
    auction_type: str

class AuctionCallback(CallbackData, prefix="Auction_id"):
    auction_id: int

class AuctionHelpCallback(CallbackData, prefix="Auction_helper"):
    auction_id: int = None
    page: str = None

class CreationAuctionTypeCallback(CallbackData, prefix="create_auction_type"):
    type: str = None

class SkipCallback(CallbackData, prefix="skip_creation"):
    value: str

class EditAuctionCallback(CallbackData, prefix="edit_auction"):
    auction_id: int
    page: str

class DeleteAdminsCallback(CallbackData, prefix="delete_admins"):
    id: int
    page: str = None

class AuctionAdminCallback(CallbackData, prefix="Auction_admin_id"):
    auction_id: int

class GetBackCallback(CallbackData, prefix="get_back"):
    page: str

class AdminMenuCallback(CallbackData, prefix="adminmenu"):
    page: str

class AuctionSettingsCallback(CallbackData, prefix="auction_settings"):
    action: str
    auction_id: int

class BidMenuCallback(CallbackData, prefix="bidmenu"):
    auction_id: int
    bid: int

class BidSettingsCallback(CallbackData, prefix="bidsettings"):
    bid_id: int
    auction_id: int
    action: str