from cryptomus import Client
from app.helper.config import Config

conf = Config()

PAYMENT_KEY = conf.get_value('PAYMENT_KEY')
MERCHANT_UUID = conf.get_value('PAYMENT_KEY')

# payment = Client.payment(PAYMENT_KEY, MERCHANT_UUID)