from crypto_pay_api_sdk import cryptopay
from app.helper import config
from time import sleep
from app.DB.DB import Bank
import requests
import uuid
import random
import string

class Crypto:
    def __init__(self) -> None:
        conf = config.Config()
        self.config = conf
        self.Crypto = cryptopay.Crypto(conf.get_value('CRYPTO_PAY_TOKEN'), testnet = False)

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Crypto, cls).__new__(cls)
        return cls.instance
    
    def get_utf8():
        # Define the length of the random string
        length = 10

        # Define the characters to be used in the random string
        characters = string.ascii_letters + string.digits + string.punctuation + ' '

        # Generate the random string
        random_string = ''.join(random.choices(characters, k=length)).encode('utf-8')

        # Print the random string
        return random_string.decode('utf-8')
    
    def exchangeValue(self, fromCoin: str, toCoin: str, amount: int):
        exchange: str
        res = requests.get(f'https://api.cryptomus.com/v1/exchange-rate/{fromCoin}/list')
        for course in res.json()['result']:
            if course['to'] == toCoin:
                exchange = float(course['course']) * amount

        return exchange
    
    def createInvoice(self, coin: str, amount: int, auction_id: int, user_id: int):

        price = self.exchangeValue('USD', coin, amount)

        invioce = self.Crypto.createInvoice(
            asset=coin, 
            amount=price, 
            params={"description": "Invoice", "expires_in": self.config.get_value('PAY_EXPARATION')}
        )
        if invioce['ok'] == True:
            result: dict = invioce['result']
            print(result)
            Bank.update_payment_bank(
                auction_id,
                user_id,
                result['invoice_id'],
                result['asset'],
                amount,
                result['amount'],
                result['bot_invoice_url'],
            )
            return result['bot_invoice_url']
        
    def transfer(self, coin: str, amount: int, auction_id: int, user_id: int):

        price = self.exchangeValue('USD', coin, amount)
        status = ''
        print(price)

        try:
            transfer = self.Crypto.transfer(
                user_id=user_id,
                asset=coin, 
                amount=price,
                spend_id=Crypto.get_utf8()
            )
        except BaseException as e:
            print(e)
            status = 'fail'
        finally:
            status = 'completed'
        print(status)
        
        return status
        
    def getInvoices(self) -> dict:
        invioce = self.Crypto.getInvoices()
        if invioce['ok'] == True:
            return invioce['result']
        
    async def checkInvoices(self):
        banks = Bank.get_bank_by_status_pay()
        items = self.getInvoices()['items']

        if len(items) == 0 or len(banks) == 0:
            return
        
        for bank in banks:
            if 'invoice_id' in bank:
                for item in items:
                    if item['invoice_id'] == bank['invoice_id']:
                        if item['status'] == 'paid' and bank['statusPay'] == 'active':
                            Bank.update_bank_status(bank['id'], 'paid')

