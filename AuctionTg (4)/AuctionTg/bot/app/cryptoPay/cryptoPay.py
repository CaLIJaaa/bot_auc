from crypto_pay_api_sdk import cryptopay
from app.helper import config
from time import sleep
from app.DB.DB import Auction
import requests

class Crypto:
    def __init__(self) -> None:
        conf = config.Config()
        self.config = conf
        self.Crypto = cryptopay.Crypto(conf.get_value('CRYPTO_PAY_TOKEN'), testnet = True) #default testnet = False

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Crypto, cls).__new__(cls)
        return cls.instance
    
    def exchangeValue(self, fromCoin: str, toCoin: str, amount: int):
        exchange: str
        res = requests.get(f'https://api.cryptomus.com/v1/exchange-rate/{fromCoin}/list')
        for course in res.json()['result']:
            if course['to'] == toCoin:
                exchange = float(course['course']) * amount

        return exchange
    
    def createInvoice(self, coin: str, amount: int, auction_id: int):

        price = self.exchangeValue('USD', coin, amount)

        invioce = self.Crypto.createInvoice(
            asset=coin, 
            amount=price, 
            params={"description": "Test Invoice", "expires_in": self.config.get_value('PAY_EXPARATION')}
        )
        if invioce['ok'] == True:
            result: dict = invioce['result']
            print(result)
            Auction.update_payment_auction(
                auction_id, 
                result['invoice_id'],
                result['asset'],
                result['amount'],
                result['bot_invoice_url'],
            )
            return result['bot_invoice_url']
        
    def getInvoices(self) -> dict:
        invioce = self.Crypto.getInvoices()
        if invioce['ok'] == True:
            return invioce['result']
        
    def checkInvoices(self):
        while True:
            auctions = Auction.get_auction_by_status_pay()
            items = self.getInvoices()['items']

            if len(items) == 0 or len(auctions) == 0:
                return
            
            for auction in auctions:
                if 'invoice_id' in auction:
                    for item in items:
                        if item['invoice_id'] == auction['invoice_id']:
                            if item['status'] == 'paid' and auction['statusPay'] == 'active':
                                Auction.update_auction(auction['id'], 'statusPay', 'paid')

            sleep(5)
