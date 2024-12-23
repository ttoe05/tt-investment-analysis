import requests
import logging
from alpha_utils import get_alpha_key


class AlphaIO:
    """
    AlphaIO class provides methods to interact with the AlphaVantage API

    - pull fundamental data
    - pull profiles for ETFs
    - pull corporate actions for dividends
    """
    def __init__(self):
        """
        Initialize the AlphaIO class
        """
        self.BASE_URL = 'https://www.alphavantage.co/query?function='
        self.request_count = 0

    def get_statement(self, ticker: str, statement: str | list=['income', 'balance', 'cash']) -> list[dict]:
        """
        Get income statement for a given ticker
        Parameters
        _____________________
         ticker:
            the ticker to get income statements for
        statement:
        the statement to pull, acceptable values = income, balance, cash. A list of the statement can be passed
        as an argument
        :return:s
        """
        ticker = ticker.upper()
        api_key = get_alpha_key()
        statement_dict = {
            'income': 'INCOME_STATEMENT',
            'balance': 'BALANCE_STATEMENT',
            'cash': 'CASH_FLOW'
        }
        financials = []
        if isinstance(statement, list):

            for financial_statment in statement:
                request_url = f'{self.BASE_URL}{statement_dict[financial_statment]}&symbol={ticker}&apikey={api_key}'
                data = requests.get(request_url).json()
                financials.append(data)
                self.request_count += 1
        else:
            request_url = f'{self.BASE_URL}{statement_dict[statement]}&symbol={ticker}&apikey={api_key}'
            data = requests.get(request_url).json()
            financials.append(data)
            self.request_count += 1
        return financials



if __name__ == '__main__':
    alphaio = AlphaIO()
    print(alphaio.get_statement(ticker='RGLD', statement='cash'))
    