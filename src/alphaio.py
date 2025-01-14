import requests
import polars as pl
import logging
from alpha_utils import get_alpha_key, parse_data


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

    def _alpha_request(self, ticker: str, statement: str, api_key:str) -> dict:
        """
        Make a request to the AlphaVantage API
        """
        request_url = f'{self.BASE_URL}{statement}&symbol={ticker}&apikey={api_key}'
        return requests.get(request_url).json()

    def get_statement(self, ticker: str, statement: str | list) -> dict[pl.DataFrame]:
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
        financials = {}
        if isinstance(statement, list):

            for financial_statement in statement:
                data = self._alpha_request(ticker=ticker, statement=statement_dict[financial_statement], api_key=api_key)
                # parse the data
                df = parse_data(data=data[0]['quarterlyReports'], str_cols=['fiscalDateEnding', 'reportedCurrency'])
                financials[financial_statement] = df
                self.request_count += 1
        else:
            data = self._alpha_request(ticker=ticker, statement=statement, api_key=api_key)
            df = parse_data(data=data[0]['quarterlyReports'], str_cols=['fiscalDateEnding', 'reportedCurrency'])
            financials[statement] = df
            self.request_count += 1
        return financials



if __name__ == '__main__':
    alphaio = AlphaIO()
    print(alphaio.get_statement(ticker='RGLD', statement='cash'))
    