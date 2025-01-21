import requests
import polars as pl
import logging
from alpha_utils import get_alpha_key, parse_data, run_end_to_end
from s3io import S3IO


class AlphaIO:
    """
    AlphaIO class provides methods to interact with the AlphaVantage API

    - pull fundamental data
    - pull profiles for ETFs
    - pull corporate actions for dividends
    """
    def __init__(self, tickers: list | pl.series):
        """
        Initialize the AlphaIO class
        """
        self.BASE_URL = 'https://www.alphavantage.co/query?function='
        self.request_count = 0
        self.tickers = tickers
        self.s3 = S3IO
        self.ticker_tracking_dict = {}

    def _alpha_request(self, ticker: str, statement: str, api_key:str) -> dict:
        """
        Make a request to the AlphaVantage API
        """
        request_url = f'{self.BASE_URL}{statement}&symbol={ticker}&apikey={api_key}'
        return requests.get(request_url).json()

    def get_statement(self, ticker: str, statement: str | list) -> dict[str: pl.DataFrame]:
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
                try:
                    data = self._alpha_request(ticker=ticker, statement=statement_dict[financial_statement], api_key=api_key)
                    # parse the data
                    df = parse_data(data=data[0]['quarterlyReports'], str_cols=['fiscalDateEnding', 'reportedCurrency'])
                    financials[financial_statement] = df
                    self.request_count += 1
                except Exception as e:
                    logging.warning(f"Could not load data from Alpha Vantage for {ticker}\n{e}")
                    financials[financial_statement] = None
        else:
            try:
                data = self._alpha_request(ticker=ticker, statement=statement, api_key=api_key)
                df = parse_data(data=data[0]['quarterlyReports'], str_cols=['fiscalDateEnding', 'reportedCurrency'])
                financials[statement] = df
                self.request_count += 1
            except Exception as e:
                logging.warning(f"Could not load data from Alpha Vantage for {ticker}\n{e}")
                financials[statement] = None
        return financials

    def get_target_data(self, ticker: str) -> dict[str: pl.DataFrame]:
        """
        ticker: str
            the name of the ticker, the symbol
        get the target data for the ticker
        """
        financials = {}
        statements = ["cash", "income", "balance"]
        for statement in statements:
            try:
                df_statement = self.s3.s3_read_parquet(file_path=f"{statement}/{ticker}/{statement}.parq")
                financials[statement] = df_statement
            except Exception as e:
                logging.warning(f"Missing data for statement {statement} for ticker {ticker}, initializing ...\n{e}")
                financials[statement] = None

        return financials

    def write_data(self,
                   ticker: str,
                   target_financials: dict[str: pl.DataFrame],
                   source_financials: dict[str: pl.DataFrame]
                   ):
        """
        update the target dataframe using the source and write to s3

        ticker: str
            ticker symbol

        target_financials: dict[str: pl.DataFrame]
            dictionary of target data frames

        source_financials: dict[str: pl.DataFrame]
            dictionary of source data frames
        """
        for statement in target_financials.keys():
            # check if the source financial is None
            if source_financials[statement] is None:
                logging.warning(f"source data missing, skipping {ticker}: {statement}")
                self.ticker_tracking_dict[ticker] = False
                continue
            # check if the target financial is null
            if target_financials[statement] is None:
                logging.warning(f"Missing target, initializing the {statement} statements for {ticker}")
                target_financials[statement] = source_financials[statement]
                self.ticker_tracking_dict[ticker] = True
            else:
                # run the end to end
                target_financials[statement] = run_end_to_end(target=target_financials[statement],
                                                              source=source_financials[statement],
                                                              id_col='fiscalDateEnding')
                self.ticker_tracking_dict[ticker] = True
            # write the data to s3 in specified location
            self.s3.s3_write_parquet(df=target_financials[statement],
                                     file_path=f"{statement}/{ticker}/{statement}.parq")

    def run(self) -> None:
        """
        run the end-to-end process of the alphio
        return a dict with the key as the ticker and the value as a boolean representing the
        process of retrieving the data has been completed
        """
        for ticker in self.tickers:
            # get the source data
            source_data = self.get_statement(ticker=ticker, statement=['income',
                                                                       'balance',
                                                                       'cash'])
            # get the target data
            target_data = self.get_target_data(ticker=ticker)
            self.write_data(ticker=ticker,
                            target_financials=target_data,
                            source_financials=source_data)




if __name__ == '__main__':
    alphaio = AlphaIO()
    print(alphaio.get_statement(ticker='RGLD', statement='cash'))
    