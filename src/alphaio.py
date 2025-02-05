import requests
import polars as pl
import logging
from datetime import datetime
from alpha_utils import (get_alpha_key, parse_data, run_end_to_end,
                         get_bucket_name, get_profile_name)
from s3io import S3IO


class AlphaIO:
    """
    AlphaIO class provides methods to interact with the AlphaVantage API

    - pull fundamental data
    - pull profiles for ETFs
    - pull corporate actions for dividends
    """
    def __init__(self, tickers: any):
        """
        Initialize the AlphaIO class
        """
        self.BASE_URL = 'https://www.alphavantage.co/query?function='
        self.request_count = 0
        self.tickers = tickers
        # get bucket name
        bucket = get_bucket_name()
        profile = get_profile_name()
        self.s3 = S3IO(bucket=bucket, profile=profile)
        self.ticker_tracking_dict = {}

    def _alpha_request(self, ticker: str, statement: str, api_key:str) -> dict:
        """
        Make a request to the AlphaVantage API
        """
        request_url = f'{self.BASE_URL}{statement}&symbol={ticker}&apikey={api_key}'
        r = requests.get(request_url)
        data = r.json()
        return data

    def get_statement(self, ticker: str,
                      api_key: str,
                      statement: str | list) -> dict[str: pl.DataFrame]:
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
        # api_key = get_alpha_key()
        statement_dict = {
            'income': 'INCOME_STATEMENT',
            'balance': 'BALANCE_SHEET',
            'cash': 'CASH_FLOW'
        }
        financials = {}
        if isinstance(statement, list):

            for financial_statement in statement:
                try:
                    data = self._alpha_request(ticker=ticker, statement=statement_dict[financial_statement], api_key=api_key)
                    logging.info(f"data retrieved for {ticker}")
                    # parse the data
                    try:
                        df = parse_data(data=data['quarterlyReports'], str_cols=['fiscalDateEnding', 'reportedCurrency'])
                    except Exception as e:
                        logging.warning(f"{ticker} has no quarterly reports, persisting annual report data")
                        df = parse_data(data=data['annualReports'],
                                        str_cols=['fiscalDateEnding', 'reportedCurrency'])
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
                update_time = datetime.now()
                target_financials[statement] = source_financials[statement]
                # add the is_current and update_time columns
                target_financials[statement] = target_financials[statement].with_columns(
                        pl.lit(True).alias("is_current"),
                        pl.lit(update_time).alias("update_time")
                    )
                self.ticker_tracking_dict[ticker] = True
            else:
                # run the end to end
                target_tmp = target_financials[statement].filter(pl.col("is_current") == True)
                target_financials[statement] = run_end_to_end(target=target_tmp,
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
        # split the  tickers list in half then pass the api keys
        api_key, api_key2 = get_alpha_key()
        split_number = int(len(self.tickers) / 2)
        counter = 0
        for ticker in self.tickers:
            if counter < split_number:
                # get the source data
                logging.info(f"Using the first api key for {ticker}")
                source_data = self.get_statement(ticker=ticker,
                                                 api_key=api_key,
                                                 statement=['income',
                                                            'balance',
                                                            'cash'])
            else: # use the second api key
                # get the source data
                logging.info(f"Using the second api key for {ticker}")
                source_data = self.get_statement(ticker=ticker,
                                                 api_key=api_key2,
                                                 statement=['income',
                                                            'balance',
                                                            'cash'])
            counter += 1
            # get the target data
            target_data = self.get_target_data(ticker=ticker)
            if target_data is None and source_data is None:
                logging.warning(f"Missing target and source data for ticker {ticker}, skipping ...")
                continue
            else:
                self.write_data(ticker=ticker,
                                target_financials=target_data,
                                source_financials=source_data)


if __name__ == '__main__':
    alphaio = AlphaIO()
    print(alphaio.get_statement(ticker='RGLD', statement='cash'))
    