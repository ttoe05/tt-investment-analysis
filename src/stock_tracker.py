""""
A data object that keeps track of all the stocks that have been persisted for the given quarter
"""
import polars as pl
import logging
from alphaio import AlphaIO
from alpha_utils import list_local_files, run_end_to_end
from datetime import datetime
from s3io import S3IO


class StockTracker:
    """
    data object to keep track of the stocks that have been persisted
    """

    def __init__(self):
        """
        initialize the object
        """
        self.df_source = None
        self.df_target = None
        self.ticker_table = "stock_tracker/tickers.parq"
        self.ticker_queue_table = "stock_tracker/tickers_queue.parq"
        self.ticker_queue = None
        self.alphaio = None
        self.s3 = S3IO()

    def _market_cap_define(self) -> None:
        """
        apply the rules to define the companies market cap
        """
        def market_cap_rules(x: int|float) -> str:
            """
            apply the rules
            """
            if x > 200000000000:
                return "Mega"
            elif x >= 10000000000 and x <=200000000000:
                return "Large"
            elif x >= 2000000000 and x < 10000000000:
                return "Medium"
            elif x >= 300000000 and x < 2000000000:
                return "Small"
            elif x >= 50000000 and x < 300000000:
                return "Micro"
            elif x < 50000000:
                return "Nano"
            else:
                return None

        self.df_source = self.df_source.with_columns(
            pl.struct(pl.col("Market Cap")).map_elements(lambda t: market_cap_rules(t["Market Cap"]),
                                                         return_dtype=pl.String).alias("Market Cap Name")
        )

    def get_stock_list_locally(self, file_path: str|list):
        """
        get the list of stocks from a config file, that initiliazes the tracker
        """
        if isinstance(file_path, list):
            dfs = []
            for data_file in file_path:
                df_data = pl.read_csv(data_file)
                dfs.append(df_data)
            # concat data
            self.df_source = pl.concat(dfs)
        else:

            self.df_source = pl.read_csv(file_path)
        # return only the needed columns
        self.df_source = self.df.select([
            'Symbol', 'Name', 'Market Cap', 'Country', 'IPO Year', 'Sector', 'Industry'
        ])
        self._market_cap_define()
        logging.info(f"Loaded data for the source successfully: {self.df_source.head()}")

    def _get_target(self) -> None:
        """
        Get the target data from s3
        """
        # get data from target
        try:
            self.df_target = self.s3.s3_read_parquet(file_path=self.ticker_table)
            logging.info(f"successfully loaded stock_tracker data from s3")
        except Exception as e:
            logging.warning(f"Could not find target data, setting target equal to source {e}")
            self.df_target = self.df_source
            self.df_target = self.df_target.with_columns(
                pl.lit(True, dtype=pl.Boolean).alias('is_current'),
                pl.lit(datetime.now(), dtype=pl.Datetime).alias('updated_time')
            )

    def _get_ticker_queue(self) -> None:
        """
        Get the ticker queue which is a dataframe
        """
        # check if the queue is initialized in s3
        try:
            self.ticker_queue = self.s3.s3_read_parquet(file_path=self.ticker_queue_table)
        except Exception as e:
            logging.warning(f"queue file does not exist, initializing the queue using target data\n{e}")
            self.ticker_queue = self.df_target.select(["Symbol"])
            # add the download column and download time
            self.ticker_queue = self.ticker_queue.with_columns(
                pl.lit(None, dtype=pl.Datetime).alias('Download_time'),
                pl.lit(False, dtype=pl.Boolean).alias('Downloaded'),
                pl.Lit(False, dtype=pl.Boolean).alias('Download_Failed')
            )

    def update_queue(self, tickers: list, val: bool):
        """
        update the queue table by ticker
        """
        # update the download flag to true
        if val:
            self.ticker_queue = self.ticker_queue.with_columns(
                                    Downloaded=pl.when(pl.col('Symbol').is_in(tickers))
                                    .then(pl.lit(True))
                                    .otherwise(pl.col('Downloaded')))
            self.ticker_queue = self.ticker_queue.with_columns(
                                    Download_time=pl.when(pl.col('Symbol').is_in(tickers))
                                    .then(pl.lit(datetime.now()))
                                    .otherwise(pl.col('Download_time')))
        else:
            self.ticker_queue = self.ticker_queue.with_columns(
                Download_Failed=pl.when(pl.col('Symbol').is_in(tickers))
                .then(pl.lit(True))
                .otherwise(pl.col('Download_Failed')))
            self.ticker_queue = self.ticker_queue.with_columns(
                Download_time=pl.when(pl.col('Symbol').is_in(tickers))
                .then(pl.lit(datetime.now()))
                .otherwise(pl.col('Download_time')))

    def write_ticker_queue(self, download_dict: dict[str: bool]) -> None:
        """
        Update and write the ticker queue to s3
        """
        # update the queue dataframe by ticker
        Download_success = []
        Download_falures = []
        for ticker in download_dict.keys():
            if download_dict[ticker]:
                Download_success.append(ticker)
            else:
                Download_falures.append(ticker)

        # update the download flag to true
        self.update_queue(tickers=Download_success, val=True)
        self.update_queue(tickers=Download_falures, val=False)
        # write the queue back to s3
        self.s3.s3_write_parquet(df=self.ticker_queue,
                                 file_path=self.ticker_queue_table)


    def run(self) -> None:
        """
        The main run of stock tracker logical flow to return the stocks for retrieving data from alpha vantage

        1. check locally if source data is available for updating or initializing the target ticker table
        2. Check if the data in s3 needs initializing, initialize if not
        3. make any updates inserts to the ticker table and write data to s3
        4. Check if the ticker queue is initialized, if not initialize it
        5. Add any new records to the queue

        """
        # check if local files are available
        source_files = list_local_files(file_path='data/')
        if len(source_files) > 0:
            # get the source data
            logging.info(f"Found source data locally: {source_files}")
            self.get_stock_list_locally(file_path=source_files) # sets the source
        self._get_target() # sets the target if not in s3 initiliaze the dataframe
        # update or insert the records from the source and target
        self.df_target = run_end_to_end(target=self.df_target, source=self.df_source, id_col="Symbol")
        # write the target data to s3
        self.s3.s3_write_parquet(self.df_target, file_path=self.ticker_table)
        # get the queue
        self._get_ticker_queue()
        # get the list of stocks for downloading
        tickers = self.ticker_queue.filter(
            pl.col('Downloaded') == False
        ).head(8).select("Symbol").to_series()
        # pass the list of tickers to the alpha io object
        self.alphaio = AlphaIO(tickers=tickers)
        # run the alphaio object
        self.alphaio.run()
        self.write_ticker_queue(download_dict=self.alphaio.ticker_tracking_dict)













if __name__ == '__main__':

    files = [
        'data/NYSE_Utilites.csv',
        'data/NYSE_basic_materials.csv'
    ]

    for file in files:
        stock_tracker = StockTracker()
        stock_tracker.get_stock_list(file_path=file)
        stock_tracker.market_cap_define()
        print(stock_tracker.df.columns)
        print(stock_tracker.df.head())
        print(stock_tracker.df.select([pl.col("Market Cap Name").value_counts(sort=True),]).unnest('Market Cap Name'))


