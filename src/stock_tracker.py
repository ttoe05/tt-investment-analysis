#!/usr/bin/python3
""""
A data object that keeps track of all the stocks that have been persisted for the given quarter
"""
import polars as pl
import logging
from alphaio import AlphaIO
from alpha_utils import list_local_files, run_end_to_end, get_bucket_name, init_logger, get_profile_name
from datetime import datetime
from s3io import S3IO
from multiprocessing import Process, Queue
from functools import reduce
from time import time

SCHEMA_DEF = {
    'Symbol': pl.String,
    'Name': pl.String,
    'Market Cap': pl.Float64,
    'Country': pl.String,
    'IPO Year': pl.Int64,
    'Sector': pl.String,
    'Industry': pl.String,
    # 'Market Cap Name': pl.String
}


class StockTracker:
    """
    data object to keep track of the stocks that have been persisted
    """

    def __init__(self, queue_depth=4):
        """
        initialize the object
        """
        self.df_source = None
        self.df_target = None
        self.queue_depth = queue_depth
        self.ticker_table = "stock_tracker/tickers.parq"
        self.ticker_queue_table = "stock_tracker/tickers_queue.parq"
        self.ticker_queue = None
        # self.alphaio = None

        # get bucket name
        bucket = get_bucket_name()
        profile = get_profile_name()
        self.s3 = S3IO(bucket=bucket, profile=profile)

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
        assumes data came NASDAQ
        """
        if isinstance(file_path, list):
            dfs = []
            for data_file in file_path:
                df_data = pl.read_csv(data_file)
                # cast the IPO Year
                df_data = df_data.with_columns(pl.col("IPO Year").cast(pl.Int64))
                dfs.append(df_data)
            # concat data
            self.df_source = pl.concat(dfs)
        else:

            self.df_source = pl.read_csv(file_path)
        # return only the needed columns
        self.df_source = self.df_source.select([
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
            logging.info(f"successfully loaded stock_tracker data from s3: {self.df_target.head()}")
            # cast IPO year if string
            if 'IPO Year' in self.df_target.columns:
                # cast to int 64
                self.df_target = self.df_target.with_columns(pl.col("IPO Year").cast(pl.Int64, strict=False))
            # self.df_target = self.df_target.filter(pl.col('Symbol').is_in(['AG', 'AEM']))
            # logging.info(f"Filtered out, {self.df_target.shape}")
        except Exception as e:
            logging.warning(f"Could not find target data, setting target equal to source {e}")
            if self.df_source is not None:
                self.df_target = self.df_source

                self.df_target = self.df_target.with_columns(
                    pl.lit(True, dtype=pl.Boolean).alias('is_current'),
                    pl.lit(datetime.now(), dtype=pl.Datetime).alias('updated_time')
                )
            else:
                logging.warning(f"While setting target, no data available and source is empty setting target to None")
                self.df_target = None

    def get_queue_total(self) -> list[str]:
        """
        Get the total number of items in the queue
        """

        return list(self.ticker_queue.sort(by="Download_time", descending=False).filter(
            pl.col('Downloaded') == False,
            pl.col('Download_Failed') == False
        ).select("Symbol").to_series())

    def _get_ticker_queue(self) -> None:
        """
        Get the ticker queue which is a dataframe
        """
        # check if the queue is initialized in s3
        try:
            self.ticker_queue = self.s3.s3_read_parquet(file_path=self.ticker_queue_table)
            logging.info(f"successfully loaded ticker queue from s3: {self.ticker_queue.head()}")
            logging.info(
                f"total amount of items in queue: {len(self.get_queue_total())}")

        except Exception as e:
            logging.warning(f"queue file does not exist, initializing the queue using target data\n{e}")
            self.ticker_queue = self.df_target.select(["Symbol"])
            # add the download column and download time
            self.ticker_queue = self.ticker_queue.with_columns(
                pl.lit(None, dtype=pl.Datetime).alias('Download_time'),
                pl.lit(False, dtype=pl.Boolean).alias('Downloaded'),
                pl.lit(False, dtype=pl.Boolean).alias('Download_Failed')
            )
            logging.info(
                f"total amount of items in queue: {len(self.get_queue_total())}")

    def update_queue(self, tickers: list, val: bool):
        """
        update queries for the ticker queue dataframe.
        tickers: list[str]
            list of tickers that had either bad or good downloads
        val: bool
            indicates if the downloaded tickers were successful or not
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

    def insert_new_queue_records(self) -> None:
        """
        Insert new tickers into the queue
        """
        # check the source if there are new tickers
        ticker_list = list(set(self.df_target.select('Symbol').to_series()) - set(self.ticker_queue.select('Symbol').to_series()))
        if len(ticker_list) > 0:
            logging.info(f"Identified {len(ticker_list)} new tickers, updating ticker queue with the following tickers: {ticker_list}")
            downloaded_vals = [False for _ in range(len(ticker_list))]
            download_failed_vals = [False for _ in range(len(ticker_list))]
            downloaded_times = [None for _ in range(len(ticker_list))]
            # create the dataframe
            diff = pl.DataFrame(
                {'Symbol': ticker_list,
                 'Download_time': downloaded_times,
                 'Downloaded': downloaded_vals,
                 'Download_Failed': download_failed_vals},
                schema={'Symbol': pl.String,
                        'Download_time': pl.Datetime,
                        'Downloaded': pl.Boolean,
                        'Download_Failed': pl.Boolean}
            )
            # concat the ticker queue
            self.ticker_queue = pl.concat([self.ticker_queue, diff.select(self.ticker_queue.columns)])
            logging.info(f"Updated ticker_queue {self.ticker_queue.head()}")
        else:
            logging.info(f"no new tickers found to add to queue")

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

    def reset_queue(self) -> None:
        """
        Reset the queue when all the tickers have been downloaded
        """
        # update the values
        self.ticker_queue = self.ticker_queue.with_columns(
            pl.lit(False).alias("Downloaded"),
            pl.lit(False).alias("Download_Failed")
        )
        # sort the queue by download time
        self.ticker_queue = self.ticker_queue.sort(by="Download_time", descending=False)

    def _check_reset(self) -> None:
        """ Check if the queue needs to be reset"""
        tickers = self.get_queue_total()
        if len(tickers) == 0:
            logging.info(f"No items in the queue resetting queue ...")
            self.reset_queue()

    def get_multiprocessing_objects(self, num_cores: int, tickers: list[str]) -> list[AlphaIO]:
        """
        Returns a list of AlphaIO objects that will be ran concurrently
        num_cores: int
            number of cores to use
        tickers: list[str]
            The list of tickers to pull data for concurrently
        """
        # split the list of tickers into chunks of size num_cores
        sub_groups = round(len(tickers) / num_cores)
        tickers_sub_list = [tickers[i:i + sub_groups] for i in range(0, len(tickers), sub_groups)]
        logging.info(f"Created sublist of tickers for multiprocessing: {tickers_sub_list}")
        # create the list of alphio objects
        return [
            AlphaIO(tickers=t) for t in tickers_sub_list
        ]

    def run(self, num_cores: int = None) -> float:
        """
        The main run of stock tracker logical flow to return the stocks for retrieving data from alpha vantage

        num_cores: int
        The number of cores to use for multiprocessing to run program concurrently.
        By default it will perform single processing

        1. check locally if source data is available for updating or initializing the target ticker table
        2. Check if the data in s3 needs initializing, initialize if not
        3. make any updates inserts to the ticker table and write data to s3
        4. Check if the ticker queue is initialized, if not initialize it
        5. Add any new records to the queue

        """
        # check if local files are available
        start_time = time()
        source_files = list_local_files(file_path='data')
        if len(source_files) > 0:
            # get the source data
            logging.info(f"Found source data locally: {source_files}")
            self.get_stock_list_locally(file_path=source_files) # sets the source
        self._get_target() # sets the target if not in s3 initiliaze the dataframe
        # check if there is no data for both the source and target
        if self.df_source is None and self.df_target is None:
            logging.warning("No source and target data, closing ...")
        else:  # if no source or target data simply exit
            # update or insert the records from the source and target
            if not (self.df_target.filter(pl.col("is_current") == True).sort(pl.col("Symbol"),
                                                                             descending=False)
                    .select(self.df_source.columns)
                    .equals(self.df_source.sort(pl.col("Symbol"),
                            descending=False))):
                logging.info(f"Source and target data are not the same for the ticker data, updating ...")
                # no need to run the process if target equals the source
                target_tmp = self.df_target.filter(pl.col("is_current") == True)
                self.df_target = run_end_to_end(target=target_tmp, source=self.df_source, id_col="Symbol",
                                                update_time=datetime.now())
            logging.info(f"Size of the target being written to s3, {self.df_target.shape}")
            # write the target data to s3
            self.s3.s3_write_parquet(self.df_target, file_path=self.ticker_table)
            # get the queue
            self._get_ticker_queue()
            # check if the queue needs resetting
            self._check_reset()
            # update ticker queue
            self.insert_new_queue_records()
            tickers = self.get_queue_total()[:self.queue_depth]
            # pass the list of tickers to the alpha io object
            if num_cores is None:
                alphaio = AlphaIO(tickers=tickers)
                # run the alphaio object
                alphaio.run()
                self.write_ticker_queue(download_dict=alphaio.ticker_tracking_dict)
            else:
                logging.info(f"Number of cores is {num_cores}, running concurrently")
                # Create the alpaio objects
                alphaio_list = self.get_multiprocessing_objects(num_cores=num_cores, tickers=tickers)
                q = Queue()
                # create the processes
                processes = []
                # processes = [
                #     Process(target=a.run, args=q).run() for a in alphaio_list
                # ]
                for a in alphaio_list:
                    p = Process(target=a.run, args=(q,))
                    processes.append(p)
                    p.start()

                for p in processes:
                    p.join()
                download_results = [q.get() for _ in range(num_cores)]
                logging.info(f"Recieved the downloaded results from the {num_cores} cores\n{download_results}")
                # merge the list of dictionaries and update the ticker que
                download_results_dict = reduce(lambda a, b: {**a, **b}, download_results)
                self.write_ticker_queue(download_dict=download_results_dict)
        end_time = time()
        process_time = end_time - start_time
        logging.info(f"Finished in {process_time}")
        return process_time


if __name__ == '__main__':

    init_logger("stock_tracker.log")
    stock_tracker = StockTracker(queue_depth=1)
    stock_tracker.run(num_cores=None)

    # num_cores = [
    #     6, #4, 6
    # ]
    # queue_depth = [
    #     42, #16, 42
    # ]
    #
    # results = []
    #
    # for num_core in num_cores:
    #     for depth in queue_depth:
    #         # initialize the stock tracker
    #         record = {}
    #         if num_core is None:
    #             record['processing'] = 'Standard'
    #         else:
    #             record['processing'] = f'Multiprocessing: {num_core} cores'
    #         record['Number of Symbols Processed'] = depth
    #         stock_tracker = StockTracker(queue_depth=depth)
    #         time_sec = stock_tracker.run(num_cores=num_core)
    #         record['Time Elapsed'] = time_sec
    #         results.append(record)
    #
    # df_result = pl.DataFrame(results)
    # print(df_result)
    # df_result.write_parquet('results/stock_tracker_multi642.parq')




