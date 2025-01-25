import polars as pl
import logging
from s3io import S3IO
from alpha_utils import list_local_files, init_logger, get_bucket_name, get_profile_name

if __name__ == '__main__':
    # load local files
    init_logger("create_test_files.log")
    # local_files_path = "data_backup"
    # local_files = list_local_files(local_files_path)
    # logging.info(local_files)
    # # load the data into a polars data frame
    # dfs = [pl.read_csv(f"{file}") for file in local_files]
    # # concatenate the data frames
    # df = pl.concat(dfs)
    # # divide the data into 10 data frames
    # # get the US companies
    # df = df.filter(pl.col('Country') == 'United States')
    # # add an id column
    # df = df.with_columns(pl.Series("id", range(1, df.height + 1)))
    # df = pl.read_csv('data_backup/NYSE_basic_materials.csv')
    # df = df.filter(pl.col('Symbol').is_in(['RGLD']))
    # logging.info(f"Market Cap column\n{df.select(['Symbol', 'Market Cap'])}")
    # logging.info(f"data to be persisted\n{df.head()}")
    # # df.write_csv(f"data/df_cvx_xom.csv")


    # logging.info(df.head())
    # # save different versions of the data
    # for id_num in range(1, df.height + 1):
    #     if id_num >= 2:
    #         if id_num % 2 == 0 or id_num == df.height:
    #             df_filtered = df.filter(pl.col("id") <= id_num)
    #             df_filtered.write_csv(f"data/df_{id_num}.csv")
    #             logging.info(f"Persisted {df_filtered.height} to data/df_{id_num}.csv")


    # update the ticker queue table
    ticker_table = "stock_tracker/tickers_queue.parq"
    bucket = get_bucket_name()
    profile = get_profile_name()
    s3io = S3IO(bucket=bucket,
                profile=profile)

    ticker_df = s3io.s3_read_parquet(file_path=ticker_table)
    logging.info(f"ticker_table: {ticker_df}")


    # # update the values
    # ticker_table = ticker_table.with_columns(
    #     pl.lit(False).alias("Downloaded"),
    #     pl.lit(False).alias("Download_Failed")
    # )
    # ticker_df = ticker_df.with_columns(
    #     Downloaded=pl.when(pl.col('Symbol').is_in(['XOM', 'CVX']))
    #     .then(pl.lit(False))
    #     .otherwise(pl.col('Downloaded')),
    #     Download_Failed=pl.when(pl.col('Symbol').is_in(['XOM', 'CVX']))
    #     .then(pl.lit(False))
    #     .otherwise(pl.col('Download_Failed'))
    # )
    # logging.info(f"ticker_table: {ticker_df}")
    # # write table to s3
    # s3io.s3_write_parquet(df=ticker_df, file_path=ticker_table)