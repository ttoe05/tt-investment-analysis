import polars as pl
import logging
from alpha_utils import list_local_files, init_logger

if __name__ == '__main__':
    # load local files
    init_logger("create_test_files.log")
    local_files_path = "data_backup"
    local_files = list_local_files(local_files_path)
    logging.info(local_files)
    # load the data into a polars data frame
    dfs = [pl.read_csv(f"{file}") for file in local_files]
    # concatenate the data frames
    df = pl.concat(dfs)
    # divide the data into 10 data frames
    # add an id column
    df = df.with_columns(pl.Series("id", range(1, df.height + 1)))
    logging.info(df.head())
    # save different versions of the data
    for id_num in range(1, df.height + 1):
        if id_num >= 2:
            if id_num % 2 == 0 or id_num == df.height:
                df_filtered = df.filter(pl.col("id") <= id_num)
                df_filtered.write_csv(f"data/df_{id_num}.csv")
                logging.info(f"Persisted {df_filtered.height} to data/df_{id_num}.csv")
