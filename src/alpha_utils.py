import os
import polars as pl
from datetime import datetime


def get_alpha_key() -> str:
    """
    get alphaVantage API key
    :return:
    """
    return os.environ['ALPHA_VANTAGE_API']


def parse_data(data: list[dict], str_cols: list[str]) -> pl.DataFrame:
    """
    Parse the data from the API
    Parameters
    _________________
    data: dict
        the data to parse
    str_cols: list
        the list of string columns to keep as string
    :return:
        pl.DataFrame
    """
    # convert to polars
    df = pl.DataFrame(data=data)
    # convert numerical columns to float and string columns to str
    df = df.with_columns(
        pl.col(column).cast(pl.Float64, strict=False) for column in df.columns if column not in str_cols
    )
    return df


def check_new_field(df_target: pl.DataFrame,
                    df_source: pl.DataFrame,
                    id_col: str = 'fiscalDateEnding') -> pl.DataFrame:
    """
    Check if there are new fields in the source dataframe that are not in the target dataframe
    :param df_target:
    :param df_source:
    :param id_col:
    :return:
        The updated target data frame
    """
    # get the new columns and merge to the target data frame
    new_column = list(set(df_source.columns) - set(df_target.columns))
    new_column.append(id_col)
    if len(new_column) == 0:
        return df_target
    else:
        return df_target.join(df_source.select(new_column), on=id_col, how='left')


def check_removed_field(df_target: pl.DataFrame,
                        df_source: pl.DataFrame) -> pl.DataFrame:
    """
    Check if there are fields in the target dataframe that are not in the source dataframe
    :param df_target:
    :param df_source:
    :return:
        The updated source data frame
    """
    # get the new columns and merge to the target data frame
    field_cols = [x for x in df_target.columns if x not in ['is_current', 'update_time']]
    removed_column = list(set(df_target.select(field_cols).columns) - set(df_source.columns))
    if len(removed_column) == 0:
        return df_source
    else:
        return df_source.with_columns(pl.lit(None).alias(x) for x in removed_column)


def update_records(
        target: pl.DataFrame,
        source: pl.DataFrame,
        on: str = 'fiscalDateEnding',
        update_time: datetime = datetime.now()
) -> pl.DataFrame:
    """
    The following function looks to update the records of a slowly changing dimension type 2 data frame. Using a source dataframe

    Parameters:
    target (pl.DataFrame): The target data frame that will be updated
    source (pl.DataFrame): The source data frame that will be used to update the target data frame


    Returns:
    pl.DataFrame: The updated target data frame
    """
    # find the records that differ between the two data frames
    diff = source.join(target, on=on, suffix='_df2').filter(pl.any_horizontal(
        pl.col(x).ne_missing(pl.col(f"{x}_df2"))
        for x in source.columns if x != 'fiscalDateEnding')).select(source.columns)
    # add the update time and is_current flag to the df_diff
    diff = diff.with_columns(
        pl.lit(True).alias("is_current"),
        pl.lit(update_time).alias("update_time")
    )
    # get the list of fiscalDateEnding that are in the df_diff
    date_list = diff.select('fiscalDateEnding').to_series()
    # update the is current flag for the old records
    df_updated = target.with_columns(
        is_current=pl.when(pl.col('fiscalDateEnding').is_in(date_list))
        .then(pl.lit(False))
        .otherwise(pl.col('is_current')))
    # concat the two data frames and order by fiscalDateEnding
    print(f"Shape of target: {df_updated.shape}")
    print(f"Shape of diff: {diff.shape}")
    df_updated = pl.concat([df_updated, diff.select(df_updated.columns)]).sort('fiscalDateEnding')
    return df_updated


def insert_new_records(target: pl.DataFrame,
                       source:pl.DataFrame,
                       id_col: str = 'fiscalDateEnding',
                       update_time: datetime = datetime.now()) -> pl.DataFrame:
    """
    Find the new records in the source dataframe and them to the target dataframe
    Parameters:
    target (pl.DataFrame): The target data frame that will be updated
    source (pl.DataFrame): The source data frame that will be used to update the target data frame


    Returns:
    pl.DataFrame: The updated target data frame with the new records added
    """
    # get the ids that are in the source and not the target
    source_ids = source.select(id_col).to_series()
    target_ids = target.select(id_col).to_series()
    # subtract the set of source ids from the target ids to get the new record ids
    new_record_ids = list(set(source_ids) - set(target_ids))
    # check if the new record ids are empty
    if len(new_record_ids) == 0:
        return target
    else:
        # filter the source data frame on the new record ids
        new_records = source.filter(pl.col(id_col).is_in(new_record_ids))
        # add the is_current and update time to the new records
        new_records = new_records.with_columns(
                pl.lit(True).alias("is_current"),
                pl.lit(update_time).alias("update_time")
            )
        # add the new records to the target data frame
        target = pl.concat([target, new_records.select(target.columns)]).sort(id_col)
        return target


def run_end_to_end(target: pl.DataFrame,
                   source:pl.DataFrame,
                   id_col: str = 'fiscalDateEnding',
                   update_time: datetime = datetime.now()) -> pl.DataFrame:
    """
    Run the full end-to-end process
    """
    # check for new fields
    target = check_new_field(df_target=target, df_source=source, id_col=id_col)
    # check for removed fields
    source = check_removed_field(df_target=target, df_source=source)
    # update the records
    target = update_records(target=target, source=source, on=id_col, update_time=update_time)
    # insert new records
    target = insert_new_records(target=target, source=source, id_col=id_col, update_time=update_time)
    return target


def list_local_files(file_path: str) -> list:
    """
    List all the files in a local directory and return a list of files
    """
    return [x for x in os.listdir(file_path) if x.endswith(".csv")]









