from s3io import S3IO
from alpha_utils import get_bucket_name, get_profile_name
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
import logging
import polars as pl


def get_statement(ticker: str, statement: str) -> pl.DataFrame:
    """
    Get the financial statement for a given ticker
    Statement values should be either cash, balance, earnings, or income
    """
    if statement not in ['cash', 'balance', 'income', 'earnings']:
        raise ValueError('Invalid statement value')
    # get the s3 credentials to access the data in s3
    s3_profile = get_profile_name()
    s3_bucket = get_bucket_name()
    s3 = S3IO(bucket=s3_bucket, profile=s3_profile)
    # get the data
    if statement == 'earnings':
        df_tmp = s3.s3_read_parquet(f"{statement}/{ticker}/{statement}.parq")
        df_tmp = df_tmp.with_columns(
            pl.col("fiscalDateEnding").dt.cast_time_unit("us").alias("fiscalDateEnding")
        )
        return df_tmp
    else:
        return s3.s3_read_parquet(f"{statement}/{ticker}/{statement}.parq")


def scale_data(df: pl.DataFrame, subset: list[str] = None) -> pl.DataFrame:
    """
    Run the standardscaler on the given dataframe. Pass a list of subset columns to run the standardscaler on.
    """
    # set the columns to run the standard scaler on
    if subset is None:
        columns = df.columns
    else:
        columns = subset
    # set the transformer
    transformers = [(col, StandardScaler(), [col]) for col in columns]
    transformer = ColumnTransformer(transformers=transformers, remainder='passthrough')
    # Set the output to polars
    transformer.set_output(transform="polars")
    return transformer.fit_transform(df)

def transform_statement_fiscal_date(df: pl.DataFrame, date_cols: list[str]) -> pl.DataFrame:
    """
    Convert the fiscal date ending
    """
    for date_col in date_cols:
        try:
            df_tmp = df.with_columns(
                pl.col(date_col).str.to_datetime(format="%Y-%m-%d").alias(date_col)
            )
        except Exception as e:
            print(e)
            df_tmp = df
    return df_tmp


def merge_dataframes(dfs: list[pl.DataFrame], on: str) -> pl.DataFrame:
    """
    Merge multiple dataframes together
    """
    counter = 0
    df_tmp = None
    for df in dfs:
        cols = [x for x in df.columns if x not in ['reportedCurrency', 'is_current', 'update_time']]
        if counter == 0:
            df_tmp = df.select(cols)
            counter += 1
            continue
        else:
            df_tmp = df_tmp.join(df.select(cols), on=on, how='inner')
    return df_tmp


def income_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate the metrics using the income statement
    """
    # sort by fisical date descending order
    df = df.sort(by=['fiscalDateEnding'], descending=True)
    return df.with_columns(
        Revenue_Growth=((pl.col("totalRevenue") - pl.col("totalRevenue").shift(1)) / pl.col("totalRevenue").shift(1)),
        grossProfit_margin=(pl.col("grossProfit") / pl.col("totalRevenue")),
        ROIC=(pl.col("operatingIncome") * ((1 - pl.col("incomeTaxExpense")) / pl.col("incomeBeforeTax"))),
        netIncome_margin=pl.col("netIncome") / pl.col("totalRevenue"),
        EBIT_margin=pl.col("ebit") / pl.col("totalRevenue"),
        EBITDA_margin=pl.col("ebitda") / pl.col("totalRevenue")
    )

def return_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate the metrics using the shareholder statement
    """
    df = df.sort(by=['fiscalDateEnding'], descending=True)
    return df.with_columns(
        ROE=(pl.col("netIncome") / pl.col("totalShareholderEquity")),
        ROA=(pl.col("netIncome") / pl.col("totalCurrentAssets")),
        assetTurnover=(pl.col("totalRevenue") / pl.col("totalCurrentAssets"))
    )

def balance_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate the metrics using the balance statement
    """
    df = df.sort(by=['fiscalDateEnding'], descending=True)
    return df.with_columns(
        debt_to_equity_ratio=(pl.col("shortLongTermDebtTotal") / pl.col("totalShareholderEquity")),
        debt_to_asset_ratio=(pl.col("shortLongTermDebtTotal") / pl.col("totalCurrentAssets")),
        debt_to_capital_ratio=(pl.col("shortLongTermDebtTotal") / (pl.col("shortLongTermDebtTotal") + pl.col("totalShareholderEquity"))),
        equity_ratio=(pl.col("totalShareholderEquity") / pl.col("totalCurrentAssets")),
        solvency_ratio=((pl.col("netIncome") + pl.col("depreciation")) / pl.col("totalCurrentLiabilities")),
    )


def cash_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate the metrics using the cash statement
    """
    df = df.sort(by=['fiscalDateEnding'], descending=True)
    return df.with_columns(
        operating_cash_flow=(pl.col("operatingCashflow") / pl.col("totalCurrentLiabilities")),
        cashflow_margin=(pl.col("operatingCashflow") / pl.col("totalRevenue")),
        efficiency_ratio=(pl.col("operatingCashflow") / pl.col("totalCurrentAssets")),
        cashflow_debt_ratio=(pl.col("operatingCashflow") / pl.col("shortLongTermDebtTotal")),

    )


if __name__ == "__main__":
    ticker = 'MU'

    statements = ['income', 'cash', 'balance', 'earnings']
    dfs = [get_statement(ticker, x) for x in statements]
    dfs = [transform_statement_fiscal_date(df=df, date_cols=['fiscalDateEnding']) for df in dfs]
    counter = 0
    for df in dfs:
        print(f"shape of dataframe {counter}:\t{df.shape}")
        print(df.head())
        counter += 1

    df_merged = merge_dataframes(dfs=dfs, on='fiscalDateEnding')
    print(f"shape of df_merged: {df_merged.shape}")
    # run the standard scaler
    # columns = [x for x in df_tmp.columns if x not in ['fiscalDateEnding', 'is_current', 'update_time', 'reportedCurrency']]
    # df_tmp = scale_data(df=df_tmp, subset=columns)
    # df_tmp = transform_statement_fiscal_date(df=df_tmp, date_cols=['remainder__fiscalDateEnding'])
    # print(df_tmp.head())


