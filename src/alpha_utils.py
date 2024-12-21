import os


def get_alpha_key() -> str:
    """
    get alphaVantage API key
    :return:
    """
    return os.environ['ALPHA_VANTAGE_API']