import yfinance as yf

def get_stock_price(symbol: str, interval: str = "daily"):
    """
    Fetch stock price data from Yahoo Finance.

    Args:
        symbol (str): Stock ticker (e.g., 'AAPL', 'PTT.BK')
        interval (str): 'daily', 'weekly', 'monthly'

    Returns:
        pandas.DataFrame: Historical price data
    """

    # Map user-friendly interval to yfinance interval
    interval_map = {
        "daily": "1d",
        "weekly": "1wk",
        "monthly": "1mo"
    }

    if interval not in interval_map:
        raise ValueError("interval must be 'daily', 'weekly', or 'monthly'")

    yf_interval = interval_map[interval]

    df = yf.Ticker(symbol).history(
        interval=yf_interval,
        period="max",
        auto_adjust=False  # keep raw + adj close
    )

    df["return"] = df["Adj Close"].pct_change()

    return df