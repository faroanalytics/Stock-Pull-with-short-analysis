import pandas as pd
import yfinance as yf
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import datetime

pd.options.display.float_format = '{:.4f}'.format
pd.options.display.max_columns = None
pd.options.display.max_rows = None

def calculate_returns(ticker, industry, adj_close, risk_free_rate=0.02):
    returns = adj_close.pct_change().dropna()

    annual_return = (returns + 1).product()**(252/returns.count()) - 1
    quarterly_return = (returns + 1).product()**(63/returns.count()) - 1
    average_monthly_return = (returns + 1).product()**(21/returns.count()) - 1

    std_dev = returns.std() * np.sqrt(252)
    sharpe_ratio = (annual_return - risk_free_rate) / std_dev

    return {
        'Tickers': ticker,
        'Industries': industry,
        'annual_return': annual_return,
        'quarterly_return': quarterly_return,
        'average_monthly_return': average_monthly_return,
        'std_dev': std_dev,
        'sharpe_ratio': sharpe_ratio
    }

def main():
    df2 = pd.read_csv(r"D:\Python - VSCode\bigtickerlist2023edit-test.csv")
    df2.columns = ['Tickers', 'Industries']
    df2 = df2[~df2['Tickers'].str.contains(r'[\^.-]')]
    df2_tickers = df2.Tickers.tolist()

    today = datetime.date.today()
    d = datetime.timedelta(days=1000)
    past_time = today - d

    stock_data = []

    for ticker in df2_tickers:
        try:
            df3 = yf.download(ticker, start=past_time, end=today)
            adj_close = df3['Adj Close']
            industry = df2[df2['Tickers'] == ticker]['Industries'].iloc[0]
            stock_data.append((ticker, industry, adj_close))
        except:
            pass

    # Calculate returns, standard deviation, and Sharpe ratio using threading
    with ThreadPoolExecutor() as executor:
        returns_futures = [
            executor.submit(calculate_returns, ticker, industry, adj_close, risk_free_rate=0.02)
            for ticker, industry, adj_close in stock_data
        ]

        returns_list = [future.result() for future in returns_futures]

    global returns_df
    returns_df = pd.DataFrame(returns_list)
    print(returns_df)

if __name__ == '__main__':
    main()