from tqdm.auto import tqdm
import logging
import pandas as pd
import zipfile, io
import requests
import shutil
import time
import glob
import os
import sys, re
from urllib.parse import urljoin
from datetime import datetime

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

def is_leap_year(year):
    if (year%4 == 0 and year%100 != 0) or (year%400 == 0) :
        return 1
    else :
        return 0

############################
#ZIPS

def download_file(url):
    req = requests.get(url)
    return req

def extract_file(fname):
    z = zipfile.ZipFile(fname)
    z.extractall("./trades/")

def bulk_extract_files():
    files = show_all_zip_files()
    for file in files:
        extract_file(file)

def delete_zip_files():
    zips = show_all_zip_files()
    for zip in zips:
        os.remove(zip)

def delete_datasets():
    zips = show_all_datasets()
    for zip in zips:
        os.remove(zip)

def show_all_zip_files(path=f"{__location__}/historical_zip/*.zip"):
    return glob.glob(path)

def show_all_datasets(path=f"{__location__}/trades/*.csv"):
    return glob.glob(path)

############################
#DOWNLOAD DATA

def download_trades_data(year: int, start_month: int, end_month: int, start_day: int, end_day: int, tf: str, symbol: str, market_type: str):
    #DAILY_TRADES_SPOT = "https://data.binance.vision/?prefix=data/spot/daily/trades/"
    #MONTHLY_TRADES_SPOT = "https://data.binance.vision/?prefix=data/spot/monthly/trades/"
    if is_leap_year(year):
        days = [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    else:
        days = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    now = datetime.today()

    for i in range(start_month, end_month+1):

        month = i
        if (month != 0) and (month <= end_month):
            if tf == "daily":
                if market_type == 'spot':
                    base_url = f"https://data.binance.vision/data/spot/daily/trades/{symbol.upper()}/"
                else:
                    base_url = f"https://data.binance.vision/data/futures/um/daily/trades/{symbol.upper()}/"
                if int(month) < 10:
                    month = '0'+str(month)

                for day in range(start_day, days[i]+1):

                    if (day != end_day+1) and (month != end_month+1):
                        if int(day) < 10:
                            day = '0'+str(day)
                        
                        suffix = f"{symbol.upper()}-trades-{year}-{month}-{day}.zip"

                        url = urljoin(base_url, suffix)
                        #print(url)
                        with requests.get(url, stream=True) as r:
                            
                            # check header to get content length, in bytes

                            total_length = int(r.headers.get("Content-Length"))
                            
                            # implement progress bar via tqdm
                            with tqdm.wrapattr(r.raw, "read", total=total_length, desc="")as raw:
                            
                                # save the output to a file
                                with open(os.path.join(__location__+"/historical_zip/", suffix), 'wb')as output:
                                    shutil.copyfileobj(raw, output)
                    else:
                        break

            elif tf == "monthly":
                if market_type == 'spot':
                    base_url = f"https://data.binance.vision/data/spot/monthly/trades/{symbol.upper()}/"
                else:
                    base_url = f"https://data.binance.vision/data/futures/um/daily/trades/{symbol.upper()}/"

                if (month == now.month) and (year == now.year):
                    break
                
                else:
                    if int(month) < 10:
                        month = '0'+str(month)
                            
                    suffix = f"{symbol.upper()}-trades-{year}-{month}.zip"
                    
                    url = urljoin(base_url, suffix)
                    with requests.get(url, stream=True) as r:
                        
                        # check header to get content length, in bytes
                        total_length = int(r.headers.get("Content-Length"))
                        
                        # implement progress bar via tqdm
                        with tqdm.wrapattr(r.raw, "read", total=total_length, desc="")as raw:
                        
                            # save the output to a file
                            with open(os.path.join(__location__+"/historical_zip/", suffix), 'wb')as output:
                                shutil.copyfileobj(raw, output)

def merge_trades_datasets(market):
    files = sorted(show_all_datasets())
    actual_symbol = 0
    df = pd.DataFrame({'price':[],'qty':[],'quote_vol':[], 'ts':[],'side':[]})

    for file in files:
        name = file.split("/")[-1]
        splits = name.split("-")
        symbol = splits[0]

        if symbol != actual_symbol:
            
            #SYMBOL IS DIFFERENT, ERASE DF AND SAVE OLD DF
            if len(df) > 0 :
                df = df[['ts', 'price', 'qty', 'side', 'quote_vol']]
                df.set_index('ts', inplace=True)
                if market == 'spot':
                    df.to_csv(__location__+"/results/"+actual_symbol+'_spot.csv')
                else:
                    df.to_csv(__location__+"/results/"+actual_symbol+'_futures.csv')
                actual_symbol = symbol
                df = pd.DataFrame({'price':[],'qty':[],'quote_vol':[], 'ts':[],'side':[]})
                df1 = pd.read_csv(file)
                col_names = df1.columns
                df1 = df1.iloc[: , :-1]
                df1.rename(columns={col_names[0]: 'trade_id', col_names[1]: 'price', col_names[2]: 'qty', col_names[3]: 'quote_vol', col_names[4]: 'ts', col_names[5]: 'side' }, inplace=True)
                df1.drop(['trade_id'], axis=1, inplace=True)             
                df = df.append(df1, ignore_index=True)

            actual_symbol = symbol
        else:
            df1 = pd.read_csv(file)
            col_names = df1.columns
            df1 = df1.iloc[: , :-1]
            df1.rename(columns={col_names[0]: 'trade_id', col_names[1]: 'price', col_names[2]: 'qty', col_names[3]: 'quote_vol', col_names[4]: 'ts', col_names[5]: 'side' }, inplace=True)
            df1.drop(['trade_id'], axis=1, inplace=True)             
            df = df.append(df1, ignore_index=True)
    
    cols = df.columns
    if 'Unnamed: 0' in cols:
        df.drop(['Unnamed: 0'], axis=1, inplace=True)             
    df.set_index('ts', inplace=True)
    if market == 'spot':
        df.to_csv(__location__+"/results/"+actual_symbol+'_spot.csv')
    else:
        df.to_csv(__location__+"/results/"+actual_symbol+'_futures.csv')
    delete_datasets()
