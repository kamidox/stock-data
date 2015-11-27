# -*- coding: utf-8 -*-
import urllib
import os
import datetime
import pandas as pd
import numpy as np


def retrive_stock_data(stockid, folder):
    """ 下载整个股票数据 """

    print('downloading %s to %s' % (stockid, folder))
    url = 'http://table.finance.yahoo.com/table.csv?s=%s' % (stockid)
    fname = os.path.join(folder, '%s.csv' % stockid.split('.')[0])
    if not os.path.isdir(folder):
        os.mkdir(folder)
    urllib.urlretrieve(url, fname)


def update_stock_data(stockid, folder):
    """ 更新股票数据，如果不存在，则下载。如果存在，则只更新最近日期的数据 """

    fname = os.path.join(folder, '%s.csv' % stockid.split('.')[0])
    if not os.path.exists(fname):
        retrive_stock_data(stockid, folder)
        return

    data = pd.read_csv(fname, index_col='Date', parse_dates=True)

    last_date = data.iloc[0:1].index.tolist()[0]
    today = pd.Timestamp(datetime.date.today())
    if today - last_date < pd.Timedelta(days=2):
        print('Nothing to update. %s last date is %s.' % (stockid, last_date))
        return

    print('updatting %s to from %s to %s' % (stockid, last_date.date(), today.date()))
    query = [
        ('a', last_date.month - 1),
        ('b', last_date.day),
        ('c', last_date.year),
        ('d', today.month - 1),
        ('e', today.day),
        ('f', today.year),
        ('s', stockid),
    ]
    url = 'http://table.finance.yahoo.com/table.csv?%s' % urllib.urlencode(query)
    temp_file = fname + '.tmp'
    urllib.urlretrieve(url, temp_file)
    update_data = pd.read_csv(temp_file, index_col='Date', parse_dates=True)
    data = data.append(update_data)
    data.sort_index(ascending=False, inplace=True)
    data.to_csv(fname, mode='w')
    os.unlink(temp_file)


def stock_list(files, postfixs):
    """ 合并股票列表，输出合并后的，可以通过 yahoo api 获取的股票列表

    files: a sequence like ['SH.txt', 'SZ.txt']
    postfixs: a sequence map to files, like ['.ss', '.sz']
    """
    if len(files) != len(postfixs):
        print('error: size of files and postfixs not match.')
        return

    stocks = []
    for i in range(len(files)):
        data = pd.read_csv(files[i], header=None, names=['name', 'id'], dtype={'id': np.string0})
        data['postfix'] = postfixs[i]
        stocks.append(data)

    data = pd.concat(stocks)
    print('%d files. %d stocks.' % (len(files), len(data)))
    return data


def update_stock_data_batch():
    """ 批量更新所有股票数据 """

    slist = stock_list(['SH.txt', 'SZ.txt'], ['.ss', '.sz'])
    for i in range(len(slist)):
        s = slist.iloc[i]
        update_stock_data(s['id'] + s['postfix'], 'yahoo-data')
