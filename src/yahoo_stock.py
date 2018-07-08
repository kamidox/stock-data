# -*- coding: utf-8 -*-
import urllib
import os
import datetime
import pandas as pd
import numpy as np
import threading
import Queue
import time
import traceback
from contextlib import closing


def retrive_stock_data(stockid, folder):
    """ 下载整个股票数据 """

    print('%s: downloading %s to %s' % (threading.current_thread().getName(), stockid, folder))
    url = 'http://table.finance.yahoo.com/table.csv?s=%s' % (stockid)
    fname = os.path.join(folder, '%s.csv' % stockid.split('.')[0])
    if not os.path.isdir(folder):
        os.mkdir(folder)
    with closing(urllib.urlopen(url)) as s:
        if s.getcode() != 200:
            print('%s: downloading %s error. status=%d' % (threading.current_thread().getName(), stockid, s.getcode()))
            return
        with open(fname, 'wb') as f:
            f.write(s.read())


def update_stock_data(stockid, folder):
    """ 更新股票数据，如果不存在，则下载。如果存在，则只更新最近日期的数据 """

    fname = os.path.join(folder, '%s.csv' % stockid.split('.')[0])
    if not os.path.exists(fname):
        retrive_stock_data(stockid, folder)
        return

    try:
        data = pd.read_csv(fname, index_col='Date', parse_dates=True)
    except Exception, e:
        print('%s: error to read %s. drop it and download again.' % (threading.current_thread().getName(),
                                                                     fname))
        retrive_stock_data(stockid, folder)
        return

    if len(data) == 0:
        print('%s: %s is empty. drop it and download again.' % (threading.current_thread().getName(),
                                                                     fname))
        retrive_stock_data(stockid, folder)
        return

    last_date = data.iloc[0:1].index.tolist()[0]
    today = pd.Timestamp(datetime.date.today())
    if today - last_date < pd.Timedelta(days=2):
        # print('%s: Nothing to update. %s last date is %s.' % (threading.current_thread().getName(),
        #                                                      stockid, last_date.date()))
        return

    print('%s: updatting %s to from %s to %s' % (threading.current_thread().getName(),
                                                 stockid, last_date.date(), today.date()))
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
        data = pd.read_csv(files[i], header=None, names=['name', 'id'],
                           dtype={'id': np.string0},
                           skipinitialspace=True)
        data['postfix'] = postfixs[i]
        stocks.append(data)

    data = pd.concat(stocks)
    print('%d files. %d stocks.' % (len(files), len(data)))
    return data


def update_stock_data_by_loop():
    """ 批量更新所有股票数据 """

    slist = stock_list(['SH.txt', 'SZ.txt'], ['.ss', '.sz'])
    for i in range(len(slist)):
        update_stock_data(s['id'] + s['postfix'], '../yahoo-data')


class StockDownloadThread(threading.Thread):
    """ thread to download stock data """
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            try:
                stockid = self.queue.get(block=True, timeout=10)
            except Queue.Empty:
                print('%s: queue is empty. exit working thread.' % self.getName())
                return

            try:
                update_stock_data(stockid, '../yahoo-data')
            except Exception, e:
                print('%s: error to download %s, try again later.' % (self.getName(), stockid))
                # print(traceback.format_exc())
            self.queue.task_done()


def update_stock_data_by_threading():
    start = time.time()
    queue = Queue.Queue()
    # create a thread pool
    for i in range(10):
        t = StockDownloadThread(queue)
        t.start()

    # fill data into queue
    slist = stock_list(['SH.txt', 'SZ.txt'], ['.ss', '.sz'])
    for i in range(len(slist)):
        s = slist.iloc[i]
        queue.put(s['id'] + s['postfix'])

    # wait until all is downloaded
    queue.join()
    print("Elapsed Time: %s" % (time.time() - start))


if __name__ == '__main__':
    update_stock_data_by_threading()
    #update_stock_data('300498.sz', '../yahoo-data')
