# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
import datetime


# Manual config black list here. We skip some stock for very bad quality of data
BLACK_LIST = [
    'SZ131809.csv',
    'SZ131800.csv',
    'SH120485.csv',
    'SH202001.csv',
    'SH202007.csv',
    'SH202003.csv',
    'SZ131801.csv',
    'SH120520.csv',
    'SH201008.csv',
    'SH201010.csv',
    'SZ131805.csv',
    'SZ131804.csv',
    'SH204014.csv',
    'SZ131806.csv',
    'SZ131802.csv',
    'SH204028.csv',
    'SH600629.csv',
    'SH120509.csv',
    'SZ000592.csv',
    'SH120519.csv',
    'SZ131803.csv',
    'SZ000650.csv',
    'SZ002272.csv',
    'SZ000578.csv',
    'SH600137.csv',
    'SH204007.csv'
]


# 股票涨跌幅检查，不能超过 10% ，过滤掉一些不合法的数据
def _valid_price(g):
    return (((g.max() - g.min()) / g.min()) < 0.223).all()


def minutes_to_days(iname, oname, mode='a'):
    """ convert 5 minutes stock data to day stock data """

    if not os.path.isfile(iname):
        print('error: file do not exists %s' % iname)
        return

    names = ['date',
             'time',
             'opening_price',
             'ceiling_price',
             'floor_price',
             'closing_price',
             'volume',
             'amount']
    raw = pd.read_csv(iname, names=names, header=None, index_col='date', parse_dates=True)

    days = raw.groupby(level=0).agg(
        {'opening_price': lambda g: _valid_price(g) and g[0] or 0,
         'ceiling_price': lambda g: _valid_price(g) and np.max(g) or 0,
         'floor_price': lambda g: _valid_price(g) and np.min(g) or 0,
         'closing_price': lambda g: _valid_price(g) and g[-1] or 0,
         'volume': 'sum',
         'amount': 'sum'})
    days = days.replace(0, np.nan).dropna()
    print('append %d items from %s to %s' % (len(days), iname, oname))
    exists = os.path.exists(oname)
    days.to_csv(oname, mode=mode, header=(mode == 'w' or (not exists)))


def minutes_to_days_batch(basedir='raw',
                          outdir='data',
                          dirs=['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008']):
    """ Convert 5 minutes stock data, 1 minutes to day data in batch mode

        This will read all stock data under basedir, convert it into outdir
    """

    if not os.path.isdir(basedir) or not os.path.exists(basedir):
        print('error: input directory not exist. %s' % basedir)
        return
    if not os.path.isdir(outdir) or not os.path.exists(outdir):
        print('error: output directory not exist. %s' % outdir)
        return

    for d in dirs:
        d = os.path.join(basedir, d)
        files = os.listdir(d)
        for f in files:
            oname = os.path.join(outdir, os.path.split(f)[1])
            f = os.path.join(d, f)
            minutes_to_days(f, oname)


def stock_ripples(data_file, period=30):
    """ calculate the mean ripples range of the stock

    data_file: The stock data file, for example 'data/SH600690.csv'
    period: Ripples in the time range of days. This is calculate base on the nature days.
    mean_items: the mean range is computed by top number of ripples. If you want to compute
         the mean of all ripples, set mean_items = 0
    """

    fname = os.path.split(data_file)[1]
    if fname in BLACK_LIST:
        print('error: %s in black list. Skip this stock.' % data_file)
        return None

    if not os.path.isfile(data_file):
        print('error: file do not exists %s' % data_file)
        return None

    def _gen_item_group_index(total, group_len):
        """ generate an item group index array

        suppose total = 10, unitlen = 2, then we will return array [0 0 1 1 2 2 3 3 4 4]
        """

        group_count = total / group_len
        group_index = np.arange(total)
        i = 0
        for i in range(group_count):
            group_index[i * group_len: (i + 1) * group_len] = i
        group_index[(i + 1) * group_len: total] = i + 1
        return group_index.tolist()

    try:
        data = pd.read_csv(data_file, index_col='date', parse_dates=True)
    except Exception, e:
        print('error: failed to read csv from %s' % data_file)
        print(e)
        return None

    # fill with missing data. use ffill method to prices, and set 0 to volumn and amount
    # compute date index
    l = len(data)
    start = data.iloc[0:1].index.tolist()[0]
    end = data.iloc[l - 1: l].index.tolist()[0]
    idx = pd.date_range(start=start, end=end)
    data = data.reindex(idx)
    # select volumn and amount
    zvalues = data.loc[~(data.volume > 0)].loc[:, ['volume', 'amount']]
    # fill data
    data.update(zvalues.fillna(0))
	data.fillna(method='ffill', inplace=True)

    group_index = _gen_item_group_index(len(data), period)
    data['group_index'] = group_index
    _ceiling_price = lambda g: g.idxmin() < g.idxmax() and np.max(g) or (-np.max(g))
    group = data.groupby('group_index').agg({'volume': 'sum',
                                             'floor_price': 'min',
                                             'ceiling_price': _ceiling_price})
    group['ripples_radio'] = group.ceiling_price / group.floor_price
    # add start date to each group
    date = pd.DataFrame({"group_index": group_index, "date": idx})
    group['date'] = date.groupby('group_index').agg('first')

    ripples = group.sort_values('ripples_radio', ascending=False)
    mean_ripples = ripples.head(10).ripples_radio.mean()
    print('mean ripples range on top 10 in period of %d for %s: %.04f'
          % (period, data_file, mean_ripples))
    return ripples


def stock_ripples_batch(basedir='data', period=20):
    """ select the top 10 stock which have largest ripple range """

    if not os.path.isdir(basedir) or not os.path.exists(basedir):
        print('error: idirectory not exist. %s' % basedir)
        return

    def _mean_rise_ripple(f):
        ripples = stock_ripples(os.path.join(basedir, f), period)
        if ripples is None:
            return np.nan
        mean_ripples = ripples.head(10).ripples_radio.mean()
        return mean_ripples

    def _mean_fall_ripple(f):
        ripples = stock_ripples(os.path.join(basedir, f), period)
        if ripples is None:
            return np.nan
        mean_ripples = ripples.tail(10).ripples_radio.mean()
        return mean_ripples

    _stock_id = lambda f: f.split('.')[0]
    files = os.listdir(basedir)
    ripples_list = [(_stock_id(f), _mean_rise_ripple(f), _mean_fall_ripple(f)) for f in files if f.endswith('.csv')]

    ripples = pd.DataFrame(ripples_list, columns=['stock_id', 'mean_rise_ripples', 'mean_fall_ripples'])

    top = 10
    all_ripples = ripples.dropna().sort_values('mean_rise_ripples', ascending=False)

    print('top %d rise ripples in period of %d for all the stocks in %s:' % (top, period, basedir))
    print(all_ripples.head(top))

    return all_ripples


def ripple_raw_data(stock_file, ripple_idx=0, days=30):
    ripple = stock_ripples(stock_file)
    date = ripple.iloc[ripple_idx].date
    date_start = pd.Timestamp(date)
    date_end = date_start + pd.Timedelta(days=days)

    days_data = pd.read_csv(stock_file, index_col='date', parse_dates=True)
    return days_data[date_start:date_end]


def recent_ripples(basedir='data', end_date=None, period=30):
    """ recent ripples for all stock in basedir

        basedir: directory of data
        end_date: end date, default to now
        period: period before end date
    """

    if not os.path.isdir(basedir) or not os.path.exists(basedir):
        print('error: idirectory not exist. %s' % basedir)
        return

    if end_date is None:
        end_date = pd.Timestamp(datetime.datetime.now())

    def _ripple(f, start, end):
        data = pd.read_csv(os.path.join(basedir, f), index_col='date', parse_dates=True)
        data = data.loc[start_date:end_date]
        _ripple_radio = lambda data: data.ceiling_price.max() / data.floor_price.min()
        if data.floor_price.idxmin() < data.ceiling_price.idxmax():
            ripple_radio = _ripple_radio(data)
        else:
            ripple_radio = - _ripple_radio(data)
        return ripple_radio

    files = os.listdir(basedir)
    _stock_id = lambda f: f.split('.')[0]
    end_date = pd.Timestamp(end_date)
    start_date = end_date - pd.Timedelta(days=period)
    ripples_list = [(_stock_id(f), _ripple(f, start_date, end_date)) for f in files if f.endswith('.csv')]
    ripples = pd.DataFrame(ripples_list, columns=['stock_id', 'ripples'])

    all_ripples = ripples.sort_values('ripples', ascending=False)

    print('head 5 recent ripples in period of %d for all stocks in %s till %s:' % (period, basedir, end_date))
    print(all_ripples.head(5))
    print('tail 5 recent ripples in period of %d for all stocks in %s till %s:' % (period, basedir, end_date))
    print(all_ripples.tail(5))

    return all_ripples


def row_data(stock_file, end_date=None, period=30):
    """ return daily data for the stock in period till end date """

    if end_date is None:
        end_date = pd.Timestamp(datetime.datetime.now())

    end_date = pd.Timestamp(end_date)
    start_date = end_date - pd.Timedelta(days=period + 1)
    data = pd.read_csv(stock_file, index_col='date', parse_dates=True)
    data = data.loc[start_date:end_date]

    # compute rise
    rise = data.closing_price.diff()
    data['rise'] = rise
    # compute rise ratio: rise / closing price on previous day
    length = len(data)
    rise_ratio = data.rise[1:length].values / data.closing_price[0: length - 1].values
    start_date = start_date + pd.Timedelta(days=1)
    data = data.loc[start_date:end_date]    # remove first row
    data['rise_ratio'] = rise_ratio
    return data


def main():
    print("Please refer to stock.ipynb. You need ipython notebook to run stock.ipynb.")
    # minutes_to_days_batch(basedir='raw', outdir='data', dirs=['2004', '2005', '2006', '2007', '2008'])
    # print('mean ripples range: %.04f' % stock_ripples('data/SH600690.csv', 20))
    # ripples = stock_ripples_batch(basedir='test-data', period=30)
    # ripples.to_csv('ripples.csv', index=False)
    # recent_ripples(basedir='test-data', end_date='2007-11-30', period=30)


if __name__ == '__main__':
    main()
