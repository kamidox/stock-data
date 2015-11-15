# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os


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


def get_stock_ripples(data_file, period=20):
    """ calculate the mean ripples range of the stock

    data_file: The stock data file, for example 'data/SH600690.csv'
    period: group size
    top: the mean range is computed by top number of ripples. If you want to compute
         the mean of all ripples, set top = 0
    """

    fname = os.path.split(data_file)[1]
    if fname in BLACK_LIST:
        print('%s in black list. Skip this stock.' % data_file)
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

    def _is_valid_ripples(group):
        """ filter out invalid ripples """
        id_of_floor_price = group['floor_price'].idxmin()
        id_of_ceiling_price = group['floor_price'].idxmax()
        return id_of_ceiling_price > id_of_floor_price

    try:
        data = pd.read_csv(data_file)
    except Exception, e:
        print('error: failed to read csv from %s' % data_file)
        print(e)
        return None

    group_index = _gen_item_group_index(len(data), period)
    data['group_index'] = group_index
    # filter out invalid ripples, since for stock, only the raising ripple is valid
    valid_data = data.groupby('group_index').filter(_is_valid_ripples)
    valid_group = valid_data.groupby('group_index').agg({'date': 'first',
                                                         'volume': 'sum',
                                                         'floor_price': 'min',
                                                         'ceiling_price': 'max'})
    valid_group['ripples_radio'] = valid_group.ceiling_price / valid_group.floor_price
    valid_ripples = valid_group.sort_values('ripples_radio', ascending=False)
    mean_ripples = valid_ripples.head(10).ripples_radio.mean()
    print('mean ripples range on top 10 in period of %d for %s: %.04f'
          % (period, data_file, mean_ripples))
    return valid_ripples


def get_all_ripples(basedir='data', period=20, mean_num=10):
    """ select the top 10 stock which have largest ripple range """

    if not os.path.isdir(basedir) or not os.path.exists(basedir):
        print('error: idirectory not exist. %s' % basedir)
        return

    def _mean_ripples(f):
        stock_ripples = get_stock_ripples(os.path.join(basedir, f), period)
        if stock_ripples is None:
            return np.nan
        mean_ripples = stock_ripples.head(mean_num).ripples_radio.mean()
        return mean_ripples
    _stock_id = lambda f: f.split('.')[0]
    files = os.listdir(basedir)
    ripples_list = [(_stock_id(f), _mean_ripples(f)) for f in files if f.endswith('.csv')]

    ripples = pd.DataFrame(ripples_list, columns=['stock_id', 'mean_ripples'])

    top = 10
    all_ripples = ripples.dropna().sort_values('mean_ripples', ascending=False)

    print('top %d ripple range in period of %d for all the stocks in %s:' % (top, period, basedir))
    print(all_ripples.head(top))

    return all_ripples


def ripple_raw_data(stock_file, ripple_idx=0, days=30):
    ripple = get_stock_ripples(stock_file)
    date = ripple.iloc[ripple_idx].date
    date_start = pd.Timestamp(date)
    date_end = date_start + pd.Timedelta(days=days)

    days_data = pd.read_csv(stock_file, index_col='date', parse_dates=True)
    return days_data[date_start:date_end]


def main():
    print("Please refer to stock.ipynb. You need ipython notebook to run stock.ipynb.")
    minutes_to_days_batch(basedir='test-raw', outdir='test-data')
    # print('mean ripples range: %.04f' % get_stock_ripples('data/SH600690.csv', 20))
    ripples = get_all_ripples(basedir='test-data', period=20)
    ripples.to_csv('ripples.csv', index=False)


if __name__ == '__main__':
    main()
