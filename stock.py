# -*- coding: utf-8 -*-
import pandas as pd
import os


def to_day_data(iname, oname, append=True):
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
    raw = pd.read_csv(iname, names=names, header=None)
    day = raw.groupby('date').agg(
        {'opening_price': 'first',
         'ceiling_price': 'max',
         'floor_price': 'min',
         'closing_price': 'last',
         'volume': 'sum',
         'amount': 'sum'})
    print('append %d items from %s to %s' % (len(day), iname, oname))
    exists = os.path.exists(oname)
    day.to_csv(oname, mode=(append and 'a' or 'w'), header=((not append) or (not exists)))


def pre_process(basedir, dirs, outdir):
    """ pre-process data. Convert 5 minutes stock data, 1 minutes """

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
            to_day_data(f, oname)


def main():
    basedir = "./raw/"
    outdir = "./data/"
    dirs = ['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008']
    pre_process(basedir, dirs, outdir)


if __name__ == '__main__':
    main()
