#!/bin/bash

rm -r test-raw
rm -r test-data

mkdir -p test-data
mkdir -p test-raw/2000
mkdir -p test-raw/2001
mkdir -p test-raw/2002
mkdir -p test-raw/2003
mkdir -p test-raw/2004
mkdir -p test-raw/2005
mkdir -p test-raw/2006
mkdir -p test-raw/2007
mkdir -p test-raw/2008

function copy_raw_data()
{
    cp raw/2000/$1 test-raw/2000/$1
    cp raw/2001/$1 test-raw/2001/$1
    cp raw/2002/$1 test-raw/2002/$1
    cp raw/2003/$1 test-raw/2003/$1
    cp raw/2004/$1 test-raw/2004/$1
    cp raw/2005/$1 test-raw/2005/$1
    cp raw/2006/$1 test-raw/2006/$1
    cp raw/2007/$1 test-raw/2007/$1
    cp raw/2008/$1 test-raw/2008/$1
}

copy_raw_data SH1A0001.csv
copy_raw_data SH600273.csv
copy_raw_data SH600685.csv
copy_raw_data SH600686.csv
copy_raw_data SH600687.csv
copy_raw_data SH600688.csv
copy_raw_data SH600689.csv
copy_raw_data SH600690.csv
copy_raw_data SH600691.csv
copy_raw_data SH600692.csv
copy_raw_data SH600693.csv
copy_raw_data SH600694.csv
copy_raw_data SZ000001.csv
copy_raw_data SZ000560.csv
copy_raw_data SZ000561.csv
copy_raw_data SZ000562.csv
copy_raw_data SZ000563.csv
copy_raw_data SZ000564.csv
copy_raw_data SZ000565.csv
copy_raw_data SZ000566.csv
copy_raw_data SZ000567.csv
copy_raw_data SZ000568.csv
copy_raw_data SZ000569.csv
copy_raw_data SZ131809.csv
copy_raw_data SZ161607.csv
