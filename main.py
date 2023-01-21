#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.9

# @Time   : 2023/1/22 0:30
# @Author : 'Lou Zehua'
# @File   : main.py 

import os
import sys


cur_dir = os.path.dirname(os.path.realpath(__file__))
pkg_rootdir = cur_dir  # os.path.dirname()向上一级，注意要对应工程root路径
if pkg_rootdir not in sys.path:
    sys.path.append(pkg_rootdir)
    print('-- Add root directory "{}" to system path.'.format(pkg_rootdir))


import socket
import pandas as pd

from script.crawling_OSDB_list import crawling_OSDB_list_soup
from script.crawling_OSDB_infos import crawling_OSDB_infos_soup


UPDATE_OSDB_LIST = False  # This will take a long time to crawl the dbdb.io website if set to True...
UPDATE_OSDB_INFO = True  # This will take a long long time to crawl many dbdb.io websites if set to True......
JOIN_OSDB_SUMMARY_INFO_ON_CARD_TITLE = False  # join OSDB summary and OSDB_infos on filed 'card_title' and 'card_title'
RECALC_OSDB_INFO = False
INCREMENTAL = False

month_yyyyMM = "202301"


def get_last_month_yyyyMM(curr_month_yyyyMM):
    curr_month_yyyyMM = str(curr_month_yyyyMM)
    assert(curr_month_yyyyMM.isdecimal() and len(curr_month_yyyyMM) == 6)
    import datetime

    curr_year = int(month_yyyyMM[:4])
    curr_month = int(month_yyyyMM[4:6])
    curr_month_datetime = datetime.datetime(curr_year, curr_month, 1)  # first day of current month
    last_month_datetime = curr_month_datetime - datetime.timedelta(days=1)  # last day of last month
    return last_month_datetime.strftime("%Y%m")


last_month_yyyyMM = get_last_month_yyyyMM(month_yyyyMM)
OSDB_crawling_path = os.path.join(pkg_rootdir, f'data/dbdbio_OSDB_list/OSDB_crawling_{month_yyyyMM}_raw.csv')
OSDB_info_crawling_path = os.path.join(pkg_rootdir, f'data/dbdbio_OSDB_list/OSDB_info_crawling_{month_yyyyMM}_raw.csv')
OSDB_info_joined_path = os.path.join(pkg_rootdir, f'data/dbdbio_OSDB_list/OSDB_info_{month_yyyyMM}_joined.csv')

encoding = 'utf-8'

# headers info when use Chrome explorer
header1 = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.221 Safari/537.36 SE 2.X MetaSr 1.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
}
header2 = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36'}
header3 = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}
header4 = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'}
header5 = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}
header6 = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
headers = [header1, header2, header3, header4, header5, header6]

if __name__ == '__main__':

    if UPDATE_OSDB_LIST:
        dbdbio_url = "https://dbdb.io"
        dbdbio_OSDB_url = dbdbio_url + "/browse?type=open-source"
        url_init = dbdbio_OSDB_url
        use_elem_dict = {
            'main_contents': ['form', {'id': 'mainsearch'}],
        }
        crawling_OSDB_list_soup(url_init, headers[0], use_elem_dict, save_path=OSDB_crawling_path, url_root=dbdbio_url)

    if UPDATE_OSDB_INFO:
        df_ranking_table = pd.read_csv(OSDB_crawling_path, encoding=encoding, index_col=False)
        # dbdbio_insitelink
        df_db_names_urls = df_ranking_table[['card_title', 'card_title_href']]
        df_db_names_urls.columns = ['db_names', 'urls']

        use_elem_dict = {
            'main_contents': ['div', {'class': 'row justify-content-md-center system-content'}],
        }
        mode = 'a'  # mode 'a' for breakpoint resumption
        batch = 20
        temp_save_path = OSDB_info_crawling_path.rstrip('.csv') + '_temp.csv'
        state = -1
        while state == -1:
            try:
                state = crawling_OSDB_infos_soup(df_db_names_urls, headers, use_elem_dict, save_path=OSDB_info_crawling_path, mode=mode, temp_save_path=temp_save_path, batch=batch)
            except socket.timeout:
                continue

    if JOIN_OSDB_SUMMARY_INFO_ON_CARD_TITLE:
        pass  # TODO
