#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.9

# @Time   : 2023/1/22 0:30
# @Author : 'Lou Zehua'
# @File   : main.py 

import os
import sys


if '__file__' not in globals():
    # !pip install ipynbname  # Remove comment symbols to solve the ModuleNotFoundError
    import ipynbname

    nb_path = ipynbname.path()
    __file__ = str(nb_path)
cur_dir = os.path.dirname(__file__)
pkg_rootdir = cur_dir  # os.path.dirname()向上一级，注意要对应工程root路径
if pkg_rootdir not in sys.path:  # 解决ipynb引用上层路径中的模块时的ModuleNotFoundError问题
    sys.path.append(pkg_rootdir)
    print('-- Add root directory "{}" to system path.'.format(pkg_rootdir))


import pandas as pd

from script.crawling_OSDB_list import crawling_OSDB_list_soup, recalc_OSDB_list
from script.crawling_OSDB_infos import crawling_OSDB_infos_soup, pd_select_col, recalc_OSDB_info
from script.join_OSDB_list_OSDB_info import join_OSDB_list_OSDB_info

UPDATE_OSDB_LIST = False  # This will take a long time to crawl the dbdb.io website if set to True...
UPDATE_OSDB_INFO = True  # This will take a long long time to crawl many dbdb.io websites if set to True......
RECALC_OSDB_LIST = True  # Add "Name" column
RECALC_OSDB_INFO = True  # Check "Name" column; Representing "Data Model" "Source Code" "Start Year" "End Year" columns.
JOIN_OSDB_SUMMARY_INFO_ON_NAME = True  # join OSDB summary and OSDB_infos on filed 'Name' and 'Name'

month_yyyyMM = "202303"


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
src_OSDB_info_joined_last_month_manulabeled_path = os.path.join(pkg_rootdir, f'data/manulabeled/OSDB_info_{last_month_yyyyMM}_joined_manulabled.csv')
OSDB_crawling_path = os.path.join(pkg_rootdir, f'data/dbdbio_OSDB_list/OSDB_crawling_{month_yyyyMM}_raw.csv')
OSDB_info_crawling_path = os.path.join(pkg_rootdir, f'data/dbdbio_OSDB_list/OSDB_info_crawling_{month_yyyyMM}_raw.csv')
OSDB_info_joined_path = os.path.join(pkg_rootdir, f'data/dbdbio_OSDB_list/OSDB_info_{month_yyyyMM}_joined.csv')
OSDB_info_joined_manulabled_path = os.path.join(pkg_rootdir, f'data/manulabeled/OSDB_info_{month_yyyyMM}_joined_manulabled.csv')

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
        # dbdb.io http link
        # - use "https://dbdb.io/browse?type=open-source" to get all the open source databases in dbdb.io
        # - use "https://dbdb.io/browse?q=*" to get all the databases in dbdb.io
        dbdbio_url = "https://dbdb.io"
        dbdbio_OSDB_url = dbdbio_url + "/browse?type=open-source"
        url_init = dbdbio_OSDB_url
        use_elem_dict = {
            'main_contents': ['form', {'id': 'mainsearch'}],
        }
        crawling_OSDB_list_soup(url_init, headers[0], use_elem_dict, save_path=OSDB_crawling_path, url_root=dbdbio_url)
    if RECALC_OSDB_LIST:
        recalc_OSDB_list(path=OSDB_crawling_path)

    if UPDATE_OSDB_INFO:
        df_OSDB_table = pd.read_csv(OSDB_crawling_path, encoding=encoding, index_col=False)
        # dbdbio_insitelink
        df_db_names_urls = df_OSDB_table[['card_title', 'card_title_href']]
        df_db_names_urls.columns = ['db_names', 'urls']

        use_elem_dict = {
            'main_contents': ['div', {'class': 'row justify-content-md-center system-content'}],
        }
        mode = 'a'  # mode 'a' for breakpoint resumption
        batch = 20
        temp_save_path = OSDB_info_crawling_path.rstrip('.csv') + '_temp.csv'
        state = -1
        crawling_OSDB_infos_soup(df_db_names_urls, headers, use_elem_dict, save_path=OSDB_info_crawling_path, mode=mode,
                                 temp_save_path=temp_save_path, batch=batch)

        use_cols = ["Name", "card_title", "Description", "Data Model", "Query Interface", "System Architecture", "Website",
                    "Source Code", "Tech Docs", "Developer", "Country of Origin", "Start Year", "End Year",
                    "Project Type", "Written in", "Supported languages", "Embeds / Uses", "Licenses",
                    "Operating Systems"]
        pd_select_col(use_cols, temp_save_path, OSDB_info_crawling_path)

    if RECALC_OSDB_INFO:
        recalc_OSDB_info(path=OSDB_info_crawling_path)

    default_dtype = {'Start Year': str, 'End Year': str}
    if JOIN_OSDB_SUMMARY_INFO_ON_NAME:
        df_OSDB_list = pd.read_csv(OSDB_crawling_path, encoding=encoding, index_col=False)
        df_OSDB_infos = pd.read_csv(OSDB_info_crawling_path, encoding=encoding, index_col=False, dtype=default_dtype)
        join_OSDB_list_OSDB_info(df_OSDB_list, df_OSDB_infos, save_path=OSDB_info_joined_path, on_pair=("Name", "Name"),
                                 key_alias="DBMS_uriform", encoding=encoding)
    LAST_MONTH_MANULABELED = True
    UPDATE_START_CHECKPOINT_USE_MANULABELED = 1
    if LAST_MONTH_MANULABELED:
        update_start_checkpoint_path = OSDB_info_joined_manulabled_path if UPDATE_START_CHECKPOINT_USE_MANULABELED else OSDB_info_joined_path
        df_update_start_checkpoint = pd.read_csv(update_start_checkpoint_path, encoding=encoding, index_col=False, dtype=default_dtype)
        df_src_OSDB_info_joined_last_month_manulabeled = pd.read_csv(src_OSDB_info_joined_last_month_manulabeled_path, encoding=encoding, index_col=False, dtype=default_dtype)
        key_update_start_checkpoint, key_last_month_joined_manulabeled = "DBMS_uriform", "DBMS_uriform"
        df_OSDB_list_OSDB_info_joined_manulabeled = df_update_start_checkpoint.set_index(key_update_start_checkpoint, inplace=False)
        df_src_OSDB_info_joined_last_month_manulabeled.set_index(key_last_month_joined_manulabeled, inplace=True)
        # overwrite=False: only update values that are NA in the original DataFrame.
        df_OSDB_list_OSDB_info_joined_manulabeled.update(df_src_OSDB_info_joined_last_month_manulabeled, join='left', overwrite=False, errors='ignore')
        df_OSDB_list_OSDB_info_joined_manulabeled.reset_index(inplace=True)
        df_OSDB_list_OSDB_info_joined_manulabeled.to_csv(OSDB_info_joined_manulabled_path, encoding=encoding, index=False)
