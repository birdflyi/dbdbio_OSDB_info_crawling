#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.7

# @Time   : 2023/2/20 22:09
# @Author : 'Lou Zehua'
# @File   : join_OSDB_list_OSDB_info.py 

import os
import sys

if '__file__' not in globals():
    # !pip install ipynbname  # Remove comment symbols to solve the ModuleNotFoundError
    import ipynbname

    nb_path = ipynbname.path()
    __file__ = str(nb_path)
cur_dir = os.path.dirname(__file__)
pkg_rootdir = os.path.dirname(cur_dir)  # os.path.dirname()向上一级，注意要对应工程root路径
if pkg_rootdir not in sys.path:  # 解决ipynb引用上层路径中的模块时的ModuleNotFoundError问题
    sys.path.append(pkg_rootdir)
    print('-- Add root directory "{}" to system path.'.format(pkg_rootdir))

import pandas as pd


def join_OSDB_list_OSDB_info(df_OSDB_list, df_OSDB_infos, save_path, on_pair,
                             use_cols_OSDB_list=None, use_cols_OSDB_info=None,
                             key_alias=None, encoding="utf-8"):
    on_pair = on_pair or ("Name", "Name")
    on_df_OSDB_list = on_pair[0]
    on_df_OSDB_info = on_pair[1]
    temp_sorted_set_keycol1 = sorted(list(set(df_OSDB_list[on_df_OSDB_list].values)))
    temp_sorted_set_keycol2 = sorted(list(set(df_OSDB_infos[on_df_OSDB_info].values)))
    assert(temp_sorted_set_keycol1 == temp_sorted_set_keycol2 and
           len(df_OSDB_list[on_df_OSDB_list]) == len(df_OSDB_infos[on_df_OSDB_info]) == len(temp_sorted_set_keycol1))

    key_alias = key_alias or on_pair[0]
    use_cols_OSDB_list = use_cols_OSDB_list or list(df_OSDB_list.columns)
    if on_df_OSDB_list not in use_cols_OSDB_list:
        use_cols_OSDB_list = [on_df_OSDB_list] + use_cols_OSDB_list
    use_cols_OSDB_info = use_cols_OSDB_info or list(df_OSDB_infos.columns)
    if on_df_OSDB_info not in use_cols_OSDB_info:
        use_cols_OSDB_info = [on_df_OSDB_info] + use_cols_OSDB_info

    # remove the overlaped column in right df df_OSDB_info_filtered from use_cols_OSDB_info
    use_cols_OSDB_info_drop_overlap = []
    for colname in use_cols_OSDB_info:
        if colname not in use_cols_OSDB_list or colname in [on_df_OSDB_list, on_df_OSDB_info, key_alias]:
            use_cols_OSDB_info_drop_overlap.append(colname)
    use_cols_OSDB_info = use_cols_OSDB_info_drop_overlap

    for colname in use_cols_OSDB_info:
        if colname not in df_OSDB_infos.columns:
            print(f'Column name {colname} not in df_OSDB_infos.columns! Try to ignore this column...')
            use_cols_OSDB_info.remove(colname)
    df_OSDB_list_filtered = df_OSDB_list[use_cols_OSDB_list]
    df_OSDB_info_filtered = df_OSDB_infos[use_cols_OSDB_info]
    df_OSDB_list_filtered.set_index(on_df_OSDB_list, inplace=True)
    df_OSDB_list_filtered.index.name = key_alias  # change index name from "Name" to "DBMS_uriform"
    df_OSDB_info_filtered.set_index(on_df_OSDB_info, inplace=True)
    df_OSDB_info_filtered.index.name = key_alias  # change index name from "Name" to "DBMS_uriform"
    # see https://stackoverflow.com/questions/26645515/pandas-join-issue-columns-overlap-but-no-suffix-specified
    # result: "card_title_left" is overlap with "card_title_right", remove the overlaped column in right df df_OSDB_info_filtered from use_cols_OSDB_info
    df_OSDB_list_OSDB_info_joined = df_OSDB_list_filtered.join(df_OSDB_info_filtered, on=key_alias, how='left',
                                                               lsuffix='_left', rsuffix='_right')

    df_OSDB_list_OSDB_info_joined.reset_index(inplace=True)
    # pd.set_option("display.max_columns", None)
    # print(df_OSDB_list_OSDB_info_joined)
    # save to csv
    df_OSDB_list_OSDB_info_joined.to_csv(save_path, encoding=encoding, index=False)
    print(save_path, 'saved!')


if __name__ == '__main__':
    encoding = "utf-8"
    OSDB_crawling_path = os.path.join(pkg_rootdir, f'data/dbdbio_OSDB_list/OSDB_crawling_202301_raw.csv')
    OSDB_info_crawling_path = os.path.join(pkg_rootdir,
                                           f'data/dbdbio_OSDB_list/OSDB_info_crawling_202301_raw.csv')
    OSDB_info_joined_path = os.path.join(pkg_rootdir, f'data/dbdbio_OSDB_list/OSDB_info_202301_joined.csv')

    df_OSDB_list = pd.read_csv(OSDB_crawling_path, encoding=encoding, index_col=False)
    df_OSDB_infos = pd.read_csv(OSDB_info_crawling_path, encoding=encoding, index_col=False, dtype={'Start Year': str, 'End Year': str})
    join_OSDB_list_OSDB_info(df_OSDB_list, df_OSDB_infos, save_path=OSDB_info_joined_path, on_pair=("Name", "Name"),
                             key_alias="DBMS_uriform", encoding=encoding)
