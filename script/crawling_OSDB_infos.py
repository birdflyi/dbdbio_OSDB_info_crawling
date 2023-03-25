#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.9

# @Time   : 2023/1/22 1:13
# @Author : 'Lou Zehua'
# @File   : crawling_OSDB_infos.py
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
    print('Add root directory "{}" to system path.'.format(pkg_rootdir))

import bs4
import re
import shutil
import socket
import time
import urllib

import pandas as pd

from bs4 import BeautifulSoup
from collections import Iterable
from urllib import request

STATE_OK = 0
KEY_ATTR_NAME = 'Name'

def process_delimeter(s):
    # new line
    s = re.sub(r'\r?\n', ' ', s)
    s = re.sub(r'\r', ' ', s)

    # deduplicate
    s = re.sub(r' +', ' ', s)
    s = re.sub(r',+', ',', s)
    return s


def crawling_dbms_info_soup(url_init, header, use_elem_dict, **kwargs):
    socket.setdefaulttimeout(60)
    request = urllib.request.Request(url_init, headers=header)
    response = urllib.request.urlopen(request)
    response_body = response.read().decode('utf-8').replace('&shy;', '')
    response.close()  # 注意关闭response
    soup = BeautifulSoup(response_body, 'lxml')  # 利用bs4库解析html

    # 取出主内容
    main_contents = soup.find_all(use_elem_dict['main_contents'][0], attrs=use_elem_dict['main_contents'][1])
    db_info_table = main_contents[0]

    # 获取所需文本
    summary_col = db_info_table.find_all("div", {"class": "col-sm-12 col-md-7 order-2 order-md-1"})[0]
    feature_col = db_info_table.find_all("div", {"class": "col-sm-12 col-md-3 order-1 order-md-2"})[0]

    # parse summary_col
    dbms_info_record_attrs_dict = {}
    preset_dict = kwargs.get("preset_dict", {})
    dbms_info_record_attrs_dict.update(**preset_dict)
    summary_divs = summary_col.find_all("div", {"class": "card"})

    descrips_div = summary_divs[0]
    db_name_value = descrips_div.find("h2", {"class": "card-title"}).get_text(',', '<br/>').strip()
    db_name_value = process_delimeter(db_name_value)
    dbms_info_record_attrs_dict["card_title"] = db_name_value

    db_descrips = descrips_div.find_all("p")
    db_descrips_strs = []
    for db_descrip in db_descrips:
        db_descrips_str = db_descrip.get_text(',', '<br/>').strip()
        db_descrips_str = process_delimeter(db_descrips_str)
        db_descrips_strs.append(db_descrips_str)

    db_descrips_str_joined = ' '.join(db_descrips_strs).strip()
    dbms_info_record_attrs_dict["Description"] = db_descrips_str_joined

    for summary_div in summary_divs[1:]:
        id_str = summary_div.attrs.get("id")
        if id_str:
            card_title = summary_div.find("h4", {"class": "card-title"}).get_text(',', '<br/>').strip()
            card_text = summary_div.find("p", {"class": "card-text"}).get_text(',', '<br/>').strip()
            card_title = process_delimeter(card_title)
            card_text = process_delimeter(card_text)
            dbms_info_record_attrs_dict[card_title] = card_text

    # parse feature_col
    feature_div_bodys = feature_col.find("div", {"class": "card has-citations"}).find("div", {"class": "card-body"})
    # feature_div_bodys = feature_divs
    feature_div_bodys = feature_div_bodys.contents
    card_titles = []
    card_texts = []
    temp_value_part_strs = []
    for feature_div_body in feature_div_bodys:
        if type(feature_div_body) != bs4.element.Tag:
            continue
        else:
            if feature_div_body.name == 'h6':
                if temp_value_part_strs:
                    temp_value_full_strs = ' '.join(temp_value_part_strs).strip()
                    temp_value_full_strs = process_delimeter(temp_value_full_strs)
                    card_texts.append(temp_value_full_strs)
                    temp_value_part_strs = []  # reset
                card_title = feature_div_body.get_text(',', '<br/>').strip()
                card_title = process_delimeter(card_title)
                card_titles.append(card_title)

            elif feature_div_body.name == 'p':
                temp_value_part_strs.append(feature_div_body.get_text(',', '<br/>').strip())
            else:
                pass

    if temp_value_part_strs:
        temp_value_full_strs = ' '.join(temp_value_part_strs).strip()
        card_texts.append(temp_value_full_strs)
        temp_value_part_strs = []
    assert(len(card_titles) == len(card_texts))
    dbms_info_record_attrs_dict.update(**dict(zip(card_titles, card_texts)))
    return dbms_info_record_attrs_dict


def crawling_OSDB_infos_soup(df_db_names_urls, headers, use_elem_dict, save_path, use_cols=None, use_all_impl_cols=True, **kwargs):
    ADD_MODE = kwargs.get('mode', None) == 'a'
    temp_save_path = kwargs.get('temp_save_path', None)
    if ADD_MODE:
        print('Add mode...')
        if not temp_save_path:
            print('Error: add mode must specify a "temp_save_path" in arguments!')
            return

    try:
        df_db_names_urls = pd.DataFrame(df_db_names_urls)[["db_names", "urls"]]  # [89:91]
    except:
        if type(df_db_names_urls) == dict:
            df_db_names_urls = pd.DataFrame(df_db_names_urls.items(), columns=["db_names", "urls"])

    default_use_cols = [KEY_ATTR_NAME, "card_title", "Description", "Data Model", "Query Interface", "System Architecture", "Website",
                        "Source Code", "Tech Docs", "Developer", "Country of Origin", "Start Year", "End Year",
                        "Project Type", "Written in", "Supported languages", "Embeds / Uses", "Licenses", "Operating Systems"]
    use_cols = use_cols or default_use_cols
    if use_all_impl_cols:
        if use_cols:
            print("Warning: use_all_impl_cols=True will disable the parameter use_cols!")
    else:
        if KEY_ATTR_NAME not in use_cols:
            use_cols = [KEY_ATTR_NAME] + use_cols

    df_dbms_infos = pd.DataFrame()

    len_db_names = len(df_db_names_urls)

    df1 = pd.DataFrame()
    batch = 100
    idx_start_end = [0, len_db_names]

    if ADD_MODE:
        encoding = kwargs.get('encoding', 'utf-8')
        index_col = kwargs.get('index_col', False)
        try:
            df1 = pd.read_csv(temp_save_path, encoding=encoding, index_col=index_col)
        except FileNotFoundError:
            df1.to_csv(temp_save_path, encoding='utf-8', index=False)
        except BaseException:
            pass

        len_df1 = len(df1)
        batch = kwargs.get('batch', batch)
        idx_start_end = kwargs.get('idx_start_end', [len_df1, min(len_df1 + batch, len_db_names)])

        if not (len_df1 == idx_start_end[0] and idx_start_end[0] <= idx_start_end[1]):
            raise ValueError(f"Wrong settings: len_df1 = {len_df1} should be in {idx_start_end}!")

    print('idx_start_end:', idx_start_end)
    for i in list(range(len_db_names))[idx_start_end[0]: idx_start_end[1]]:
        db_name_card_title, url = df_db_names_urls.iloc[i]
        db_name_uri = str(url).split('/')[-1]  # db_name_card_title may be duplicated! use db_name splited from url instead.
        print(f"{i}/{len_db_names}: Crawling data for {db_name_card_title} on {url} ...")
        header = headers[i % len(headers)]
        dbms_info_record_attrs_dict = crawling_dbms_info_soup(url, header, use_elem_dict, preset_dict={KEY_ATTR_NAME: db_name_uri})
        if use_all_impl_cols:
            use_cols = list(dbms_info_record_attrs_dict.keys())
        try:
            crawling_db_name = dbms_info_record_attrs_dict[KEY_ATTR_NAME]
        except ValueError:
            print("The website dbdb.io may have changed the key attribute of DBMS system properties table! Please"
                  "update KEY_ATTR_NAME!")
            return

        if db_name_uri == crawling_db_name:
            series_dbms_info = pd.Series(data=None, index=use_cols, dtype=str)
            series_dbms_info.update(pd.Series(dbms_info_record_attrs_dict))
            # series_dbms_info = series_dbms_info[use_cols]
            df_dbms_infos[db_name_uri] = series_dbms_info
        else:
            print(f"Unmatched dbms name, expect {db_name_uri} but get {crawling_db_name} please check the website: {url}")
        time.sleep(0.5)
        # break

    df_dbms_infos = df_dbms_infos.T
    # print(df_dbms_infos)

    if ADD_MODE:
        df2 = df_dbms_infos
        has_new_data = idx_start_end[1] < len_db_names or len(df2)
        if not has_new_data:  # The exit of recursive crawling
            print(f"Index >= {len_db_names}, the crawling tasks has done! idx_start_end:{idx_start_end}.")
            print(f'- Copy {temp_save_path} directly to {save_path}!')
            shutil.copyfile(temp_save_path, save_path)
            return STATE_OK
        else:  # need breakpoint resumption
            # save the data crawled in this batch
            join = kwargs.get('join', 'outer')
            df_dbms_infos_batched = pd.concat([df1, df2], join=join)
            df_dbms_infos_batched.to_csv(temp_save_path, encoding='utf-8', index=False)
            print(f"{temp_save_path} saved! idx_start_end:{idx_start_end}.")
            # Recursive crawling when this batch has new data
            new_idx_start_end = [idx_start_end[1], min(idx_start_end[1] + batch, len_db_names)]
            kwargs['idx_start_end'] = new_idx_start_end
            crawling_OSDB_infos_soup(df_db_names_urls, headers, use_elem_dict, save_path, use_cols=use_cols,
                                     use_all_impl_cols=use_all_impl_cols, **kwargs)

    # save to csv
    df_dbms_infos.to_csv(save_path, encoding='utf-8', index=False)
    print(save_path, 'saved!')
    return STATE_OK


def pd_select_col(cols, src_path, tar_path, encoding="utf-8", index_col=False, **kwargs):
    df = pd.read_csv(src_path, encoding=encoding, index_col=index_col, **kwargs)
    df[cols].to_csv(tar_path, encoding=encoding, index=index_col)
    return


def validate_label_mapping_table(str_series, k_v_colnames=None, mapping_table_path=None, encoding="utf-8", index_col=False):
    elem_splited_notna = [[e.strip() for e in s.split(',')] for s in pd.Series(str_series).dropna()]
    elem_splited_flatten = sum(elem_splited_notna, [])  # use sum as the iterate tool
    elem_set_sorted = list(set(elem_splited_flatten))
    mapping_table_path = mapping_table_path or os.path.join(pkg_rootdir, f'data/existing_tagging_info/category_labels_mapping_table.csv')
    df_category_labels_mapping_table = pd.read_csv(mapping_table_path, encoding=encoding, index_col=index_col)
    k_v_colnames = k_v_colnames or ["category_label", "category_name"]
    category_name_col = df_category_labels_mapping_table[k_v_colnames[1]]
    # validate
    for e in elem_set_sorted:
        if e not in list(category_name_col):
            raise KeyError(f"The key {e} must be in category_name_col: \n{category_name_col} ")
    raw_df_k_v_cols = df_category_labels_mapping_table[k_v_colnames]

    def merge2dict_df_k_v_cols(df, k_colname, v_colname):
        temp_dict = {}
        for i in range(len(df)):
            k = df.loc[i, k_colname]
            v = df.loc[i, v_colname]
            if temp_dict.get(k, None) is not None:
                temp_elem_list = temp_dict[k].split(',')
                temp_elem_list.append(v)
                temp_dict[k] = ','.join(temp_elem_list)
            else:
                temp_dict[k] = v
        return temp_dict

    dict_k_category_labels__v_category_names = merge2dict_df_k_v_cols(raw_df_k_v_cols, k_v_colnames[0], k_v_colnames[1])
    dict_k_category_names__v_category_labels = merge2dict_df_k_v_cols(raw_df_k_v_cols, k_v_colnames[1], k_v_colnames[0])
    mapping_dicts = {
        "raw_df_k_v_cols": raw_df_k_v_cols,
        "label_dict": dict_k_category_labels__v_category_names,
        "mapping_dict": dict_k_category_names__v_category_labels,
    }
    return mapping_dicts


def mapping_values2labels(item, **kwargs):
    mapping_dict = kwargs.get("mapping_dict")
    if not mapping_dict:
        raise KeyError("Key 'mapping_dict' can not be found!")
    if pd.isna(item):
        return item
    else:
        temp_item_list = [mapping_dict[e.strip()] for e in item.split(',')]  # e.g. "Object-Relational, Network"
        flatten_item_list = []
        for elem in temp_item_list:
            elem_list = elem.split(',')  # "Object oriented,Relational",Object-Relational: the key may be multi-types.
            flatten_item_list.append(elem_list)
        flatten_item_list = sum(flatten_item_list, [])
        flatten_item_list_dedup = list(set(flatten_item_list))
        flatten_item_list_dedup.sort(key=flatten_item_list.index)  # recover the order by the first hit index
        return ",".join(flatten_item_list_dedup)


def recalc_OSDB_info(path, encoding="utf-8", index_col=False):
    df_dbms_infos = pd.read_csv(path, encoding=encoding, index_col=index_col)
    check_not_empty = lambda x: any(x) if isinstance(x, Iterable) else pd.notna(x)
    check_distinct = lambda x: check_not_empty(x) and len(x) == len(set(x))
    is_from_github = lambda x: False if pd.isna(x) else str(x).startswith("https://github.com/")
    get_github_owner_repo = lambda x: '/'.join(x.replace("https://github.com/", "").split("/")[:2]) if is_from_github(x) else ""
    to_int_str = lambda x: "" if pd.isna(x) else str(int(x))
    # def abstract_label_mapping_table(strs):
    #     return list(set(strs))
    # def mapping_values2labels(strs):
    #     set(strs)
    recalc_func_dict = {
        KEY_ATTR_NAME: {"validate_func": check_distinct},
        # Representing "Data Model" "Source Code" "Start Year" "End Year" columns.
        "Data_Model_mapping": {"apply_param_preprocess_func": validate_label_mapping_table, "apply_func": mapping_values2labels, "input_col": "Data Model"},
        "has_open_source_github_repo": {"apply_func": lambda x: "Y" if is_from_github(x) else "", "input_col": "Source Code"},  # need to be labeled manually
        "github_repo_link": {"apply_func": lambda x: get_github_owner_repo(x), "input_col": "Source Code"},  # need to be labeled manually
        "org_name": {"apply_func": lambda x: x.split("/")[0] if x else "", "input_col": "github_repo_link"},
        "repo_name": {"apply_func": lambda x: x.split("/")[1] if len(x.split("/")) > 1 else "", "input_col": "github_repo_link"},
        "open_source_license": {"apply_func": lambda x: "Y" if check_not_empty(x) else "", "input_col": "Source Code"},
        "Start Year": {"apply_func": to_int_str},
        "End Year": {"apply_func": to_int_str},
    }
    for recalc_k, recalc_v in recalc_func_dict.items():
        input_col = recalc_v.get("input_col", recalc_k)
        kwargs = {}
        try:
            if recalc_v.get("validate_func"):
                if not recalc_v["validate_func"](df_dbms_infos[input_col]):
                    raise Warning(f"Column {recalc_k} can not pass the validation settings in {recalc_k}: {recalc_v}!")
            if recalc_v.get("apply_param_preprocess_func"):
                kwargs = recalc_v["apply_param_preprocess_func"](df_dbms_infos[input_col])
            if recalc_v.get("apply_func"):
                df_dbms_infos[recalc_k] = df_dbms_infos[input_col].apply(recalc_v["apply_func"], **kwargs)
        except (TypeError, KeyError, ValueError) as e:
            raise ValueError(f"Bad settings in {recalc_k}: {recalc_v}!\nError message: {e}")
    # 4. save to csv
    save_path = path
    df_dbms_infos.to_csv(save_path, encoding='utf-8', index=False)
    print(save_path, 'recalculated!')
    return None


if __name__ == '__main__':
    month_yyyyMM = "202301"
    OSDB_crawling_path = os.path.join(pkg_rootdir, f'data/dbdbio_OSDB_list/OSDB_crawling_{month_yyyyMM}_raw.csv')
    OSDB_info_crawling_path = os.path.join(pkg_rootdir, f'data/dbdbio_OSDB_list/OSDB_info_crawling_{month_yyyyMM}_raw.csv')

    encoding='utf-8'
    df_OSDB_table = pd.read_csv(OSDB_crawling_path, encoding=encoding, index_col=False)
    # dbdbio_insitelink
    df_db_names_urls = df_OSDB_table[['card_title', 'card_title_href']]
    df_db_names_urls.columns = ['db_names', 'urls']

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
                "Project Type", "Written in", "Supported languages", "Embeds / Uses", "Licenses", "Operating Systems"]
    pd_select_col(use_cols, temp_save_path, OSDB_info_crawling_path)

    recalc_OSDB_info(path=OSDB_info_crawling_path)
