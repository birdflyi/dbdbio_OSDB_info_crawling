#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.9

# @Time   : 2022/11/20 22:30
# @Author : 'Lou Zehua'
# @File   : crawling_ranking_table.py

import os
import sys

cur_dir = os.path.dirname(os.path.realpath(__file__))
pkg_rootdir = os.path.dirname(cur_dir)  # os.path.dirname()向上一级，注意要对应工程root路径
if pkg_rootdir not in sys.path:
    sys.path.append(pkg_rootdir)
    print('Add root directory "{}" to system path.'.format(pkg_rootdir))


import urllib

import pandas as pd

from bs4 import BeautifulSoup
from urllib import request


def crawling_OSDB_list_soup(url_init, header, use_elem_dict, save_path, **kwargs):
    request = urllib.request.Request(url_init, headers=header)
    response = urllib.request.urlopen(request)
    # print(response.read().decode("utf-8"))
    soup = BeautifulSoup(response, 'lxml')  # 利用bs4库解析html
    # soup = BeautifulSoup(s, 'lxml')  # 利用bs4库解析html
    response.close()  # 注意关闭response

    # 取出主内容
    main_contents = soup.find_all(use_elem_dict['main_contents'][0], attrs=use_elem_dict['main_contents'][1])
    main_search = main_contents[0]

    # 获取所需文本
    # 1. get results_info
    results_info = main_search.find('p', attrs={"class": "results-info"}).text.strip()
    print(results_info)

    # 2. get osdb_list
    osdb_rendered_cards = main_search.find('div', attrs={"class": "card-columns"})
    url_root = kwargs.get("url_root")
    card_title_elems = osdb_rendered_cards.find_all('a', attrs={"class": "card-title"})

    len_card_title_elems = len(card_title_elems)
    if str(len_card_title_elems) not in results_info:
        print(f"Error: Wrong length of table body, get len_card_title_elems = {len_card_title_elems}, "
              f"while it cannot be found in results_info: '{results_info}'!")
        return

    osdb_records = []
    db_column_names = ["card_title", "card_title_href", "card_img_href", "card_text"]
    for card_title_elem in card_title_elems:
        card_title_href = card_title_elem.attrs['href'] if url_root is None else url_root + card_title_elem.attrs['href']

        card_header = card_title_elem.find('div', attrs={"class": "card-header"})
        card_img = card_header.find_all('img')[0]
        if "card-img-placeholder" in card_img.attrs["class"]:
            card_img_href = card_img.attrs["data-src"] if url_root is None else url_root + card_img.attrs["data-src"]
        else:
            card_img_href = ""

        card_body = card_title_elem.find('div', attrs={"class": "card-body"})
        card_title = card_body.find('h5').text.strip()
        card_text = card_body.find('p', attrs={"class": "card-text"}).text.strip()

        record_items = [card_title, card_title_href, card_img_href, card_text]
        osdb_records.append(record_items)

    # 4. save to csv
    df = pd.DataFrame(osdb_records, columns=db_column_names)
    df.to_csv(save_path, encoding='utf-8', index=False)
    print(save_path, 'saved!')
    return None


if __name__ == '__main__':
    # dbdb.io http link
    dbdbio_url = "https://dbdb.io"
    dbdbio_OSDB_url = dbdbio_url + "/browse?type=open-source"
    url_init = dbdbio_OSDB_url
    # headers info when use Chrome explorer
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.221 Safari/537.36 SE 2.X MetaSr 1.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
    use_elem_dict = {
        'main_contents': ['form', {'id': 'mainsearch'}],
    }
    OSDB_crawling_path = os.path.join(pkg_rootdir, 'data/dbdbio_OSDB_list/OSDB_crawling_202301.csv')
    crawling_OSDB_list_soup(url_init, header, use_elem_dict, OSDB_crawling_path, url_root=dbdbio_url)
