#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.9

# @Time   : 2023/1/22 1:13
# @Author : 'Lou Zehua'
# @File   : crawling_OSDB_infos.py 

import os
import sys

cur_dir = os.path.dirname(os.path.realpath(__file__))
pkg_rootdir = os.path.dirname(cur_dir)  # os.path.dirname()向上一级，注意要对应工程root路径
if pkg_rootdir not in sys.path:
    sys.path.append(pkg_rootdir)
    print('Add root directory "{}" to system path.'.format(pkg_rootdir))


def crawling_OSDB_infos_soup(df_db_names_urls, headers, use_elem_dict, save_path, use_cols=None, use_all_impl_cols=True, **kwargs):
    pass  # TODO
    return 0
