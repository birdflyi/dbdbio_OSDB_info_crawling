#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.9

# @Time   : 2023/3/29 17:34
# @Author : 'Lou Zehua'
# @File   : validate.py
import itertools
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

import re

import pandas as pd

from collections import Iterable

class ValidateFunc:

    def __init__(self):
        pass

    @staticmethod
    def _not_empty(x):
        return any(x) if isinstance(x, Iterable) else pd.notna(x)

    @staticmethod
    def check_not_empty(x):
        if ValidateFunc._not_empty(x):
            flag = True
        else:
            flag = False
        return flag

    @staticmethod
    def check_distinct(x):
        return ValidateFunc.check_not_empty(x) and len(x) == len(set(x))

    @staticmethod
    def is_from_github(x):
        return False if pd.isna(x) else str(x).startswith("https://github.com/")

    @staticmethod
    def is_from_common_opensource_platforms(x):
        if pd.isna(x):
            flag = False
        else:
            common_opensource_platforms_site_prefix_dict = {
                "GitHub": "https://github.com/",
                "SourceForge": "https://sourceforge.net/",
                "BitBucket": "https://bitbucket.org/"
            }
            flags = [str(x).startswith(v) for k, v in common_opensource_platforms_site_prefix_dict.items()]
            flag = any(flags)
        return flag

    @staticmethod
    def _str_has_substr_in_check_list(s, check_list):
        if pd.isna(s):
            return False
        s = str(s)
        flag = False
        for e in check_list:
            osl_pattern = e + "\s*[vV]?\d*"
            if re.findall(osl_pattern, s):
                flag = True  # set flag=True if any pattern matched
        return flag

    @staticmethod
    def has_common_opensource_license(s, strict=False, single_elem_series_squeeze_out=True):
        # common open source licenses:
        # 1. OSI(Open Source Initiative) report: https://opensource.org/proliferation-report/
        # - Licenses that are popular and widely used or with strong communities (9):
        common_osl_fullname = [
            "Apache License, 2.0",  # Apache v2, Apache 2.0, Apache License
            "Apache License",  # Apache
            "3-Clause BSD License",  # BSD-3-Clause
            "New BSD license",  # BSD-3-Clause
            "Modified BSD License",  # BSD-3-Clause
            "Berkeley Software Distribution",  # BSD
            "GNU General Public License (GPL version 2)",  # GPL v2
            "GNU General Public License",  # GPL
            "GNU Library or “Lesser” General Public License (LGPL version 2)",  # LGPL v2
            "GNU Library General Public License",  # LGPL
            "GNU Lesser General Public License",  # LGPL
            "The MIT license",  # MIT
            "MIT license",  # MIT
            "Mozilla Public License 1.1 (MPL)",  # MPL 1.1
            "Mozilla Public License",  # MPL
            "Common Development and Distribution License",  # CDDL
            "Common Public License",  # CPL
            "Eclipse Public License",  # EPL
        ]
        common_osl_abbr = ["Apache", "BSD", "GPL", "LGPL", "MIT", "MPL", "CDDL", "CPL", "EPL"]
        # 2. choose a license website: https://choosealicense.com/licenses/
        common_osl_fullname += [
            "GNU Affero General Public License v3.0",  # GNU AGPLv3, AGPL v3
            "GNU Affero General Public License",  # AGPL
            "GNU General Public License v3.0",  # GNU GPLv3, GPL v3
            "GNU Lesser General Public License v3.0",  # GNU LGPLv3, LGPL v3
            "Mozilla Public License 2.0",  # MPL 2.0, MPL v2.0
            "Apache License 2.0",  # Apache 2.0, Apache v2.0, Apache v2, Apache License, Apache
            "MIT License",  # MIT
            "Boost Software License 1.0",  # BSL 1.0, BSL v1.0
            "Boost Software License",  # Boost
        ]
        common_osl_abbr += ["AGPL", "Boost"]
        # 3. other open source license: https://opensource.org/license/
        common_osl_fullname += [
            # "Business Source License",  # BSL  ## Source code is guaranteed to become Open Source at a certain point in time.
            "Mulan Permissive Software License，Version 2",  # Mulan PSL v2, MulanPSL-2.0
            "Mulan Permissive Software License",  # Mulan PSL, MulanPSL
            "Mulan Public License，Version 2",  # Mulan PubL v2,
            "Mulan Public License",  # Mulan PubL, Mulan
            "MulanOWL",
            "Public Domain",
            "Creative Commons License",  # CC
            "Creative Commons",  # CC
            "ISC License",  # ISC
            "OpenLDAP Public License",  # OLDAP
            "Zope Public License",  # ZPL
            "Python License",  # Python
            "PostgreSQL Licence",  # PostgreSQL
            "Open Software License 3.0 (OSL-3.0)",  # OSL-3.0
            "Open Software License",  # OSL
        ]
        common_osl_abbr += ["Mulan", "CC", "ISC", "ZPL", "Python", "PostgreSQL", "OSL"]

        common_osl_fullname = list(set(common_osl_fullname))
        common_osl_abbr = list(set(common_osl_abbr))
        check_list = common_osl_fullname if strict else common_osl_abbr

        if isinstance(s, Iterable):
            s = pd.Series(s)
            flag = s.apply(ValidateFunc._str_has_substr_in_check_list, check_list=check_list)
            if single_elem_series_squeeze_out and len(flag) == 1:
                flag = flag[0]
        else:
            s = str(s)
            flag = ValidateFunc._str_has_substr_in_check_list(s, check_list=check_list)
        return flag

    @staticmethod
    def check_open_source_license(series, strict=False):
        series = pd.Series(series)
        if not strict:
            col__project_tpye = "Project Type"
            flag = True if re.findall("open source", str(series[col__project_tpye]).lower()) else False
        else:
            flag_has_open_source_link = ValidateFunc.has_open_source_link(series)
            flag_common_open_source_licenses_valid = ValidateFunc.common_open_source_licenses_valid(series)
            flag = flag_has_open_source_link or flag_common_open_source_licenses_valid
        return flag

    @staticmethod
    def has_open_source_github_repo(series):
        flag = False
        series = pd.Series(series)
        col__website = "Website"
        col__source_code = "Source Code"
        has_open_source_github_repo_colnames = [col__website, col__source_code]
        temp_has_open_source_github_repo_colnames = []
        for c in has_open_source_github_repo_colnames:
            if c in list(series.index.values):
                temp_has_open_source_github_repo_colnames.append(c)
        if len(temp_has_open_source_github_repo_colnames):
            for c in temp_has_open_source_github_repo_colnames:
                if ValidateFunc.is_from_github(series[c]):
                    flag = True
                    break
        return flag

    @staticmethod
    def has_open_source_link(series):
        flag = False
        series = pd.Series(series)
        col__website = "Website"
        col__source_code = "Source Code"
        has_open_source_github_repo_colnames = [col__website, col__source_code]
        defaut_use_col = col__source_code
        if pd.notna(series[defaut_use_col]):
            flag = True
        else:
            has_open_source_github_repo_colnames.remove(defaut_use_col)
            for c in has_open_source_github_repo_colnames:
                if ValidateFunc.is_from_common_opensource_platforms(series[c]):
                    flag = True
                    break
        return flag

    @staticmethod
    def common_open_source_licenses_valid(series):
        flag = False
        series = pd.Series(series)
        col_licenses = "Licenses"
        common_open_source_licenses_valid_colnames = [col_licenses]
        temp_common_open_source_licenses_valid_colnames = []
        for c in common_open_source_licenses_valid_colnames:
            if c in series.index.values:
                temp_common_open_source_licenses_valid_colnames.append(c)
        if len(temp_common_open_source_licenses_valid_colnames):
            for c in temp_common_open_source_licenses_valid_colnames:
                if ValidateFunc.has_common_opensource_license(series[c]):
                    flag = True
                    break
        return flag
