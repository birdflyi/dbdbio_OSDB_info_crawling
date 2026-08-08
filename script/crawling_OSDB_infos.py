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
pkg_rootdir = os.path.dirname(cur_dir)
if pkg_rootdir not in sys.path:
    sys.path.append(pkg_rootdir)
    print('Add root directory "{}" to system path.'.format(pkg_rootdir))

import bs4
import re
import shutil
import time
import urllib
import urllib.parse
import urllib.request

import pandas as pd

from bs4 import BeautifulSoup
from urllib import request

from script.validate import ValidateFunc

STATE_OK = 0
KEY_ATTR_NAME = 'Name'

def process_delimeter(s):
    s = "" if s is None else str(s)
    # new line
    s = re.sub(r'\r?\n', ' ', s)
    s = re.sub(r'\r', ' ', s)

    # deduplicate
    s = re.sub(r'\s+', ' ', s)
    s = re.sub(r'\s+,\s*', ',', s)
    s = re.sub(r',+', ',', s)
    return s.strip(' ,')


def decode_email(encoded_email):
    e = ''
    r = int(encoded_email[0:2], 16) | 0
    n = 2
    while len(encoded_email) > n:
        e += '%' + "%0.2x" % (int(encoded_email[n:n + 2], 16) ^ r)
        n += 2

    return urllib.parse.unquote(e, encoding='utf-8')


EMPTY_VALUE_TEXTS = {"", "-", "--", "\u2014", "\u2013", "N/A", "n/a", "None", "none", "nan"}
INFO_ATTR_NAME_ALIASES_PATH = os.path.join(
    pkg_rootdir, "data", "existing_tagging_info", "dbdbio_info_attr_name_aliases.csv"
)
DEFAULT_INFO_ATTR_NAME_ALIASES = {
    "Blog": "Blog URL",
    "Coding Agents": "Coding Agent",
    "Developers": "Developer",
    "Embedded": "Embeds / Uses",
    "Former Names": "Former Name",
    "License": "Licenses",
    "Licenses": "Licenses",
    "Operating System": "Operating Systems",
    "Operating Systems": "Operating Systems",
    "Project Types": "Project Type",
    "Project Type": "Project Type",
    "Source Code": "Source Code",
    "Source Repo URL": "Source Code",
    "Supported Languages": "Supported Languages",
    "Supported languages": "Supported Languages",
    "Written in": "Programming Language",
    "Documentation": "Documentation",
    "Tech Docs": "Documentation",
    "Website": "Website URL",
    "Wikipedia": "Wikipedia URL",
}
INFO_COMPAT_COLUMN_PAIRS = [
    ("Website URL", "Website"),
    ("Documentation", "Tech Docs"),
    ("Programming Language", "Written in"),
    ("Supported Languages", "Supported languages"),
    ("Wikipedia URL", "Wikipedia"),
]
CONTROLLED_SECTION_ATTR_NAMES = {
    "Acquired By",
    "Checkpoints",
    "Compatible With",
    "Compression",
    "Concurrency Control",
    "Data Model",
    "Derived From",
    "Embeds / Uses",
    "Foreign Keys",
    "Indexes",
    "Inspired By",
    "Isolation Levels",
    "Joins",
    "Logging",
    "Parallel Execution",
    "Query Compilation",
    "Query Execution",
    "Query Interface",
    "Storage Architecture",
    "Storage Format",
    "Storage Model",
    "Storage Organization",
    "Stored Procedures",
    "System Architecture",
    "Views",
}


def load_info_attr_name_aliases(mapping_table_path=None, encoding="utf-8"):
    mapping_table_path = mapping_table_path or INFO_ATTR_NAME_ALIASES_PATH
    aliases = dict(DEFAULT_INFO_ATTR_NAME_ALIASES)
    try:
        df_aliases = pd.read_csv(mapping_table_path, encoding=encoding, index_col=False)
    except FileNotFoundError:
        return aliases

    for _, row in df_aliases.iterrows():
        attr_name = process_delimeter(row.get("attr_name", ""))
        canonical_attr_name = process_delimeter(row.get("canonical_attr_name", ""))
        if attr_name and canonical_attr_name:
            aliases[attr_name] = canonical_attr_name
    return aliases


INFO_ATTR_NAME_ALIASES = load_info_attr_name_aliases()
INFO_ATTR_CANONICAL_NAMES = set(INFO_ATTR_NAME_ALIASES.values())


def normalize_info_attr_name(s):
    s = process_delimeter(s)
    s = re.sub(r'(\s*\[\d+\])+', '', s).strip()
    if s in INFO_ATTR_NAME_ALIASES:
        return INFO_ATTR_NAME_ALIASES[s]
    if s in INFO_ATTR_CANONICAL_NAMES:
        return s
    return s


def add_info_compat_attrs(attrs_dict):
    for canonical_col, legacy_col in INFO_COMPAT_COLUMN_PAIRS:
        canonical_value = attrs_dict.get(canonical_col)
        legacy_value = attrs_dict.get(legacy_col)
        if (legacy_col not in attrs_dict or is_empty_info_value(legacy_value)) and not is_empty_info_value(canonical_value):
            attrs_dict[legacy_col] = canonical_value
        if (canonical_col not in attrs_dict or is_empty_info_value(canonical_value)) and not is_empty_info_value(legacy_value):
            attrs_dict[canonical_col] = legacy_value
    return attrs_dict


def ensure_info_compat_columns(df):
    df = pd.DataFrame(df).copy()
    for canonical_col, legacy_col in INFO_COMPAT_COLUMN_PAIRS:
        if canonical_col in df.columns and legacy_col not in df.columns:
            df[legacy_col] = df[canonical_col]
        if legacy_col in df.columns and canonical_col not in df.columns:
            df[canonical_col] = df[legacy_col]
    return df


def is_empty_info_value(s):
    return process_delimeter(s) in EMPTY_VALUE_TEXTS


def clean_info_element_text(elem, separator=' '):
    if elem is None:
        return ""
    elem_soup = BeautifulSoup(str(elem), 'lxml')
    for drop_elem in elem_soup.select(".cites, .citation, time, .text-muted, script, style"):
        drop_elem.decompose()
    return process_delimeter(elem_soup.get_text(separator, strip=True))


def extract_info_value(elem):
    if elem is None:
        return ""

    value_parts = []
    for link in elem.find_all("a"):
        link_classes = link.attrs.get("class", [])
        if "citation" in link_classes:
            continue
        link_text = clean_info_element_text(link)
        if link_text and not is_empty_info_value(link_text):
            value_parts.append(link_text)

    if value_parts:
        return process_delimeter(",".join(value_parts))

    value = clean_info_element_text(elem)
    return "" if is_empty_info_value(value) else value


def merge_info_attr_value(attrs_dict, key, value):
    value = process_delimeter(value)
    if not key or is_empty_info_value(value):
        return
    if key not in attrs_dict or pd.isna(attrs_dict[key]) or is_empty_info_value(attrs_dict[key]):
        attrs_dict[key] = value
        return
    old_value = process_delimeter(attrs_dict[key])
    if old_value == value:
        return

    merged_values = []
    for item in (old_value + "," + value).split(","):
        item = process_delimeter(item)
        if item and item not in merged_values:
            merged_values.append(item)
    attrs_dict[key] = ",".join(merged_values)


def parse_modern_dbms_info_soup(soup, preset_dict=None):
    entry_main = soup.select_one("main.page-system .entry-main") or soup.select_one(".entry-main")
    if not entry_main:
        return {}

    dbms_info_record_attrs_dict = {}
    dbms_info_record_attrs_dict.update(**(preset_dict or {}))

    title_elem = (
        entry_main.select_one(".d-none.d-md-block .page-title h1")
        or entry_main.select_one(".page-title h1")
        or soup.select_one("main.page-system h1")
        or soup.find("h1")
    )
    if title_elem:
        merge_info_attr_value(dbms_info_record_attrs_dict, "card_title", clean_info_element_text(title_elem))

    description_elems = (
        entry_main.select(".d-none.d-md-block .entry-lead-block")
        or entry_main.select(".entry-lead-block")
    )
    description = " ".join([clean_info_element_text(e) for e in description_elems if clean_info_element_text(e)])
    if description:
        merge_info_attr_value(dbms_info_record_attrs_dict, "Description", description)

    fact_elems = (
        entry_main.select(".entry-sidebar dl .fact")
        or entry_main.select("dl .fact")
        or soup.select("main.page-system dl .fact")
    )
    for fact_elem in fact_elems:
        dt_elem = fact_elem.find("dt")
        dd_elem = fact_elem.find("dd")
        key = normalize_info_attr_name(clean_info_element_text(dt_elem))
        value = extract_info_value(dd_elem)
        merge_info_attr_value(dbms_info_record_attrs_dict, key, value)

    for section_elem in entry_main.select(".entry-section"):
        title_elem = section_elem.find(["h2", "h3", "h4"])
        key = normalize_info_attr_name(clean_info_element_text(title_elem))
        if not key:
            continue

        badge_values = [clean_info_element_text(e) for e in section_elem.select(".badge-section")]
        badge_values = [e for e in badge_values if e and not is_empty_info_value(e)]
        if badge_values:
            value = ",".join(badge_values)
        else:
            link_values = [extract_info_value(e) for e in section_elem.find_all("a")]
            link_values = [e for e in link_values if e and not is_empty_info_value(e)]
            if link_values:
                value = ",".join(link_values)
            elif key in CONTROLLED_SECTION_ATTR_NAMES:
                continue
            else:
                section_copy = BeautifulSoup(str(section_elem), 'lxml')
                for drop_elem in section_copy.select("h2, h3, h4, hr, .cites, .citation, time, .text-muted"):
                    drop_elem.decompose()
                value = process_delimeter(section_copy.get_text(" ", strip=True))

        merge_info_attr_value(dbms_info_record_attrs_dict, key, value)

    return add_info_compat_attrs(dbms_info_record_attrs_dict)


def fetch_dbms_info_soup(url_init, header):
    request = urllib.request.Request(url_init, headers=header)
    response = urllib.request.urlopen(request, timeout=60*5)
    response_body = response.read().decode('utf-8').replace('&shy;', '')
    resolved_url = response.geturl()
    response.close()
    re_email = re.compile(r'<span class="__cf_email__" data-cfemail="([0-9a-f]+)">\[email[^<>]+protected\]</span>')
    response_body = re.sub(re_email, lambda s: decode_email(s[1]), response_body, re.I)
    soup = BeautifulSoup(response_body, 'lxml')
    return soup, resolved_url


def parse_legacy_dbms_info_soup(soup, use_elem_dict, preset_dict=None):
    # Parse the legacy card layout.
    main_contents = soup.find_all(use_elem_dict['main_contents'][0], attrs=use_elem_dict['main_contents'][1])
    db_info_table = main_contents[0]

    summary_col = db_info_table.find_all("div", {"class": "col-sm-12 col-md-7 order-2 order-md-1"})[0]
    feature_col = db_info_table.find_all("div", {"class": "col-sm-12 col-md-3 order-1 order-md-2"})[0]

    # parse summary_col
    dbms_info_record_attrs_dict = {}
    dbms_info_record_attrs_dict.update(**(preset_dict or {}))
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
            card_title = normalize_info_attr_name(card_title)
            card_text = process_delimeter(card_text)
            merge_info_attr_value(dbms_info_record_attrs_dict, card_title, card_text)

    # parse feature_col
    feature_div_bodys = feature_col.find("div", {"class": "card has-citations"}).find("div", {"class": "card-body"})
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
                card_title = normalize_info_attr_name(card_title)
                card_titles.append(card_title)

            elif feature_div_body.name == 'p':
                temp_value_part_strs.append(feature_div_body.get_text(',', '<br/>').strip())
            else:
                pass

    if temp_value_part_strs:
        temp_value_full_strs = ' '.join(temp_value_part_strs).strip()
        temp_value_full_strs = process_delimeter(temp_value_full_strs)
        card_texts.append(temp_value_full_strs)
        temp_value_part_strs = []
    assert(len(card_titles) == len(card_texts))
    for card_title, card_text in zip(card_titles, card_texts):
        merge_info_attr_value(dbms_info_record_attrs_dict, card_title, card_text)
    return add_info_compat_attrs(dbms_info_record_attrs_dict)


def crawling_dbms_info_soup(url_init, header, use_elem_dict, **kwargs):
    preset_dict = kwargs.get("preset_dict", {})
    soup, _ = fetch_dbms_info_soup(url_init, header)
    dbms_info_record_attrs_dict = parse_modern_dbms_info_soup(soup, preset_dict=preset_dict)
    if dbms_info_record_attrs_dict:
        return dbms_info_record_attrs_dict
    return parse_legacy_dbms_info_soup(soup, use_elem_dict, preset_dict=preset_dict)


def inspect_dbms_info_soup(url_init, header, use_elem_dict=None, **kwargs):
    preset_dict = kwargs.get("preset_dict", {})
    try:
        soup, resolved_url = fetch_dbms_info_soup(url_init, header)
    except BaseException as e:
        return {
            "ok": False,
            "url": url_init,
            "resolved_url": "",
            "attrs": {},
            "error": f"{type(e).__name__}: {e}",
        }

    dbms_info_record_attrs_dict = parse_modern_dbms_info_soup(soup, preset_dict=preset_dict)
    if not dbms_info_record_attrs_dict and use_elem_dict:
        dbms_info_record_attrs_dict = parse_legacy_dbms_info_soup(soup, use_elem_dict, preset_dict=preset_dict)

    return {
        "ok": True,
        "url": url_init,
        "resolved_url": resolved_url,
        "attrs": dbms_info_record_attrs_dict,
        "error": "",
    }

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

    default_use_cols = [KEY_ATTR_NAME, "card_title", "Description", "Data Model", "Query Interface", "System Architecture",
                        "Website URL", "Website", "Source Code", "Documentation", "Tech Docs", "Developer",
                        "Country of Origin", "Start Year", "End Year", "Project Type", "Programming Language",
                        "Written in", "Supported Languages", "Supported languages", "Embeds / Uses", "Licenses",
                        "Operating Systems", "Blog URL", "Twitter", "Wikipedia URL", "Wikipedia", "Coding Agent", "Tags",
                        "Crawl Error",
                        "Compression", "Storage Architecture", "Storage Model",
                        "Checkpoints", "Concurrency Control", "Foreign Keys", "Indexes", "Isolation Levels", "Joins",
                        "Logging", "Query Compilation", "Query Execution", "Stored Procedures", "Views", "Derived From",
                        "Embedded", "Storage Organization", "Inspired By", "Parallel Execution", "Storage Format",
                        "Acquired By", "Compatible With", "Former Name", "Governance", "Hosted Systems"]
    if use_all_impl_cols:
        use_cols = default_use_cols
        if use_cols:
            print("Warning: use_all_impl_cols=True will disable the parameter use_cols!")
    else:
        use_cols = use_cols or default_use_cols

    if KEY_ATTR_NAME not in use_cols:
        use_cols = [KEY_ATTR_NAME] + use_cols

    df_dbms_infos = pd.DataFrame(columns=use_cols)
    df_dbms_infos = df_dbms_infos.T

    len_db_names = len(df_db_names_urls)

    df1 = pd.DataFrame(columns=use_cols)
    default_batch = 20
    batch = kwargs.get('batch', default_batch)
    idx_start_end = [0, len_db_names]

    if ADD_MODE:
        encoding = kwargs.get('encoding', 'utf-8')
        index_col = kwargs.get('index_col', False)
        try:
            df1 = pd.read_csv(temp_save_path, encoding=encoding, index_col=index_col)
            df1 = df1[use_cols]
        except FileNotFoundError:
            df1.to_csv(temp_save_path, encoding='utf-8', index=False)
        except BaseException:
            pass

        len_df1 = len(df1)
        idx_start_end = kwargs.get('idx_start_end', [len_df1, min(len_df1 + batch, len_db_names)])

        if not (len_df1 == idx_start_end[0] and idx_start_end[0] <= idx_start_end[1]):
            raise ValueError(f"Wrong settings: len_df1 = {len_df1} should be in {idx_start_end}!")

    order_id_start_end = [idx_start_end[0] + 1, idx_start_end[1]]
    print('order_id_start_end:', order_id_start_end)
    for i in list(range(len_db_names))[idx_start_end[0]: idx_start_end[1]]:
        db_name_card_title, url = df_db_names_urls.iloc[i]
        db_name_urn = str(url).split('/')[-1]  # db_name_card_title may be duplicated! use db_name splited from url instead.
        print(f"{i + 1}/{len_db_names}: Crawling data for {db_name_card_title} on {url} ...")
        header = headers[i % len(headers)]
        try:
            dbms_info_record_attrs_dict = crawling_dbms_info_soup(
                url, header, use_elem_dict, preset_dict={KEY_ATTR_NAME: db_name_urn}
            )
        except BaseException as e:
            error_msg = f"{type(e).__name__}: {e}"
            print(f"Warning: failed to crawl data for {db_name_card_title} on {url}: {error_msg}")
            dbms_info_record_attrs_dict = {
                KEY_ATTR_NAME: db_name_urn,
                "card_title": db_name_card_title,
                "Crawl Error": error_msg,
            }
        if use_all_impl_cols:
            temp_use_cols = list(dbms_info_record_attrs_dict.keys())
            use_cols.extend(e for e in temp_use_cols if e not in use_cols)
        try:
            crawling_db_name = dbms_info_record_attrs_dict[KEY_ATTR_NAME]
        except (KeyError, ValueError):
            print("The website dbdb.io may have changed the key attribute of DBMS system properties table! Please"
                  "update KEY_ATTR_NAME!")
            return

        if db_name_urn == crawling_db_name:
            series_dbms_info = pd.Series(data=None, index=use_cols, dtype=str)
            series_dbms_info.update(pd.Series(dbms_info_record_attrs_dict))
            # series_dbms_info = series_dbms_info[use_cols]
            df_dbms_infos[db_name_urn] = series_dbms_info
        else:
            print(f"Unmatched dbms name, expect {db_name_urn} but get {crawling_db_name} please check the website: {url}")
        time.sleep(1)
        # break

    df_dbms_infos = df_dbms_infos.T
    # print(df_dbms_infos)

    if ADD_MODE:
        df2 = df_dbms_infos
        has_new_data = idx_start_end[1] < len_db_names or len(df2)
        if has_new_data:  # need breakpoint resumption
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
        else:  # The exit of recursive crawling
            print(f"Index >= {len_db_names}, the crawling tasks has done! idx_start_end:{idx_start_end}.")
            # save to csv
            shutil.copyfile(temp_save_path, save_path)
            print(save_path, 'saved!')
            return STATE_OK
    else:
        # save to csv
        df_dbms_infos.to_csv(save_path, encoding='utf-8', index=False)
        print(save_path, 'saved!')
    return STATE_OK


def pd_select_col(cols, src_path, tar_path, encoding="utf-8", index_col=False, **kwargs):
    df = pd.read_csv(src_path, encoding=encoding, index_col=index_col, **kwargs)
    df = ensure_info_compat_columns(df)
    for col in cols:
        if col not in df.columns:
            df[col] = pd.NA
    df[cols].to_csv(tar_path, encoding=encoding, index=index_col)
    return


def is_probable_prose_category_noise(item, full_value):
    item = process_delimeter(item)
    full_value = process_delimeter(full_value)
    return (
        len(full_value) > 120
        or "." in full_value
        or "(" in item
        or ")" in item
        or len(item.split()) > 4
    )


def clean_category_value_series(str_series, mapping_table_path=None, encoding="utf-8", index_col=False):
    mapping_table_path = mapping_table_path or os.path.join(
        pkg_rootdir, 'data/existing_tagging_info/category_labels_mapping_table.csv'
    )
    df_category_labels_mapping_table = pd.read_csv(mapping_table_path, encoding=encoding, index_col=index_col)
    valid_category_names = set(df_category_labels_mapping_table["category_name"].dropna().astype(str))
    dropped_noise_values = []
    strict_unknown_values = []

    def clean_one(value):
        if pd.isna(value):
            return value
        value = process_delimeter(value)
        if not value:
            return pd.NA

        kept_items = []
        for item in value.split(","):
            item = process_delimeter(item)
            if not item:
                continue
            if item in valid_category_names:
                kept_items.append(item)
            elif is_probable_prose_category_noise(item, value):
                dropped_noise_values.append(item)
            else:
                strict_unknown_values.append(item)

        if kept_items:
            return ",".join(kept_items)
        if value and all(is_probable_prose_category_noise(item, value) for item in value.split(",")):
            return pd.NA
        return value

    cleaned_series = pd.Series(str_series).apply(clean_one)
    if dropped_noise_values:
        dropped_noise_values = sorted(set(dropped_noise_values))
        print(f"Warning: dropped probable prose noise from category values: {dropped_noise_values[:20]}")
    if strict_unknown_values:
        strict_unknown_values = sorted(set(strict_unknown_values))
        raise KeyError(
            f"Unknown category values need mapping maintenance: {strict_unknown_values}. "
            f"Check the category_name column in {mapping_table_path}!"
        )
    return cleaned_series


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
            raise KeyError(f"The key '{e}' must be in category_name_col: {list(category_name_col)}. "
                           f"Check the category_name column in {mapping_table_path}!")
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


def recalc_OSDB_info(path, encoding="utf-8", index_col=False, check_github_response=False):
    df_dbms_infos = pd.read_csv(path, encoding=encoding, index_col=index_col)
    df_dbms_infos = ensure_info_compat_columns(df_dbms_infos)
    for col in ["Data Model", "Website", "Source Code", "Project Type", "Licenses", "Start Year", "End Year"]:
        if col not in df_dbms_infos.columns:
            df_dbms_infos[col] = pd.NA
    if "Data Model" in df_dbms_infos.columns:
        df_dbms_infos["Data Model"] = clean_category_value_series(
            df_dbms_infos["Data Model"], encoding=encoding, index_col=index_col
        )
    to_int_str = lambda x: "" if pd.isna(x) else str(int(x))
    recalc_func_dict = {
        KEY_ATTR_NAME: {"validate_func": ValidateFunc.check_distinct},
        # Representing "Data Model" "Source Code" "Start Year" "End Year" columns.
        "Data_Model_mapping": {"apply_param_preprocess_func": validate_label_mapping_table, "apply_func": mapping_values2labels, "input_col": "Data Model"},
        "has_github_repo": {"apply_func": lambda x: "Y" if ValidateFunc.has_github_repo(
            x, check_response=check_github_response) else "",
                                        "input_col": ["Website", "Source Code"], "apply_param_preprocess_func": lambda x: {"axis": 1}},  # need to be labeled manually
        "github_repo_link": {"apply_func": lambda x: get_github_owner_repo(
            x, check_response=check_github_response), "input_col": ["Website", "Source Code"],
                             "apply_param_preprocess_func": lambda x: {"axis": 1}},  # need to be labeled manually
        "org_name": {"apply_func": lambda x: x.split("/")[0] if x else "", "input_col": "github_repo_link"},
        "repo_name": {"apply_func": lambda x: x.split("/")[1] if len(x.split("/")) > 1 else "", "input_col": "github_repo_link"},
        "open_source_license": {
            "apply_func": lambda x: "Y" if ValidateFunc.check_open_source_license(x, strict=False) else "",
            "input_col": ["Project Type", "Website", "Source Code", "Licenses"],
            "apply_param_preprocess_func": lambda x: {"axis": 1}},
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


def get_github_owner_repo(series, check_response=False):
    series = pd.Series(series)
    get_github_owner_repo_from_github_website = lambda x: '/'.join(
        ValidateFunc.normalize_github_repo_url(x).replace("https://github.com/", "").split("/")[:2]
    ) if (ValidateFunc.is_from_github(x) and
          ValidateFunc.github_repo_url_exists(x, check_response=check_response)) else ""
    col__website = "Website"
    col__source_code = "Source Code"
    has_github_repo_colnames = [col__website, col__source_code]
    defaut_use_col = col__source_code
    github_link_uri = get_github_owner_repo_from_github_website(series[defaut_use_col])
    if github_link_uri:
        return github_link_uri
    else:
        has_github_repo_colnames.remove(defaut_use_col)
        for c in has_github_repo_colnames:
            github_link_uri = get_github_owner_repo_from_github_website(series[c])
            if github_link_uri:
                return github_link_uri
    return github_link_uri


if __name__ == '__main__':
    month_yyyyMM = "202306"
    OSDB_crawling_path = os.path.join(pkg_rootdir, f'data/dbdbio_OSDB_list/OSDB_crawling_{month_yyyyMM}_raw.csv')
    OSDB_info_crawling_path = os.path.join(pkg_rootdir, f'data/dbdbio_OSDB_list/OSDB_info_crawling_{month_yyyyMM}_raw.csv')

    encoding = 'utf-8'
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
                "Project Type", "Written in", "Supported Languages", "Supported languages", "Embeds / Uses",
                "Licenses", "Operating Systems", "Crawl Error"]
    pd_select_col(use_cols, temp_save_path, OSDB_info_crawling_path)

    recalc_OSDB_info(path=OSDB_info_crawling_path)
