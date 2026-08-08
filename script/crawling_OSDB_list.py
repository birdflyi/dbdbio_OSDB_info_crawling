#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.9

# @Time   : 2022/11/20 22:30
# @Author : 'Lou Zehua'
# @File   : crawling_ranking_table.py

import os
import re
import sys
import time
import urllib
import urllib.parse
import urllib.request

import pandas as pd

from bs4 import BeautifulSoup


cur_dir = os.path.dirname(os.path.realpath(__file__))
pkg_rootdir = os.path.dirname(cur_dir)
if pkg_rootdir not in sys.path:
    sys.path.append(pkg_rootdir)
    print('Add root directory "{}" to system path.'.format(pkg_rootdir))


DEFAULT_OSDB_BROWSE_FILTER = "project-type=open-source"
LEGACY_OSDB_BROWSE_FILTER = "type=open-source"
DB_COLUMN_NAMES = ["card_title", "card_title_href", "card_img_href", "card_text"]


def process_delimeter(s):
    s = re.sub(r'\r?\n', ' ', str(s))
    s = re.sub(r'\r', ' ', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip()


def make_abs_url(url, url_root=None):
    if not url:
        return ""
    if url_root is None:
        return url
    return urllib.parse.urljoin(url_root, url)


def normalize_osdb_browse_url(url_init):
    url_init = str(url_init)
    split_url = urllib.parse.urlsplit(url_init)
    query_params = urllib.parse.parse_qsl(split_url.query, keep_blank_values=True)
    if not query_params:
        return url_init

    normalized_params = []
    changed = False
    for key, value in query_params:
        if key == "type" and value == "open-source":
            normalized_key, normalized_value = DEFAULT_OSDB_BROWSE_FILTER.split("=", 1)
            normalized_params.append((normalized_key, normalized_value))
            changed = True
        else:
            normalized_params.append((key, value))

    if not changed:
        return url_init

    normalized_query = urllib.parse.urlencode(normalized_params)
    return urllib.parse.urlunsplit((
        split_url.scheme,
        split_url.netloc,
        split_url.path,
        normalized_query,
        split_url.fragment,
    ))


def _parse_legacy_card_rows(main_search, url_root=None):
    osdb_rendered_cards = main_search.find('div', attrs={"class": "card-columns"})
    if not osdb_rendered_cards:
        return []

    card_title_elems = osdb_rendered_cards.find_all('a', attrs={"class": "card-title"})
    osdb_records = []
    for card_title_elem in card_title_elems:
        card_title_href = make_abs_url(card_title_elem.attrs.get('href'), url_root)

        card_header = card_title_elem.find('div', attrs={"class": "card-header"})
        card_imgs = card_header.find_all('img') if card_header else []
        card_img_href = ""
        if card_imgs and "card-logo" in card_imgs[0].attrs.get("class", []):
            card_img_href = make_abs_url(card_imgs[0].attrs.get("src"), url_root)

        card_body = card_title_elem.find('div', attrs={"class": "card-body"})
        card_title = card_body.find('h5').text.strip() if card_body and card_body.find('h5') else ""
        card_text_elem = card_body.find('p', attrs={"class": "card-text"}) if card_body else None
        card_text = card_text_elem.text.strip() if card_text_elem else ""

        osdb_records.append([card_title, card_title_href, card_img_href, card_text])
    return osdb_records


def _get_results_table(soup, main_search):
    return (
        main_search.find("table", attrs={"id": "results-table"})
        or main_search.find("table", attrs={"class": "rtable"})
        or soup.find("table", attrs={"id": "results-table"})
        or soup.find("table", attrs={"class": "rtable"})
    )


def _extract_name_link_from_table_row(row):
    name_link = row.find("a", attrs={"class": "r-name"})
    if name_link:
        return name_link

    for link in row.find_all("a", href=True):
        if str(link["href"]).startswith("/db/"):
            return link
    return None


def _parse_browse_table_rows(soup, main_search, url_root=None):
    table = _get_results_table(soup, main_search)
    if not table:
        return []

    browse_rows = table.find_all("tr", attrs={"class": lambda x: x and "browse-row" in x})
    osdb_records = []
    for row in browse_rows:
        name_link = _extract_name_link_from_table_row(row)
        if name_link:
            card_title = process_delimeter(name_link.get_text(" ", strip=True))
            card_title_href = make_abs_url(name_link.get("href"), url_root)
        else:
            card_title = process_delimeter(row.get("data-name", ""))
            card_title_href = make_abs_url(row.get("data-href", ""), url_root)

        if not card_title or not card_title_href:
            continue

        logo_cell = row.find("td", attrs={"class": lambda x: x and "r-logo" in x})
        logo_img = logo_cell.find("img") if logo_cell else row.find("img")
        card_img_href = make_abs_url(logo_img.get("src"), url_root) if logo_img else ""

        # The new browse table no longer exposes the legacy "Last updated" text.
        osdb_records.append([card_title, card_title_href, card_img_href, ""])
    return osdb_records


def crawling_OSDB_list_soup(url_init, header, use_elem_dict, save_path, **kwargs):
    url_init = normalize_osdb_browse_url(url_init)
    request = urllib.request.Request(url_init, headers=header)
    response = urllib.request.urlopen(request)
    response_body = response.read().decode('utf-8', errors='replace')
    soup = BeautifulSoup(response_body, 'lxml')
    response.close()

    main_contents = soup.find_all(use_elem_dict['main_contents'][0], attrs=use_elem_dict['main_contents'][1])
    main_search = main_contents[0] if main_contents else soup

    url_root = kwargs.get("url_root")
    osdb_records = _parse_browse_table_rows(soup, main_search, url_root=url_root)
    if osdb_records:
        print(f"{len(osdb_records)} systems found in browse table.")
    else:
        results_info_elem = main_search.find('p', attrs={"class": "results-info"})
        results_info = results_info_elem.text.strip() if results_info_elem else ""
        if results_info:
            print(results_info)
        osdb_records = _parse_legacy_card_rows(main_search, url_root=url_root)
        if results_info and str(len(osdb_records)) not in results_info:
            print(f"Error: Wrong length of card body, get len(osdb_records) = {len(osdb_records)}, "
                  f"while it cannot be found in results_info: '{results_info}'!")
            return

    if not osdb_records:
        raise ValueError(f"No OSDB records were parsed from {url_init}. The dbdb.io browse layout may have changed.")

    df = pd.DataFrame(osdb_records, columns=DB_COLUMN_NAMES)
    df.drop_duplicates(subset=["card_title_href"], keep="last", inplace=True)

    df.to_csv(save_path, encoding='utf-8', index=False)
    print(save_path, 'saved!')
    return None


def _ensure_name_col(df):
    df = pd.DataFrame(df).copy()
    if "Name" not in df.columns:
        df["Name"] = df["card_title_href"].apply(lambda x: str(x).rstrip("/").split("/")[-1])
    return df


def _normalize_dbdb_url(url):
    if pd.isna(url):
        return ""
    split_url = urllib.parse.urlsplit(str(url).strip())
    path = split_url.path.rstrip("/")
    return urllib.parse.urlunsplit((split_url.scheme, split_url.netloc, path, "", ""))


def _has_open_source_project_type(value):
    return bool(re.findall(r"open\s+source", str(value), flags=re.I))


def rescue_missing_open_source_records(current_list_path, last_month_list_path, headers, **kwargs):
    from script.crawling_OSDB_infos import inspect_dbms_info_soup
    from script.validate import ValidateFunc

    encoding = kwargs.get("encoding", "utf-8")
    index_col = kwargs.get("index_col", False)
    sleep_seconds = kwargs.get("sleep_seconds", 1)
    audit_save_path = kwargs.get("audit_save_path")
    if audit_save_path is None:
        base_path, ext = os.path.splitext(current_list_path)
        audit_save_path = base_path + "_missing_audit" + (ext or ".csv")

    if not os.path.exists(last_month_list_path):
        print(f"Last month OSDB list not found, skip rescue: {last_month_list_path}")
        return None

    df_current = _ensure_name_col(pd.read_csv(current_list_path, encoding=encoding, index_col=index_col))
    df_last_month = _ensure_name_col(pd.read_csv(last_month_list_path, encoding=encoding, index_col=index_col))

    current_names = set(df_current["Name"].astype(str))
    current_urls = set(df_current["card_title_href"].apply(_normalize_dbdb_url))
    missing_df = df_last_month[~df_last_month["Name"].astype(str).isin(current_names)].copy()
    if not len(missing_df):
        print("No missing records compared with last month OSDB list.")
        return None

    if not isinstance(headers, (list, tuple)):
        headers = [headers]

    rescue_rows = []
    audit_records = []
    for order_id, (_, row) in enumerate(missing_df.iterrows(), start=1):
        db_name = str(row["Name"])
        url = row["card_title_href"]
        header = headers[(order_id - 1) % len(headers)]
        print(f"Checking missing record {order_id}/{len(missing_df)}: {db_name} on {url} ...")
        inspect_res = inspect_dbms_info_soup(url, header, preset_dict={"Name": db_name})
        attrs = inspect_res.get("attrs", {})
        project_type = attrs.get("Project Type", "")
        licenses = attrs.get("Licenses", "")
        project_type_has_open_source = _has_open_source_project_type(project_type)
        license_has_open_source = bool(ValidateFunc.has_common_opensource_license(licenses))
        resolved_url = inspect_res.get("resolved_url", "")
        resolved_url_in_current = _normalize_dbdb_url(resolved_url) in current_urls
        rescued = bool(inspect_res.get("ok") and not resolved_url_in_current and
                       (project_type_has_open_source or license_has_open_source))

        if rescued:
            rescued_row = row.copy()
            if attrs.get("card_title"):
                rescued_row["card_title"] = attrs["card_title"]
            rescue_rows.append(rescued_row)

        audit_records.append({
            "Name": db_name,
            "card_title_href": url,
            "resolved_url": resolved_url,
            "page_ok": inspect_res.get("ok"),
            "resolved_url_in_current": resolved_url_in_current,
            "Project Type": project_type,
            "Licenses": licenses,
            "project_type_has_open_source": project_type_has_open_source,
            "license_has_open_source": license_has_open_source,
            "rescued": rescued,
            "error": inspect_res.get("error", ""),
        })

        if sleep_seconds:
            time.sleep(sleep_seconds)

    df_audit = pd.DataFrame(audit_records)
    df_audit.to_csv(audit_save_path, encoding=encoding, index=False)
    print(audit_save_path, 'saved!')

    if rescue_rows:
        df_rescue = pd.DataFrame(rescue_rows)
        df_current = pd.concat([df_current, df_rescue], join="outer", ignore_index=True)
        df_current.drop_duplicates(subset=["Name"], keep="last", inplace=True)
        df_current.drop_duplicates(subset=["card_title_href"], keep="last", inplace=True)
        df_current = _ensure_name_col(df_current)
        df_current.to_csv(current_list_path, encoding=encoding, index=False)
        print(f"{len(rescue_rows)} missing open-source records rescued into {current_list_path}.")
    else:
        print("No missing open-source records need to be rescued.")
    return None


def recalc_OSDB_list(path, encoding="utf-8", index_col=False):
    df_OSDB_table = pd.read_csv(path, encoding=encoding, index_col=index_col)
    get_name_from_url = lambda x: str(x).split("/")[-1]
    recalc_func_dict = {
        "Name": {"apply_func": get_name_from_url, "input_col": "card_title_href"},
    }
    for recalc_k, recalc_v in recalc_func_dict.items():
        df_OSDB_table[recalc_k] = df_OSDB_table[recalc_v["input_col"]].apply(recalc_v["apply_func"])
    df_OSDB_table.to_csv(path, encoding='utf-8', index=False)
    print(path, 'recalculated!')
    return None


if __name__ == '__main__':
    dbdbio_url = "https://dbdb.io"
    dbdbio_OSDB_url = dbdbio_url + "/browse?" + DEFAULT_OSDB_BROWSE_FILTER
    url_init = dbdbio_OSDB_url
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.221 Safari/537.36 SE 2.X MetaSr 1.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
    }
    use_elem_dict = {
        'main_contents': ['form', {'id': 'mainsearch'}],
    }
    OSDB_crawling_path = os.path.join(pkg_rootdir, 'data/dbdbio_OSDB_list/OSDB_crawling_202301_raw.csv')
    # crawling_OSDB_list_soup(url_init, header, use_elem_dict, OSDB_crawling_path, url_root=dbdbio_url)

    recalc_OSDB_list(path=OSDB_crawling_path)
