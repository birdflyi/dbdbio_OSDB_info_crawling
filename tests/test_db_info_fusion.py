import contextlib
import csv
import io
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from main import AUTO_KEEP_CURRENT_MONTH_URLS, CURRENT_MONTH_URL_COLUMNS
from script.db_info_fusion import merge_info_start_checkpoint_last_month_manulabeled


DELIMITER = "#start_checkpoint>|<last_month_manulabeled#"


class MergePolicyTests(unittest.TestCase):
    def merge(self, current, previous, **options):
        with tempfile.TemporaryDirectory() as folder:
            dest = Path(folder) / "merged.csv"
            with contextlib.redirect_stdout(io.StringIO()):
                merge_info_start_checkpoint_last_month_manulabeled(
                    pd.DataFrame(current), pd.DataFrame(previous), dest,
                    ["DBMS_urnform", "DBMS_urnform"], "DBMS_urnform",
                    conflict_delimiter=DELIMITER, **options)
            with dest.open(encoding="utf-8", newline="") as stream:
                return list(csv.DictReader(stream))

    def test_main_policy_keeps_current_urls_but_retains_manual_conflicts(self):
        current = {"DBMS_urnform": "hive", "Description": "Current description",
                   "Source Code": "https://github.com/current/repo", "Start Year": "2006"}
        previous = {"DBMS_urnform": "hive", "Description": "Manually annotated",
                    "Source Code": "https://github.com/curated/repo", "Start Year": "2005"}
        for column in CURRENT_MONTH_URL_COLUMNS:
            current[column] = "https://current.example/path"
            previous[column] = "https://previous.example/path/"
        self.assertTrue(AUTO_KEEP_CURRENT_MONTH_URLS)
        result = self.merge([current], [previous], prefer_current_columns=CURRENT_MONTH_URL_COLUMNS)[0]
        for column in CURRENT_MONTH_URL_COLUMNS:
            self.assertEqual(result[column], current[column], column)
        for column in ("Description", "Source Code", "Start Year"):
            self.assertEqual(result[column], current[column] + DELIMITER + previous[column], column)

    def test_disabled_policy_restores_original_conflict_markers(self):
        current = [{"DBMS_urnform": "hive", "Website": "https://hive.apache.org"}]
        previous = [{"DBMS_urnform": "hive", "Website": "https://hive.apache.org/"}]
        for columns in (None, (), []):
            with self.subTest(columns=columns):
                result = self.merge(current, previous, prefer_current_columns=columns)[0]
                self.assertEqual(result["Website"], current[0]["Website"] + DELIMITER + previous[0]["Website"])

    def test_missing_current_url_falls_back_and_records_match_by_key(self):
        current = [
            {"DBMS_urnform": "a", "Website": None},
            {"DBMS_urnform": "b", "Website": "https://new.example/b/"},
            {"DBMS_urnform": "new", "Website": "https://new.example/new"},
        ]
        previous = [
            {"DBMS_urnform": "b", "Website": "https://old.example/b"},
            {"DBMS_urnform": "a", "Website": "https://old.example/a"},
            {"DBMS_urnform": "removed", "Website": "https://old.example/removed"},
        ]
        result = self.merge(current, previous, prefer_current_columns=["Website"])
        self.assertEqual([r["DBMS_urnform"] for r in result], ["a", "b", "new"])
        self.assertEqual([r["Website"] for r in result],
                         ["https://old.example/a", "https://new.example/b/", "https://new.example/new"])

    def test_missing_previous_url_and_equal_values_do_not_create_conflicts(self):
        current = [{"DBMS_urnform": "a", "Website": "https://same.example/", "Documentation": "https://docs.example"}]
        previous = [{"DBMS_urnform": "a", "Website": "https://same.example/", "Documentation": None}]
        result = self.merge(current, previous, prefer_current_columns=CURRENT_MONTH_URL_COLUMNS)[0]
        self.assertEqual(result, current[0])

    def test_columns_present_in_only_one_month_are_preserved(self):
        current = [{"DBMS_urnform": "a", "Website URL": "https://new.example"}]
        previous = [{"DBMS_urnform": "a", "Wikipedia URL": "https://en.wikipedia.org/wiki/A"}]
        result = self.merge(current, previous, prefer_current_columns=CURRENT_MONTH_URL_COLUMNS)[0]
        self.assertEqual(result["Website URL"], "https://new.example")
        self.assertEqual(result["Wikipedia URL"], "https://en.wikipedia.org/wiki/A")


if __name__ == "__main__":
    unittest.main()
