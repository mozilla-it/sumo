#!/usr/bin/env python3

import unittest
import unittest.mock as mock
import os, sys
from datetime import datetime, timedelta

PACKAGE_PARENT = "../../../"
SCRIPT_DIR = os.path.dirname(
    os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
)
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from GoogleAnalytics.get_ga_data import *


a_pst_time       = datetime.strptime("2020-04-09T13:13:42Z", '%Y-%m-%dT%H:%M:%SZ')
another_pst_time = datetime.strptime("2020-04-29T13:13:42Z", '%Y-%m-%dT%H:%M:%SZ')
a_utc_time_str   = "2020-04-09T20:13:42Z"

class TestSimpleThings(unittest.TestCase):
    def setUp(self):
        pass

    def test_daterange(self):
        r = daterange(a_pst_time, another_pst_time)
        day_num = 0
        for day in r:
            self.assertEqual(day, a_pst_time + timedelta(day_num), msg="daterange test")
            day_num += 1

    def test_get_dimension_filter_clauses_fenix(self):
        dim_filter_list = [
            {"filters":
                [
                {
                    "operator": "REGEXP",
                    "dimensionName": "DIMENSION_HERE",
                    "expressions": [".*/kb/.*firefox-preview.*"]
                },
                {
                    "operator": "PARTIAL",
                    "dimensionName": "DIMENSION_HERE",
                    "expressions": ["/kb/firefox-sync-troubleshooting-and-tips"]
                },
                {
                    "operator": "PARTIAL",
                    "dimensionName": "DIMENSION_HERE",
                    "expressions": ["/kb/send-usage-data-firefox-mobile-browsers"]
                },
                {
                    "operator": "PARTIAL",
                    "dimensionName": "DIMENSION_HERE",
                    "expressions": ["/kb/firefox-sync-troubleshooting-and-tips"]
                },
                ]
            }
        ]
        r = get_dimension_filter_clauses_fenix("DIMENSION_HERE")
        self.assertEqual(r, dim_filter_list, msg="get_dimension_filter_clauses_fenix test")

if __name__ == "__main__":
    # unittest.main(verbosity=5)
    unittest.main()
