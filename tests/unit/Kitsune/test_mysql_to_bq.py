#!/usr/bin/env python3

import unittest
import unittest.mock as mock
import os, sys
from datetime import datetime

PACKAGE_PARENT = "../../../"
SCRIPT_DIR = os.path.dirname(
    os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
)
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from Kitsune.mysql_to_bq import *


a_pst_time     = datetime.strptime("2020-04-09T13:13:42Z", '%Y-%m-%dT%H:%M:%SZ')
a_utc_time_str = "2020-04-09T20:13:42Z"

class TestSimpleThings(unittest.TestCase):
    def setUp(self):
        pass


    #@mock.patch("requests.post")
    def test_convert_pst_to_utc(self):
        # will this break during DST? Probably.
        r = convert_pst_to_utc(a_pst_time)
        self.assertEqual(r, a_utc_time_str, msg="convert_pst_to_utc test")

    def test_safe_cast(self):
        r = safe_cast(5, str)
        self.assertEqual(r, "5", msg="safe_cast int->str test")
        r = safe_cast("5", str)
        self.assertEqual(r, "5", msg="safe_cast str->str test")
        r = safe_cast("5", int)
        self.assertEqual(r, 5, msg="safe_cast str->int test")
        r = safe_cast("pickles", int)
        self.assertEqual(r, None, msg="safe_cast str->int test 2")
        r = safe_cast(None, int)
        self.assertEqual(r, None, msg="safe_cast None->int test")
        r = safe_cast([1,2,3,4], int)
        self.assertEqual(r, None, msg="safe_cast list->int test")
        r = safe_cast(5, list)
        self.assertEqual(r, None, msg="safe_cast int->list test")
        r = safe_cast(True, bool)
        self.assertEqual(r, True, msg="safe_cast bool->bool test")
        r = safe_cast(False, bool)
        self.assertEqual(r, False, msg="safe_cast bool->bool test 2")

    def test_format_kb_votes(self):
        r = format_kb_votes(["1", 2, 3, a_pst_time, "5"])
        self.assertEqual(r, [1, "2", "3", a_utc_time_str, 5], msg="format_kb_votes test")
        r = format_kb_votes([1, 2, 3, 4, 5])
        self.assertEqual(r, [1, "2", "3", "", 5], msg="format_kb_votes test 2")

    def test_format_forums_forum(self):
        r = format_forums_forum([1, 2, 3, 4, 5, 6, 7])
        self.assertEqual(r, [1, "2", "3", "4", 5, 6, 7], msg="format_forums_forum test")

    def test_format_forums_post(self):
        r = format_forums_post([1, 2, 3, 4, a_pst_time, a_pst_time, 7])
        self.assertEqual(r, [1, 2, "3", 4, a_utc_time_str, a_utc_time_str, 7], msg="format_forums_post test")
        bad_string = """Here is
a string with
newlines."""
        r = format_forums_post([1, 2, bad_string, 4, a_pst_time, a_pst_time, 7])
        self.assertEqual(r, [1, 2, 'Here is\\na string with\\nnewlines.', 4, a_utc_time_str, a_utc_time_str, 7], msg="format_forums_post test 2")

    def test_format_forums_thread(self):
        r = format_forums_thread([1, 2, 3, a_pst_time, 5, 6, 7, 8, 9])
        self.assertEqual(r, [1, "2", 3, a_utc_time_str, 5, 6, 7, True, True], msg="format_forums_thread test")
        r = format_forums_thread([1, 2, 3, 4, 5, 6, 7, '', False])
        self.assertEqual(r, [1, "2", 3, '', 5, 6, 7, False, False], msg="format_forums_thread test 2")

    def test_format_customercare_reply(self):
        r = format_customercare_reply([1, 2, 3, 4, 5, 6, a_pst_time, 8])
        self.assertEqual(r, [1, 2, "3", 4, "5", "6", a_utc_time_str, 8], msg="format_customercare_reply test")

    def test_format_customercare_tweet(self):
        r = format_customercare_tweet([1, 2, 3, a_pst_time, 5, 6])
        self.assertEqual(r, [1, "2", "3", a_utc_time_str, 5, True], msg="format_customercare_tweet test")

    # stopping here because this code is supposed to be replaced with the Kitsune API real soon now

if __name__ == "__main__":
    # unittest.main(verbosity=5)
    unittest.main()
