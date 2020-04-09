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

from Twitter.get_twitter_data import *


a_pst_time       = datetime.strptime("2020-04-09T13:13:42Z", '%Y-%m-%dT%H:%M:%SZ')
another_pst_time = datetime.strptime("2020-04-29T13:13:42Z", '%Y-%m-%dT%H:%M:%SZ')
a_utc_time_str   = "2020-04-09T20:13:42Z"

class TestSimpleThings(unittest.TestCase):
    def setUp(self):
        pass

    def test_get_tweet_data_row(self):
        row = mock.Mock()
        row.id_str = 12345
        row.created_at = 12345
        row.full_text = """A string
with a newline"""
        row.user.id = 12345
        row.in_reply_to_status_id_str = 12345
        r = get_tweet_data_row(row)
        self.assertEqual(r, [12345, 12345, 'A string\\nwith a newline', 12345, 12345,], msg="get_tweet_data_row test")


if __name__ == "__main__":
    # unittest.main(verbosity=5)
    unittest.main()
