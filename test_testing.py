"""
Simple unittest file to illustrate testing.
Named test_X.py because of the way that unittest will discover test files in
large projects
See https://docs.python.org/3/library/unittest.html for more
"""

import unittest
from testing import select_from_list

class TestSelect(unittest.TestCase):
    def test_returns_from_list(self):
        our_list = list(range(100))
        for i in range(100):
            elem = select_from_list(our_list)
            self.assertIn(elem, our_list)

    def test_is_num(self):
        our_list = [0]
        elem = select_from_list(our_list)
        self.assertEqual(elem, 0)

if __name__ == "__main__":
    unittest.main()
