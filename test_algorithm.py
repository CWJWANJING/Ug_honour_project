import unittest
from Algorithm import sampling, FPRAS


class TestSampling(unittest.TestCase):
    # check whether distinct_prims are distinct
        # is it in the original attributes
        # is there duplicates in distinct_prims

    # check whether all tables are properly created

    # check whether there's no rows not being selected when forming blocks
        # check the sum of all rows equals the previous table

    # check whether every row from each block has the same primary keys

class TestFPRAS(unittest.TestCase):
    # keywidth should be no larger than the number of attributes

    # epsilon should be greater than 0, and confidence(delta) should between 0 and 1

    def test_baobao(self):
        baobao = "themostwonderful"
        self.assertEqual(baobao, "themostwonderful")


if __name__ == "__main__":
    unittest.main()
