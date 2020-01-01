import sys
sys.path.append('.')

import db
import utils
import unittest

class TestData(unittest.TestCase):
    """Tester for the function patients_with_missing_values in
    treatment_functions.
    """

    def test_read_data(self):
        expected = 6593
        x = utils.get_data('./initdata/data.txt')
        actual = len(x)
        self.assertEqual(expected, actual)

    def test_sum(self):
        """最后返回的列表"""

        tran_date = '20191127'
        dr_cr_flag = '2'
        x = utils.get_time_range_dtl(tran_date, dr_cr_flag)
        y = utils.get_sum(x)
        for k in y:
            print(k)

    def test_details(self):
        """获取每个时段的明细"""

        q = db.DB()
        tran_date = '20191127'
        dr_cr_flag = '2'
        v1 = q.get_time_range_dtl(tran_date, dr_cr_flag)
        v2 = q.get_time_range_dtl_apart(tran_date, dr_cr_flag)

        x = utils.merge_details(v1, v2)
        for k in x[:2]:
            print(k)



if __name__ == '__main__':
    unittest.main(exit=False)