import sys
sys.path.append('.')

import pddb
import unittest

class TestPdata(unittest.TestCase):
    """Tester for the function patients_with_missing_values in
    treatment_functions.
    """

    def setUp(self):
        self.db = pddb.PDDB()

    def test_time_range_dtl(self):
        """测试时间间隔区间交易金额汇总"""

        tran_date = '20191127'
        dr_cr_flag = '2'
        x = self.db.get_time_range_dtl(tran_date, dr_cr_flag)
        for k in x:
            print(k)

    def test_time_range_dtl_apart(self):
        """测试时间间隔区间交易金额摘要分布汇总"""

        tran_date = '20191127'
        dr_cr_flag = '2'
        x = self.db.get_time_range_dtl_apart(tran_date, dr_cr_flag)
        for k in x:
            print(k)

    def test_details_amt(self):
        """获取所有明细"""

        tran_date = '20191127'
        dr_cr_flag = '2'
        x = self.db.get_details_amt(tran_date, dr_cr_flag)
        for k in x:
            print(k)

    def test_members_rank(self):
        """获取所有人员的排名(前十)"""

        tran_date = '20191127'
        dr_cr_flag = '2'
        x = self.db.get_members_rank(tran_date, dr_cr_flag)
        for k in x[:10]:
            print(k)

if __name__ == '__main__':
    unittest.main(exit=False)