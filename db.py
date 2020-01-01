import psycopg2
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Dict, List

class DB(object):
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname="money", 
            user="hjh", 
            password="1234", 
            host="localhost", 
            port="5432"
        )
        self.cursor = self.conn.cursor()

    def close(self):
        self.conn.close()


    def write_data(self, data: str):
        for x in data:
            self.cursor.execute("""
                insert into brch_qry_dtl (
                    tran_date, timestampl, acc, 
                    amt, dr_cr_flag, rpt_sum) 
                    values (
                        %s, %s, %s, 
                        %s, %s, %s)
            """, (
                x[0], x[1], x[2], 
                Decimal(x[3]), int(x[4]), x[5]
            ))

        self.conn.commit()

    def get_time_range_dtl_apart(self, tran_date: str, dr_cr_flag: str) -> List[Dict]:
        '''时间间隔区间交易金额摘要分布汇总'''

        # datetime.strptime把str转换为date
        d = datetime.strptime(tran_date, '%Y%m%d') - timedelta(days=1)
        # datetime.strftime把date转换为str
        start = d.strftime('%Y%m%d') + '230000'
        end = tran_date[:8] + '223000'

        self.cursor.execute("""
            select t1.time, t2.rpt_sum, coalesce(t2.amt, 0) as amt
            from (
                select time from generate_series(to_timestamp(%s, 'yyyymmddhh24miss'), 
                to_timestamp(%s, 'yyyymmddhh24miss'), '30 min') as time
            ) t1

            left join (
                select p.gs, p.rpt_sum, sum(p.amt) as amt
                from (
                    select ceil_minute(to_timestamp(timestampl, 'yyyymmddhh24miss'), '30 minutes'
                ) as gs, rpt_sum, amt 
                from brch_qry_dtl 
                    where tran_date=%s
                    and dr_cr_flag=%s
                ) as p
                group by p.gs, p.rpt_sum
            ) t2
            on (t1.time=t2.gs)
            order by t1.time, amt desc
        """, (start, end, tran_date, int(dr_cr_flag))
        )

        results = []
        for row in self.cursor.fetchall():
            d = dict()
            d['time'] = row[0]
            d['rpt_sum'] = row[1]
            d['amt'] = row[2]
            results.append(d)
        
        return results


    def get_time_range_dtl(self, tran_date: str, dr_cr_flag: str) -> List[Dict]:
        '''时间间隔区间交易金额汇总'''

        d = datetime.strptime(tran_date, '%Y%m%d') - timedelta(days=1)
        start = d.strftime('%Y%m%d') + '230000'
        end = tran_date[:8] + '223000'

        self.cursor.execute("""
            select t1.time, coalesce(t2.amt, 0) as amt
            from (
                select time from generate_series(to_timestamp(%s, 'yyyymmddhh24miss'), 
                to_timestamp(%s, 'yyyymmddhh24miss'), '30 min') as time
            ) t1

            left join (
                select p.gs, sum(p.amt) as amt
                from (
                    select ceil_minute(to_timestamp(timestampl, 'yyyymmddhh24miss'), '30 minutes') as gs, 
                    timestampl, amt 
                from brch_qry_dtl 
                where tran_date=%s 
                    and dr_cr_flag=%s
                order by timestampl) as p
                group by p.gs) t2
            on (t1.time=t2.gs)
            order by t1.time
        """, (start, end, tran_date, int(dr_cr_flag)))

        results = []
        for row in self.cursor.fetchall():
            d = dict()
            d['time'] = row[0]
            d['amt'] = row[1]
            results.append(d)
        
        return results


    def get_details_amt(self, tran_date: str, dr_cr_flag: str) -> List[Dict]:
        """获取所有明细"""

        self.cursor.execute("""
            select rpt_sum, sum(amt) as amt_sum 
            from brch_qry_dtl 
            where tran_date=%s
                and dr_cr_flag=%s
            group by rpt_sum
            order by amt_sum desc
        """, (tran_date, int(dr_cr_flag)))

        results = []
        for row in self.cursor.fetchall():
            d = dict()
            d['rpt_sum'] = row[0]
            d['amt'] = row[1]
            results.append(d)
        
        return results

    
    def get_members_rank(self, tran_date: str, dr_cr_flag: str) -> List[Dict]:
        """获取所有人员的排名（前十）"""

        self.cursor.execute("""
            select acc, rpt_sum, sum(amt) as amt_sum
            from brch_qry_dtl
            where tran_date=%s
                and dr_cr_flag=%s
            group by rpt_sum, acc 
            order by amt_sum desc
        """, (tran_date, int(dr_cr_flag))
        )

        results = []
        for row in self.cursor.fetchall():
            d = dict()
            d['acc'] = row[0]
            d['rpt_sum'] = row[1]
            d['amt'] = row[2]
            results.append(d)

        return results