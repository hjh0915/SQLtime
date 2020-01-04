import pandas as pd              
import numpy as np
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Dict, List  

class PDDB(object):
    def __init__(self):
        self.df = pd.read_csv('./initdata/data.csv', names=[
            'tran_date', 'timestampl', 'acc', 'amt', 'dr_cr_flag', 'rpt_sum']) 

        self.df['timestamp2'] = self.df['timestampl'].map(
            lambda x: datetime.strptime(str(x), '%Y%m%d%H%M%S')
        )       
        self.df['interval'] = self.df['timestamp2'].dt.ceil('30T')


    def get_time_range_dtl(self, tran_date: str, dr_cr_flag: str) -> List[Dict]:
        '''时间间隔区间交易金额汇总'''
        
        x = datetime.strptime(tran_date, '%Y%m%d')
        d = x - timedelta(days=1)
        start = d.strftime('%Y-%m-%d') + ' 23:00:00'
        end = x.strftime('%Y-%m-%d') + ' 22:30:00'

        rng = pd.date_range(start, end, freq='30T')  
        ts = pd.Series(rng, index=np.arange(len(rng)))  # DatetimeIndex->Series
        ts = pd.DataFrame(ts, columns=['time'])   # Series->DataFrame

        tm = self.df[(self.df['tran_date']==x.strftime('%Y-%m-%d')) & (self.df['dr_cr_flag']==int(dr_cr_flag))][['amt', 'interval']].groupby('interval').sum()   
        tm = tm.reset_index()  # 重构索引列

        df2 = pd.merge(ts, tm, left_on=['time'], right_on=['interval'], how='left') 
        df2 = df2[['time', 'amt']].fillna(0)

        results = []
        for i, r in df2.iterrows():
            d = dict()
            d['time'] = r[0].to_pydatetime()
            d['amt'] = Decimal(str(round(r[1], 2)))
            results.append(d)
        
        return results  


    def get_time_range_dtl_apart(self, tran_date: str, dr_cr_flag: str) -> List[Dict]:
        '''时间间隔区间交易金额摘要分布汇总'''

        x = datetime.strptime(tran_date, '%Y%m%d')
        d = x - timedelta(days=1)
        start = d.strftime('%Y-%m-%d') + ' 23:00:00'
        end = x.strftime('%Y-%m-%d') + ' 22:30:00'

        rng = pd.date_range(start, end, freq='30T')  
        ts = pd.Series(rng, index=np.arange(len(rng)))  # DatetimeIndex->Series
        ts = pd.DataFrame(ts, columns=['time'])   # Series->DataFrame

        tm2 = self.df[(self.df['tran_date']==x.strftime('%Y-%m-%d')) & (self.df['dr_cr_flag']==int(dr_cr_flag))][['rpt_sum', 'amt', 'interval']].groupby(['interval', 'rpt_sum']).sum() 
        tm2 = tm2.reset_index()       
        df3 = pd.merge(ts, tm2, left_on=['time'], right_on=['interval'], how='left') 
        df3 = df3[['time', 'rpt_sum', 'amt']].fillna(0)

        results = []
        for i, r in df3.iterrows():
            d = dict()
            d['time'] = r[0].to_pydatetime()
            d['rpt_sum'] = r[1]
            d['amt'] = Decimal(str(round(r[2], 2)))
            results.append(d)
        
        return results 

    
    def get_details_amt(self, tran_date: str, dr_cr_flag: str) -> List[Dict]:
        """获取所有明细"""

        x = datetime.strptime(tran_date, '%Y%m%d')
        df4 = self.df[(self.df['tran_date']==x.strftime('%Y-%m-%d')) & (self.df['dr_cr_flag']==int(dr_cr_flag))][['amt', 'rpt_sum']].groupby('rpt_sum').sum()    
        df4 = df4.reset_index()  
        df4 = df4[['rpt_sum', 'amt']].sort_values(ascending=False, by=['amt'])

        results = []
        for i, r in df4.iterrows():
            d = dict()
            d['rpt_sum'] = r[0]
            d['amt'] = Decimal(str(round(r[1], 2)))
            results.append(d)
        
        return results

    def get_members_rank(self, tran_date: str, dr_cr_flag: str) -> List[Dict]:
        """获取所有人员的排名（前十）"""

        x = datetime.strptime(tran_date, '%Y%m%d')
        df5 = self.df[(self.df['tran_date']==x.strftime('%Y-%m-%d')) & (self.df['dr_cr_flag']==int(dr_cr_flag))][['acc', 'amt', 'rpt_sum']].groupby(['rpt_sum', 'acc']).sum()  
        df5 = df5.reset_index()     
        df5 = df5[['acc', 'rpt_sum', 'amt']].sort_values(ascending=False, by=['amt'])

        results = []
        for i, r in df5.iterrows():
            d = dict()
            d['acc'] = r[0]
            d['rpt_sum'] = r[1]
            d['amt'] = Decimal(str(round(r[2], 2)))
            results.append(d)
        
        return results