import db
from typing import List, Dict

def get_data(filename: str) -> List[str]:
    data = []
    f = open(filename, 'rt')
    for r in f.readlines()[2:-1]:
        x = r.strip('\n').split('|')
        data.append([y.strip() for y in x])

    f.close()

    return data

def convert_data(data: List[str], filename: str) -> None:
    """把获取的数据转换放入CSV文件类型中"""

    f = open(filename, 'wt')
    for x in data:
        y = ','.join(x)
        y += '\n'
        f.write(y)


def get_sum(values: list) -> list:
    """最后返回的列表是为：[每个时段的金额总和]"""

    results = []
    s = 0
    for x in values:
        s += float(x)
        results.append(s)
        
    return results

def merge_details(v1: List[Dict], v2:List[Dict]) -> List[Dict]:
    """
        获取每个时段的明细
        v1: get_time_range_dtl
        v2: get_time_range_dtl_apart
    """

    results = []
    for x in v1:
        d = dict()
        d['time'] = x['time']
        d['amt'] = x['amt']

        rec = []
        for y in v2:
            if y['time'] == x['time'] and y['rpt_sum'] is not None:
                rec.append({'rpt_sum': y['rpt_sum'], 'amt': float(y['amt'])})
        
        d['details'] = rec 
        results.append(d)

    return results


if __name__ == '__main__':
    data = get_data('initdata/data.txt')
    convert_data(data, 'initdata/data.csv')
