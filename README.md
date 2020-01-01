建表
---
一个表为brch_qry_dtl，8列

创建数据库函数
------------
CREATE OR REPLACE FUNCTION ceil_minute(TIMESTAMP WITH TIME ZONE, INTERVAL)
RETURNS TIMESTAMP WITH TIME ZONE AS $$
  SELECT date_trunc('hour', $1) + $2 * ceil(date_part('minute', $1) / (to_char($2, 'MI')::integer * 1.0))
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION将创建一个新函数或者替换一个现有的函数。

页面模板
-------
采用flask
export FLASK_APP=server.py
export FLASK_ENV=development
flask run