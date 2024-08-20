import os
import duckdb
from os import path
from pathlib import Path

import duckdb

dir="/home/pei/Project/benchmarks/imdb_job-postgres/skinnerdb_queries"
iteration=1


con = duckdb.connect("imdb.db", config={'threads': 12, })
dir_path = Path(dir).glob('*.sql')
for sql in dir_path:
    con.sql(str(sql))
