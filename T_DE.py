import requests
import json
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
# import pyodbc
import vertica_python

# Vertica Connection
conn_info = {'host': "127.0.0.1",
             'port': 5433,
             'user': "dbadmin",
             'password': "tesla123",
             'database': "testdb"}

with vertica_python.connect(**conn_info) as connection:
  cur = connection.cursor()

  def run():
  #   #st_dt = '2017-01-01'
  #   #ed_dt = '2017-12-31'
  #   source_url = f'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={st_dt}&endtime={ed_dt}'

      for m in range(1, 13):
          st = datetime.strptime(f'2017-{m}-01', '%Y-%m-%d')
          ed = st + relativedelta(months=1) - timedelta(1)
          st_dt = st.strftime('%Y-%m-%d')
          ed_dt = ed.strftime('%Y-%m-%d')
          # print(m, st.strftime('%Y-%m-%d'), ed.strftime('%Y-%m-%d'))
          source_url = f'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={st_dt}&endtime={ed_dt}'
          req = requests.get(source_url)
            if (req.status_code == 200):
              opt = req.json()
              # print(m, st_dt, ed_dt, len(opt["features"]))
              eq = opt["features"]
              # print(eq[1])
              eq_ntype = pd.json_normalize(eq)
              # print(eq_ntype)
              # eq_ntype.to_csv(f'usgs_{m}.csv') -- We can use parallel COPY statements with error exceptions table
              """
                copy_cmd = /opt/vertica/bin/vsql -U <user> -d <db> --password='password' -h <host> 
                           -c "COPY schema.table(col1,col2) FROM LOCAL 'path/to/your.csv' PARSER 
                           fcsvparser(header='true') DIRECT REJECTMAX 1000 EXCEPTIONS 'path/to/exception.csv' 
                           REJECTED DATA 'path/to/rejected.csv'" 
                os.system(copy_cmd)
              """
              # count = 0
              for index,row in eq_ntype.iterrows():
                # if count < 10:  -- limiting row to see output format
                  # count=count+1
                  # print(row['id'], row['properties.mag'])
                  cur.execute("INSERT INTO usgs.earthquake(
                  id,
                  mag,
                  place,
                  time,
                  updated,
                  tz,
                  url,
                  detail,
                  felt,
                  cdi,
                  mmi,
                  alert,
                  status,
                  tsunami,
                  sig,
                  net,
                  code,
                  ids,
                  sources,
                  sources_types,
                  nst,
                  dmin,
                  rms,
                  gap,
                  magType,
                  event_type,
                  title,
                  geometry_type,
                  coordinates) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  (row['id'],
                   row['properties.mag'],
                   row['properties.place'],
                   pd.to_datetime(int(row['properties.time']), utc=True, unit='ms'),
                   pd.to_datetime(int(row['properties.updated']), utc=True, unit='ms'),
                   row['properties.tz'],
                   row['properties.url'],
                   row['properties.detail'],
                   row['properties.felt'],
                   row['properties.cdi'],
                   row['properties.mmi'],
                   row['properties.alert'],
                   row['properties.status'],
                   row['properties.tsunami'],
                   row['properties.sig'],
                   row['properties.net'],
                   row['properties.code'],
                   row['properties.ids'],
                   row['properties.sources'],
                   row['properties.types'],
                   row['properties.nst'],
                   row['properties.dmin'],
                   row['properties.rms'],
                   row['properties.gap'],
                   row['properties.magType'],
                   row['properties.type'],
                   row['properties.title'],
                   row['geometry.type'],
                   row['geometry.coordinates'])
                  )
            else:
                # print(r.text)
                print(m, req.status_code)

  connection.commit()
  connection.close()

  pass

if __name__ == "__main__":
    run()
