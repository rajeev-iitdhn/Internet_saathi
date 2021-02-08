import pandas as pd
import numpy as np

from google.cloud import bigquery
from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_file(
    'infra-service.json')
project_id = 'infra-211714'
client = bigquery.Client(credentials= credentials,project=project_id)

def outliers(did, table_name, formid):
  data= client.query('''select formId, deviceId, cast(timeTaken as int64)/1000 as time from `infra-211714.is_dashboard.{tname}` where formId={fid}'''.format(tname=table_name,fid=formid)).result().to_dataframe()
  a=np.quantile(data.time, [0.25,0.75])
  iqr=a[1]-a[0]
  uc=a[1]+1.5*iqr
  lc=a[0]-1.5*iqr
  #The conditional function which will create the requisite column......
  def out(x):
    if x>uc:
      y=1
    elif x<lc:
      y=1
    else:
      y=0
    return y
  data["outliers"]=data.time.apply(lambda x:out(x))
  sathi_data=data[data.deviceId=="{dev_id}".format(dev_id=did)].outliers.values.sum()
  total_len=len(data[data.deviceId=="{dev_id}".format(dev_id=did)])
  if total_len>0:
    quality=int(np.ceil(100-(sathi_data*100/total_len)))
  else:
    quality=0
  return quality

print(outliers("5af483a1fa7d750dbcc106c9", "clientformdatas_tataamc", 70))

