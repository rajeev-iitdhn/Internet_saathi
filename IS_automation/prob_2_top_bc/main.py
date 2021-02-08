def topbc(): 
  import pandas as pd
  import numpy as np
  import os
  import matplotlib.pyplot as plt

  from sklearn.model_selection import cross_val_score
  from sklearn.linear_model import LinearRegression, LogisticRegression
  from sklearn.ensemble import RandomForestRegressor
  from sklearn.tree import DecisionTreeRegressor
  import xgboost as xgb


  from google.cloud import bigquery
  from google.oauth2 import service_account
  credentials = service_account.Credentials.from_service_account_file(
  'infra-service.json')
  project_id = 'infra-211714'
  client = bigquery.Client(credentials= credentials,project=project_id)





  original_data = client.query('''with
  ccf as (
  select  id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt, language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_bain` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_Busara` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt, language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_beneficiaryprofile` union
  all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_cisco` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt, language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_Feedback` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt, language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_googlebolo` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_hulplastic` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_internetsafety` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt, language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_kantar` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_multilink` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_plasticconsumption` union
  all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_redbus` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_undpharyana` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt, language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_undpkarnataka` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt, language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_Unicef` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_washprogramme`
  ),
  ccf_assess as (
  select  id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt, language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt, assessmentScore from
  `infra-211714.is_dashboard.clientformdatas_bain` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt, assessmentScore from
  `infra-211714.is_dashboard.clientformdatas_cisco` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt, language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt, assessmentScore from
  `infra-211714.is_dashboard.clientformdatas_googlebolo` union all

  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt,assessmentScore from
  `infra-211714.is_dashboard.clientformdatas_hulplastic` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt,assessmentScore from
  `infra-211714.is_dashboard.clientformdatas_multilink` union all

  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt,assessmentScore from
  `infra-211714.is_dashboard.clientformdatas_redbus` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt,assessmentScore from
  `infra-211714.is_dashboard.clientformdatas_undpharyana` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt, language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt,assessmentScore from
  `infra-211714.is_dashboard.clientformdatas_undpkarnataka`
  ),
  sp as( SELECT id, ARRAY_AGG( sp ORDER BY modifiedAt DESC LIMIT 1
  )[OFFSET(0)].* EXCEPT (id) FROM
  `infra-211714.is_dashboard.saathiprofiles` as sp  GROUP BY id),
  bc as( SELECT id, ARRAY_AGG( sp ORDER BY modifiedAt DESC LIMIT 1
  )[OFFSET(0)].* EXCEPT (id) FROM
  `infra-211714.is_dashboard.blockcoordinator`  as sp GROUP BY id),
  b as( SELECT id, ARRAY_AGG( sp ORDER BY modifiedAt DESC LIMIT 1
  )[OFFSET(0)].* EXCEPT (id) FROM `infra-211714.is_dashboard.blocks` as
  sp GROUP BY id),
  st as( SELECT id, ARRAY_AGG( sp ORDER BY modifiedAt DESC LIMIT 1
  )[OFFSET(0)].* EXCEPT (id) FROM `infra-211714.is_dashboard.states` as
  sp GROUP BY id),
  ds as( SELECT id, ARRAY_AGG( sp ORDER BY modifiedAt DESC LIMIT 1
  )[OFFSET(0)].* EXCEPT (id) FROM `infra-211714.is_dashboard.districts`
  as sp GROUP BY id),
  bcb as ( SELECT bcb.id, ARRAY_AGG( bcb ORDER BY bcb.modifiedAt DESC
  LIMIT 1 )[OFFSET(0)].* EXCEPT (id) FROM
  `infra-211714.is_dashboard.blockcoordinator_block` as bcb join bc on
  bc.id =bcb.id where bc.modifiedAt = bcb.modifiedAt GROUP BY bcb.id,
  bcb.blockId, bcb.createdAt, bcb.modifiedAt ),
  vl as( SELECT id, ARRAY_AGG( v ORDER BY modifiedAt DESC LIMIT 1
  )[OFFSET(0)].* EXCEPT (id) FROM `infra-211714.is_dashboard.villages`
  as v GROUP BY id),
  question as (select count(distinct  question) as count,formId from
  `infra-211714.is_dashboard.formquestions` group by formId  ),
  saathis as (select sp.id as SaathiId,vl.block as BlockId,sp.name as
  SaathiName,sp.deviceId from sp join vl on vl.id = sp.village),
  surveys as (select count(distinct id) as surveyCount,deviceId
  ,ccf_assess.formId,assessmentScore from ccf_assess 
  -- ,avg(ccf_assess.timeTaken) as avgtime,avg(ccf_assess.timeTaken)/question.count as perquestionAvg from
  -- ccf_assess left join question on question.formId = ccf_assess.formId 
  group by
  deviceId,formId,assessmentScore),


  ageMapping as(  select age,deviceId
  from
  (
  SELECT
  CASE WHEN ( (cast(age as int64)  >= 1 AND cast(age as int64) <=25)) THEN "<25"
  WHEN ( (cast(age as int64)  >= 26 AND cast(age as int64) <=30)) THEN "26-30"
  WHEN ( (cast(age as int64) >= 31 AND cast(age as int64) <=35)) THEN "31-35"
  WHEN ( (cast(age as int64) >= 36 AND cast(age as int64) <=40)) THEN "36-40"
  WHEN ( (cast(age as int64) >= 41 AND cast(age as int64) <=45)) THEN "41-45"
  WHEN ( (cast(age as int64) >= 51 AND cast(age as int64) <=50)) THEN "46-50"
  WHEN ( cast(age as int64) > 50) THEN "Above 50"
  ELSE "NA" END as age,deviceId
  FROM sp where age is not null and age != ""
  )
  group by age,deviceId),

  ageGrouping as (select count(*) as Count ,age,bc.id from bc join bcb
  on bcb.id = bc.id  join b on b.id = bcb.blockId
  join saathis on saathis.BlockId = bcb.blockId join ageMapping on
  ageMapping.deviceId = saathis.deviceId group by age,bc.id),
  forms as( SELECT formId, ARRAY_AGG( forms ORDER BY modifiedAt DESC
  LIMIT 1 )[OFFSET(0)].* EXCEPT (formid) FROM
  `infra-211714.is_dashboard.forms` as forms GROUP BY formId),

  surveySubmittedByDevices as (select count(distinct id) as
  Count,deviceId,ccf.formId,avg(ccf.timeTaken) as avgtime,avg(ccf.timeTaken)/question.count as perquestionAvg  from ccf
  left join question on question.formId = ccf.formId
  group by deviceId,formId,question.count),
  deviceNotCompletedTheirTarget as (select
  surveySubmittedByDevices.deviceId from surveySubmittedByDevices join
  forms on forms.formId = surveySubmittedByDevices.formId where
  forms.fillcount = surveySubmittedByDevices.Count    group by deviceId)


  -- select * from surveySubmittedByDevices where formId = 200
  select distinct
  id ,
  BlockCoordinatorName,
  StateName,
  (sum(NoOfSaathisbelow25)/SaathiCount)*100 as PercentOfSaathisbelow25 ,
  (sum(NoOfSaathis26_30)/SaathiCount)*100 as PercentOfSaathis26_30,
  (sum(NoOfSaathis31_35)/SaathiCount)*100 as PercentOfSaathis31_35,
  (sum(NoOfSaathis36_40)/SaathiCount)*100 as PercentOfSaathis36_40,
  (sum(NoOfSaathis41_45)/SaathiCount)*100 as PercentOfSaathis41_45,
  (sum(NoOfSaathis46_50)/SaathiCount)*100 as PercentOfSaathis46_50,
  (sum(NoOfSaathisAbove50)/SaathiCount)*100 as PercentOfSaathisAbove50,
  DistrictName,
  BlockCount,
  SaathiCount,
  VillageCount,
  AvgNoOfSurveys,
  ToNoOfForms,
  AvgVillageCount,
  AvgAssessmentScore,
  (FractionOfError/SaathiCount)*100 as FractionOfError
  from
  (
  select distinct bc.id ,bc.name as
  BlockCoordinatorName,string_agg(distinct st.name) as
  StateName,
  case when ageGrouping.age = "<25" then ageGrouping.Count end as
  NoOfSaathisbelow25,
  case when ageGrouping.age = "26-30" then ageGrouping.Count end as
  NoOfSaathis26_30,
  case when ageGrouping.age = "31-35" then ageGrouping.Count end as
  NoOfSaathis31_35,
  case when ageGrouping.age = "36-40" then ageGrouping.Count end as
  NoOfSaathis36_40,
  case when ageGrouping.age = "41-45" then ageGrouping.Count end as
  NoOfSaathis41_45,
  case when ageGrouping.age = "46-50" then ageGrouping.Count end as
  NoOfSaathis46_50,
  case when ageGrouping.age = "Above 50" then ageGrouping.Count end as
  NoOfSaathisAbove50,
  count(distinct deviceNotCompletedTheirTarget.deviceId) as FractionOfError,
  string_agg(distinct ds.name) as DistrictName,count(distinct
  b.id) as BlockCount,count(distinct saathis.SaathiId) as
  SaathiCount,count(distinct vl.id) as
  VillageCount,Round(sum(surveys.surveyCount)/count(distinct
  saathis.SaathiId),2) as AvgNoOfSurveys,count(distinct surveys.formId)
  as ToNoOfForms,Round(count(distinct vl.id)/count(distinct
  saathis.SaathiId),2) as
  AvgVillageCount,sum(surveys.assessmentScore)/count(distinct
  saathis.SaathiId) as AvgAssessmentScore
  from bc  join bcb on bcb.id = bc.id  join b on b.id = bcb.blockId
  join saathis on saathis.BlockId = bcb.blockId  join vl on vl.block =
  b.id join st on vl.state = st.id join ds on vl.district = ds.id left
  outer join surveys on surveys.deviceId = saathis.deviceId
  left join ageGrouping on ageGrouping.id = bc.id
  left join deviceNotCompletedTheirTarget on
  deviceNotCompletedTheirTarget.deviceId = saathis.deviceId
  group by
  bc.name,bc.id,ageGrouping.age,ageGrouping.Count) group by id ,
  BlockCoordinatorName,
  StateName,
  DistrictName,
  BlockCount,
  SaathiCount,
  VillageCount,
  AvgNoOfSurveys,
  ToNoOfForms,
  AvgVillageCount,
  AvgAssessmentScore,
  FractionOfError
  ''').result().to_dataframe()




  #This line gives me those features which are having more than 80% of data non null.
  non_sparse_features=list(original_data.isnull().sum()[original_data.isnull().sum().values<0.8*len(original_data)].index)

  original_data=original_data[non_sparse_features].interpolate().bfill().ffill()

  def cat_cont_divider(data):
    cat_col=[]
    cont_col=[]
    for col in data.columns:
      if len (data[str(col)].value_counts())<15:
        cat_col.append(str(col))
      else:
        cont_col.append(col)
    return cat_col, cont_col



  useful_cont=original_data[list(set(original_data.select_dtypes(include=["int64","float64"]).columns).difference(set(cat_cont_divider(original_data)[0])))]
  useful_cat=original_data[list(set(cat_cont_divider(original_data)[0]).difference(set(original_data.select_dtypes(include=["object"]).columns)))]
  useful_cat["FractionOfError"]=useful_cont.FractionOfError.values

  model_cont=useful_cont.corr()[abs(useful_cont.corr().FractionOfError)>0.2].T.columns
  model_cat=useful_cat.corr()[abs(useful_cat.corr().FractionOfError)>0.2].T.columns
  output_feature="FractionOfError"
  model_var=list(set(model_cont.append(model_cat)))
  model_var.remove(output_feature)

  x=original_data[model_var]
  y=original_data[output_feature]

  lm=LinearRegression()
  rfr=RandomForestRegressor(random_state=10)
  dtr=DecisionTreeRegressor(random_state=10)
  xgbr=xgb.XGBRegressor()

  score_lm=int(abs(cross_val_score(lm, x,y, cv=10, scoring='neg_mean_squared_error')).mean())
  score_rfr=int(abs(cross_val_score(rfr, x,y, cv=10, scoring='neg_mean_squared_error')).mean())
  score_dtr=int(abs(cross_val_score(dtr, x,y,cv=10, scoring='neg_mean_squared_error')).mean())
  score_xgb=int(abs(cross_val_score(xgbr, x,y,cv=10, scoring='neg_mean_squared_error')).mean())
  scores=[score_lm, score_rfr, score_dtr, score_xgb]

  model_name=["lm","rfr","dtr","xgbr"]
  df=pd.DataFrame()
  df["mdl_name"]=model_name
  df["accuracy"]=scores
  best_mdl=df.loc[df.accuracy.nsmallest(1).index,].mdl_name.values[0]
  best_mdl

  mdl=dict({"rfr":RandomForestRegressor(random_state=10), 
            "lm":LinearRegression(), 
            "dtr":DecisionTreeRegressor(random_state=10),
            "xgb":xgb.XGBRegressor()})

  demo=["id","BlockCoordinatorName","DistrictName","StateName"]

  final_prediction=mdl[best_mdl].fit(x,y).predict(original_data[model_var])
  bc_with_fraction_of_error=original_data[demo]
  bc_with_fraction_of_error["perf_score"]=100-pd.Series(final_prediction).apply(lambda x:round(x, 3))
  topbc=bc_with_fraction_of_error.sort_values("perf_score",ascending=False)

  print(list(topbc.head().T.to_dict().values()))

topbc()
