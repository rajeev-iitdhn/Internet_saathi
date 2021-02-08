# pip install --upgrade google-cloud-bigquery

# os.chdir("/content/drive/My Drive/ML_works/Internet_sathi/IS_ML_PROBLEMS/prob_0_program_wise_data_error_stats_and_recommendation/Data_anamolies/outliers_files")
def outlier_saathis():
  if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
      headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

      return ('', 204, headers)

  # Set CORS headers for the main request
  headers = {
        'Access-Control-Allow-Origin': '*'
    }
    
  import pandas as pd
  import numpy as np
  # import os

  from google.cloud import bigquery
  from google.oauth2 import service_account
  credentials = service_account.Credentials.from_service_account_file(
      '/content/drive/My Drive/ML_works/Internet_sathi/IS_ML_PROBLEMS/infra-211714-fa79bf90e271.json')
  project_id = 'infra-211714'
  client = bigquery.Client(credentials= credentials,project=project_id)

  def upper_outliers_for_formId(i):
    data=original_data[original_data.formId==i]
    a=np.quantile(data.time_minutes, [0.25,0.75])
    iqr=a[1]-a[0]
    uc=a[1]+1.5*iqr
    lc=a[0]-1.5*iqr
    #count=len(data[data.time_minutes>uc])+len(data[data.time_minutes<lc])
    #quartiles=pd.Series({"count_of_outliers":count,"inter_quartile_range":iqr,"upper_limit":uc,"lower_limit":lc})
    #col=data.columns
    #outlier_saathi=data[(data.time_minutes>uc)|(data.time_minutes<lc)].loc[:,[col[1],col[2],col[0],col[4],col[3],col[-3]]]
    outlier_saathi=data[data.time_minutes>uc]
    outlier_saathi.insert(3, "mean_time", data.time_minutes.mean())
    outlier_saathi.insert(4, "surplus_time", outlier_saathi.time_minutes-data.time_minutes.mean())
    outlier_saathi=outlier_saathi.round(2)
    # outlier_saathi=outlier_saathi.fillna(0)
    return outlier_saathi




  def lower_outliers_for_formId(i):
    data=original_data[original_data.formId==i]
    #col=data.columns
    outlier_saathi=data[data.time_minutes<0.3*data.time_minutes.mean()]
    outlier_saathi.insert(3, "mean_time", data.time_minutes.mean())
    outlier_saathi.insert(4,"deficit_time",outlier_saathi.time_minutes-data.time_minutes.mean())
    outlier_saathi=outlier_saathi.round(2)
    # outlier_saathi=outlier_saathi.fillna(0)
    return outlier_saathi



  #This program gives the list of names of all the tables in the infra-211714 project.
  all_programs= client.query('''SELECT distinct * EXCEPT(is_typed)
  FROM
  `infra-211714.is_dashboard.INFORMATION_SCHEMA.TABLES` where not REGEXP_CONTAINS(table_name, r'clientformdatas_answer') and REGEXP_CONTAINS(table_name, r'clientformdatas_') order by creation_time
    ''').result().to_dataframe()
  programs=all_programs.table_name

  #This program gives the list of names of all the tables in the infra-211714 project.
  cfa= client.query('''SELECT distinct * EXCEPT(is_typed)
  FROM
  `infra-211714.is_dashboard.INFORMATION_SCHEMA.TABLES` where REGEXP_CONTAINS(table_name, r'clientformdatas_answer') order by creation_time
    ''').result().to_dataframe()

  cfa = cfa.table_name
  temp = cfa[4]
  cfa[4] = cfa[5]
  cfa[5] = temp


  active_forms=client.query('''WITH  
  fg AS(   SELECT  id,     ARRAY_AGG( fa     ORDER BY       modifiedAt DESC     LIMIT       1 )[   OFFSET     (0)].* EXCEPT (id)   FROM     `infra-211714.is_dashboard.formgroups`  AS fa   GROUP BY id),
  fgf AS(   SELECT  id,formId,   ARRAY_AGG( fa     ORDER BY       modifiedAt DESC     LIMIT       1 )[   OFFSET     (0)].* EXCEPT (id,formId)   FROM     `infra-211714.is_dashboard.formgroups_form`   AS fa   GROUP BY id,formId),
  cf as (
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
  union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_tataamc` 
  union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_google200` 
  union all 
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_hulplastic`  
  )

  select distinct fgf.formId from fg join fgf on fgf.id = fg.id where fgf.formId in (   select distinct formId from cf where DATE(createdat) between cast(FORMAT_DATE("%Y-%m-%d", DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) as date) AND
    cast(FORMAT_DATE("%Y-%m-%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) as date)) ''').result().to_dataframe()
  active_forms=active_forms.formId



  form_pro=pd.DataFrame(columns=["program","formlist"])
  for index in range(0,len(programs)):
    # print(programs[index],cfa[index])
    forms= client.query('''with
    cf as( SELECT id, ARRAY_AGG( cfd ORDER BY modifiedAt DESC LIMIT 1 )[OFFSET(0)].* EXCEPT (id) FROM `infra-211714.is_dashboard.{program}`  
      as cfd  GROUP BY id),
    cfa as( SELECT fa.id, ARRAY_AGG( fa ORDER BY fa.modifiedAt DESC LIMIT 1 )[OFFSET(0)].* EXCEPT (id) FROM
    `infra-211714.is_dashboard.{secondary_table}` 
      as fa join cf on cf.id = fa.id where cf.modifiedAt = fa.modifiedAt GROUP BY id,fa.question ,fa.answer,fa.nested_question,fa.answer,fa.parent_value ),
    sp as( SELECT id, ARRAY_AGG( sp ORDER BY modifiedAt DESC LIMIT 1
    )[OFFSET(0)].* EXCEPT (id) FROM
    `infra-211714.is_dashboard.saathiprofiles` as sp  GROUP BY id)
    SELECT distinct a.formId as formId
      FROM cf as a inner join cfa  as b on a.id=b.id join sp on sp.deviceId = a.deviceId where a.modifiedAt = b.modifiedAt group by a.formId,a.deviceId,
      sp.name,sp.age, sp.phoneNumber,sp.isLiteracySaathi ,sp.isLivelyhoodSaathi
    '''.format(program=programs[index],secondary_table=cfa[index])).result().to_dataframe()

    form_pro=form_pro.append([{"program":programs[index],"index":index,"formlist":list(pd.Series(forms.formId).apply(int))}], ignore_index=True)
    form_pro["formlist"]=form_pro.formlist.apply(lambda x: list(set(x) & set(active_forms)))

  lis=[]
  for i in range(len(form_pro)):
    if len(form_pro.formlist[i])>0:
      lis.append(i)
  form_pro=form_pro.loc[lis,:]




  for pro in list(form_pro.index):
    original_data = client.query('''with
    cf as( SELECT id, ARRAY_AGG( cfd ORDER BY modifiedAt DESC LIMIT 1 )[OFFSET(0)].* EXCEPT (id) FROM `infra-211714.is_dashboard.{prog}`  
      as cfd  GROUP BY id),
    cfa as( SELECT fa.id, ARRAY_AGG( fa ORDER BY fa.modifiedAt DESC LIMIT 1 )[OFFSET(0)].* EXCEPT (id) FROM
    `infra-211714.is_dashboard.{secondary}` 
      as fa join cf on cf.id = fa.id where cf.modifiedAt = fa.modifiedAt GROUP BY id,fa.question ,fa.answer,fa.nested_question,fa.answer,fa.parent_value ),
    sp as( SELECT id, ARRAY_AGG( sp ORDER BY modifiedAt DESC LIMIT 1
    )[OFFSET(0)].* EXCEPT (id) FROM
    `infra-211714.is_dashboard.saathiprofiles` as sp  GROUP BY id),
    dv as( SELECT id, ARRAY_AGG( sp ORDER BY modifiedAt DESC LIMIT 1
    )[OFFSET(0)].* EXCEPT (id) FROM
    `infra-211714.is_dashboard.devices` as sp  GROUP BY id),
    v as( SELECT id, ARRAY_AGG( sp ORDER BY modifiedAt DESC LIMIT 1
    )[OFFSET(0)].* EXCEPT (id) FROM
    `infra-211714.is_dashboard.villages` as sp  GROUP BY id),
    bl as( SELECT id, ARRAY_AGG( sp ORDER BY modifiedAt DESC LIMIT 1
    )[OFFSET(0)].* EXCEPT (id) FROM
    `infra-211714.is_dashboard.blocks` as sp  GROUP BY id),
    d as( SELECT id, ARRAY_AGG( sp ORDER BY modifiedAt DESC LIMIT 1
    )[OFFSET(0)].* EXCEPT (id) FROM
    `infra-211714.is_dashboard.districts`  as sp  GROUP BY id),
    s as( SELECT id, ARRAY_AGG( sp ORDER BY modifiedAt DESC LIMIT 1
    )[OFFSET(0)].* EXCEPT (id) FROM
    `infra-211714.is_dashboard.states` as sp  GROUP BY id),
    form as( SELECT id, ARRAY_AGG( sp ORDER BY modifiedAt DESC LIMIT 1
    )[OFFSET(0)].* EXCEPT (id) FROM
    `infra-211714.is_dashboard.forms` as sp  GROUP BY id),
    lastSevenDays as (select distinct deviceId,count(distinct DATE(modifiedAt)) as daysactive,formId,count(*) as recordSubmitted from cf where DATE(modifiedat) between cast(FORMAT_DATE("%Y-%m-%d", DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) as date) AND
    cast(FORMAT_DATE("%Y-%m-%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) as date )group by deviceId,formID),
    lastFifteenDays as (select distinct deviceId,count(distinct DATE(modifiedAt)) as daysactive,formId,count(*) as recordSubmitted from cf where DATE(modifiedat) between cast(FORMAT_DATE("%Y-%m-%d", DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY)) as date) AND
    cast(FORMAT_DATE("%Y-%m-%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) as date )group by deviceId,formID),
    lastThirtyDays as (select distinct deviceId,count(distinct DATE(modifiedAt)) as daysactive,formId,count(*) as recordSubmitted from cf where DATE(modifiedat) between cast(FORMAT_DATE("%Y-%m-%d", DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY)) as date) AND
    cast(FORMAT_DATE("%Y-%m-%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) as date )group by deviceId,formID),
    thirtyDaysBefore as (select distinct deviceId,count(distinct DATE(modifiedAt)) as daysactive,formId,count(*) as recordSubmitted from cf where DATE(modifiedat) < cast(FORMAT_DATE("%Y-%m-%d", DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY)) as date)group by deviceId,formID)
    
  SELECT
    a.deviceId,
    a.formId,
    round((AVG(a.timeTaken)/1000)/60,2) AS time_minutes,
    COUNT(DISTINCT a.id) AS resptilltoday,
    form.fillCount as totalAssigned,
    lastSevenDays.daysactive as daysact_7,
    lastSevenDays.recordSubmitted as resp_7,
    lastFifteenDays.daysactive as daysact_15,
    lastFifteenDays.recordSubmitted as resp_15,
    lastThirtyDays.daysactive as daysact_31,
    lastThirtyDays.recordSubmitted as resp_31,
    thirtyDaysBefore.daysactive as daysactbef_31,
    thirtyDaysBefore.recordSubmitted as respbef_31,
    sp.name,
    sp.age,
    sp.phoneNumber AS contactNumber1,
    dv.mobilenumber AS contactNumber2,
    v.name as Village,
    bl.name as Block,
    d.name as District,
    s.name as State
  FROM
    cf AS a
  INNER JOIN
    cfa AS b
  ON
    a.id=b.id
  left JOIN
    sp
  ON
    sp.deviceId = a.deviceId
  left JOIN
    dv
  ON
    sp.deviceId = dv.id
  join form on form.formId = a.formId
  left join v on v.id = sp.village
  left join bl on v.block = bl.id
  left join d on v.district = d.id
  left join s on v.state = s.id
  left join lastSevenDays on lastSevenDays.deviceId = a.deviceID and a.formId = lastSevenDays.formId
  left join lastFifteenDays  on lastFifteenDays.deviceId = a.deviceID and a.formId = lastFifteenDays.formId
  left join lastThirtyDays  on lastThirtyDays.deviceId = a.deviceID and a.formId = lastThirtyDays.formId
  left join thirtyDaysBefore   on thirtyDaysBefore.deviceId = a.deviceID and a.formId = thirtyDaysBefore.formId


  WHERE
    a.modifiedAt = b.modifiedAt
  GROUP BY
    a.formId,
    a.deviceId,
    sp.name,
    sp.age,
    sp.phoneNumber,
    sp.isLiteracySaathi,
    sp.isLivelyhoodSaathi,
    dv.mobilenumber,
    s.name,
    d.name,
    bl.name,
    v.name ,
    form.fillCount,
    lastSevenDays.daysactive,
    lastSevenDays.recordSubmitted,
    lastFifteenDays.daysactive,
    lastFifteenDays.recordSubmitted,
    lastThirtyDays.daysactive,
    lastThirtyDays.recordSubmitted,
      thirtyDaysBefore.daysactive,
    thirtyDaysBefore.recordSubmitted
  ORDER BY
    a.deviceId
    '''.format(prog=programs[pro],secondary=cfa[pro])).result().to_dataframe()
    forms=form_pro[form_pro.program==programs[pro]]["formlist"].iloc[0]
    # writer=pd.ExcelWriter("{prog}_outliers_all_forms.xlsx".format(prog=programs[pro]), engine="xlsxwriter")
    for m in forms:
      print(upper_outliers_for_formId(m).sort_values("surplus_time", ascending=False).head().T.to_dict().values())
      print(lower_outliers_for_formId(m).sort_values("deficit_time", ascending=False).head().T.to_dict().values())
      # upper_outliers_for_formId(m).sort_values("surplus_time", ascending=False).to_excel(writer, sheet_name="{fid}_upper_outlier".format(fid=m))
      # lower_outliers_for_formId(m).sort_values("deficit_time", ascending=False).to_excel(writer, sheet_name="{fid}_lower_outlier".format(fid=m))
    # writer.save()

outlier_saathis