def automation_biases():
  import pandas as pd
  import numpy as np
  from google.cloud import bigquery
  import pandas_gbq
  import pydata_google_auth
  import os
  # pip install --upgrade google-cloud-bigquery

  from google.cloud import bigquery
  from google.oauth2 import service_account
  credentials = service_account.Credentials.from_service_account_file(
      '/content/drive/My Drive/ML_works/Internet_sathi/IS_ML_PROBLEMS/infra-211714-fa79bf90e271.json')
  project_id = 'infra-211714'
  client = bigquery.Client(credentials= credentials,project=project_id)

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

  all_programs= client.query('''SELECT distinct * EXCEPT(is_typed)
  FROM
  `infra-211714.is_dashboard.INFORMATION_SCHEMA.TABLES` where not REGEXP_CONTAINS(table_name, r'clientformdatas_answer') and REGEXP_CONTAINS(table_name, r'clientformdatas_') order by creation_time
    ''').result().to_dataframe()
  programs=all_programs.table_name

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



  def biased_questions(df):
    a=[]
    b=[]
    c=[]
    d=[]
    e=[]
    f=[]
    g=[]
    h=[]
    for i in range(len(df)):
      a.append(df.index[i])
      b.append(df.iloc[i,].max())
      c.append(original_data[original_data.Title==df.index[i]].AnswerLabel.value_counts().sort_values(ascending=False).index[0])
      d.append(list(original_data[original_data.Title==df.index[i]].AnswerLabel.value_counts().sort_values(ascending=False).index[1:4]))
      e.append(int(df.iloc[i,].max()*100/df.iloc[i,].sum()))
      f.append(df.iloc[i,].sum())
      g.append(len(original_data[original_data.Title==df.index[i]].AnswerLabel.unique()))
      h.append((int(df.iloc[i,].max()*100/df.iloc[i,].sum()) -(100/(len(original_data[original_data.Title==df.index[i]].AnswerLabel.unique()))))
      *(len(original_data[original_data.Title==df.index[i]].AnswerLabel.unique())-1))
    output=pd.DataFrame()
    output["QuestionsWithBiasedAnswers"]=pd.Series(a)
    output["MostFrequentAnswer(MFA)"]=pd.Series(c)
    output["Count_of_MFA"]=pd.Series(b)
    output["Total_count"]=pd.Series(f)
    output["%of_MFA"]=pd.Series(e)
    output["CountOfDistinctAnswers"]=pd.Series(g)
    output["BiasParameter"]=pd.Series(h).apply(int)
    output["OtherTopAnswers"]=pd.Series(d)
    return output

  for pro in form_pro.index:
    original_data = client.query('''
        WITH   cfa AS(   SELECT     id,     ARRAY_AGG( fa     ORDER BY       modifiedAt DESC     LIMIT       1 )[   OFFSET     (0)].* EXCEPT (id)  
        FROM     `infra-211714.is_dashboard.{secondary}` AS fa   GROUP BY     id,     fa.question,     fa.answer,   
          fa.nested_question,     fa.nested_answer,     fa.parent_value),   cf AS(   SELECT     id,     ARRAY_AGG( fa     ORDER BY      
            modifiedAt DESC     LIMIT       1 )[   OFFSET     (0)].* EXCEPT (id)   FROM     `infra-211714.is_dashboard.{primary}` AS fa 
              GROUP BY     id),   fl AS (   SELECT     id,     ARRAY_AGG( fl     ORDER BY       modifiedAt DESC     LIMIT       1 )[   OFFSET     
              (0)].* EXCEPT (id)   FROM     `infra-211714.is_dashboard.form_label` AS fl   GROUP BY     id,     fl.ORDER,     fl.value,     fl.formId),
                fq AS (   SELECT     id,     ARRAY_AGG( fq     ORDER BY       modifiedAt DESC     LIMIT       1 )[   OFFSET     (0)].* EXCEPT (id) 
                  FROM     `infra-211714.is_dashboard.formquestions` AS fq   GROUP BY     id,     fq.formId,     fq.question,     fq.LANGUAGE,   
                    fq.title),   sf AS(   SELECT     id,     ARRAY_AGG( sf     ORDER BY       modifiedAt DESC     LIMIT       1 )[   OFFSET   
                      (0)].* EXCEPT (id)   FROM     `infra-211714.is_dashboard.saathi_form` AS sf   GROUP BY     id),   sfv AS(   SELECT     id,   
                        ARRAY_AGG( sfv     ORDER BY       modifiedAt DESC     LIMIT       1 )[   OFFSET     (0)].* EXCEPT (id)   FROM   
                          `infra-211714.is_dashboard.saathiformvillages` AS sfv   GROUP BY     id,     sfv.stateId),   st AS(   SELECT     id,   
                            ARRAY_AGG( state     ORDER BY       modifiedAt DESC     LIMIT       1 )[   OFFSET     (0)].* EXCEPT (id)   FROM   
                              `infra-211714.is_dashboard.states` AS state   GROUP BY     id),   dt AS(   SELECT     id,     ARRAY_AGG( district   
                                ORDER BY       modifiedAt DESC     LIMIT       1 )[   OFFSET     (0)].* EXCEPT (id)   FROM     
                                `infra-211714.is_dashboard.districts` AS district   GROUP BY     id),   p AS(   SELECT     id,   
                                  ARRAY_AGG( pt     ORDER BY       modifiedAt DESC     LIMIT       1 )[   OFFSET     (0)].* EXCEPT (id)   FROM  
                                      `infra-211714.is_dashboard.partner` AS pt   GROUP BY     id)


        SELECT
          fl.label AS AnswerLabel,
          fq.title AS Title,
          fl.ORDER AS QuestionOrder,
          fl.formId
        FROM
          cf
        JOIN
          cfa
        ON
          cf.id=cfa.id
        LEFT OUTER JOIN
          sf
        ON
          sf.deviceId = cf.deviceId
        JOIN
          sfv
        ON
          SFV.id =sf.id
        JOIN
          fq
        ON
          fq.question = cfa.question
        JOIN
          fl
        ON
          fl.ORDER = cfa.question
        LEFT OUTER JOIN
          st
        ON
          st.id = sfv.stateId
        LEFT OUTER JOIN
          dt
        ON
          dt.id = sfv.districtId
        LEFT OUTER JOIN
          p
        ON
          p.id = cf.partner
        WHERE
          fq.LANGUAGE='en'
          AND fq.formId= cf.formId
          AND fl.value = cfa.answer
          AND fl.formId = cf.formId
          AND sf.modifiedAT= sfv.modifiedAt
          AND cfa.modifiedAt = cf.modifiedAt
          AND sf.formId = cf.formId
        '''.format(primary=programs[pro],secondary=cfa[pro])).result().to_dataframe()
    forms=form_pro[form_pro.program==programs[pro]]["formlist"].iloc[0]
    writer=pd.ExcelWriter("{prog}_formId_wise_biased_questions.xlsx".format(prog=cfa[pro]), engine="xlsxwriter")
    pingu=[]
    for m in forms:
      df=pd.crosstab(original_data[original_data.formId==m].Title,original_data[original_data.formId==m].AnswerLabel)
      pingu.append(biased_questions(df).sort_values("BiasParameter",ascending=False).T.to_dict().values())
      # biased_questions(df).sort_values("BiasParameter",ascending=False).to_excel(writer, sheet_name="formId_{form}".format(form=m))
      # biased_questions(df).to_excel(writer, sheet_name="formId_{form}".format(form=m))
    # writer.save()
  return pingu








