# pip install --upgrade google-cloud-bigquery

import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_file(
    'infra-service.json')
project_id = 'infra-211714'
client = bigquery.Client(credentials= credentials,project=project_id)

def normalise(a,b,c):
  x=min(a)
  y=max(a)
  if y-x!=0:
   norm_list=[round((((i-x)/(y-x))*(c-b)+b),2) for i in a]
   normalised=[100-i for i in norm_list]
  else:
   normalised=[0]
  return normalised

def bias_parameter_for_saathi(table_name, answer_table_name, form_id, device_id):

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
          fl.formId,
          cf.deviceId
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
          and cf.formId={input_fid}
          AND fq.formId= cf.formId
          AND fl.value = cfa.answer
          AND fl.formId = cf.formId
          AND sf.modifiedAT= sfv.modifiedAt
          AND cfa.modifiedAt = cf.modifiedAt
          AND sf.formId = cf.formId
          AND cf.deviceId="{input_did}"
        '''.format(primary=str(table_name),secondary=str(answer_table_name), input_fid=form_id, input_did=str(device_id))).result().to_dataframe()

  qns=list(original_data.Title.unique())
  bp_saathi=[]
  for m in qns:
    qns_data=original_data[original_data.Title==m]
    total_options=len(qns_data.AnswerLabel.value_counts())
    unbiased_percentage=100/total_options
    options=list(qns_data.AnswerLabel.value_counts().index)
    bias_param=[]
    for i in options:
      if ((qns_data.AnswerLabel.value_counts()[str(i)]/len(qns_data))*100-unbiased_percentage>0)&(total_options>0):
        bias_param.append(((qns_data.AnswerLabel.value_counts()[str(i)]/len(qns_data))*100-unbiased_percentage)*(total_options-1))
      else:
        bias_param.append(0)
    bp_saathi.append(int(np.mean(bias_param)))

  if len(bp_saathi)>0:
    output=int(np.mean(normalise(bp_saathi,0,100)))
  else:
    output=0
  return output


print(bias_parameter_for_saathi("clientformdatas_tataamc","clientformdatas_answer_tataamc",70,"5acf2ccd750df44e5ef189cc"))








