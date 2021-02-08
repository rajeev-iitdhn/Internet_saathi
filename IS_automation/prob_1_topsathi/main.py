pip install --upgrade google-cloud-bigquery


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from google.cloud import bigquery
import pandas_gbq
import pydata_google_auth
import json

from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor
import xgboost as xgb

from google.cloud import bigquery
from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_file(
    'infra-211714-fa79bf90e271.json')
project_id = 'infra-211714'
client = bigquery.Client(credentials= credentials,project=project_id)

def run():
  original_data = client.query("""with
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
  `infra-211714.is_dashboard.clientformdatas_washprogramme` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt ,language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt from
  `infra-211714.is_dashboard.clientformdatas_tataamc` 
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
  `infra-211714.is_dashboard.clientformdatas_undpkarnataka` union all
  select id, formId, transactionID, formUniqueId, isActive, createdAt,
  modifiedAt, language, location, timeTaken, deviceId, loginId,
  internetSathiProfileId, village, block, district, state, partner,
  mobileCreatedAt, mobileUpdatedAt,assessmentScore from
  `infra-211714.is_dashboard.clientformdatas_tataamc` 
  ),
  question as (select count(distinct  question) as count,formId from
  `infra-211714.is_dashboard.formquestions` group by formId  ),
  preass as (select cf.deviceId,cf.formId, avg(cf.assessmentScore)as
  mean_assscore  from
  ccf_assess
  as cf
  group by deviceId, formId),
  livelihoodSaathi as (SELECT distinct deviceId FROM
  `infra-211714.is_dashboard.saathiprofiles`),
  overallDate as (select cf.deviceId,MIN(cf.createdAt) as
  StartDate,MAX(cf.createdAt) as EndDate from
  ccf as cf join livelihoodSaathi on cf.deviceId =
  livelihoodSaathi.deviceId group by cf.deviceID),
  sp as ( SELECT  id, ARRAY_AGG( fl ORDER BY modifiedAt DESC LIMIT 1
  )[OFFSET(0)].* EXCEPT (id) FROM
  `infra-211714.is_dashboard.saathiprofiles` as fl  GROUP BY id),
  WorkingDuration as (select b.deviceId,count(distinct
  Date(cf.mobileCreatedAt  )) as
  OnFieldDaysCount,DATE_DIFF(DATE(overallDate.EndDate),DATE(overallDate.StartDate)
  ,day) as SystemDaysCount from sp as b  join ccf  as cf on cf.deviceId
  = b.deviceId join overallDate on overallDate.deviceId = cf.deviceId
  group
  by b.deviceId,overallDate.StartDate,overallDate.EndDate),
  vilblkCount as (select deviceId,count(distinct village) as
  vil,count(distinct block) as blk from ccf group by deviceId),
  forms as( SELECT formId, ARRAY_AGG( forms ORDER BY modifiedAt DESC
  LIMIT 1 )[OFFSET(0)].* EXCEPT (formid) FROM
  `infra-211714.is_dashboard.forms` as forms GROUP BY formId)

  select
  DISTINCT mt.deviceId,
    name,
    husbandprofession,
    NoOfFormsWorked,
     NoOfBeneficiaryToBeFilled,
  count(distinct ccf.id) as BeneficiariesFilled,
    vill_count,
    block_count,
    OnFieldDaysCount,
    SystemDaysCount,
    meanTimePerQuestion,
    meantimeperform,
    percent_ass_score,
    engagedWithLivelihoodActivity,
    otherLivelihoodActivity,
    numberOfChildren,
    educationalQualification,
    age,
    haveAadhaarCard,
    haveAtmCard,
    mt.village,
    mt.district,
    mt.state
  from

  (SELECT
    DISTINCT mainTable.deviceId,
    name,
    husbandprofession,
    count(mainTable.formId) as NoOfFormsWorked,
    sum(forms.fillCount) as NoOfBeneficiaryToBeFilled,
    vilblkCount.vil AS vill_count,
    vilblkCount.blk AS block_count,
    sum(OnFieldDaysCount) as OnFieldDaysCount,
    sum(SystemDaysCount)+1 AS SystemDaysCount,
    SUM(questionsinform)/SUM(mean_time_per_form) AS meanTimePerQuestion,
    AVG(mean_time_per_form) AS meantimeperform,
    AVG(percent_ass_marks) AS percent_ass_score,
    engagedWithLivelihoodActivity,
    otherLivelihoodActivity,
    numberOfChildren,
    educationalQualification,
    age,
    haveAadhaarCard,
    haveAtmCard,
    mainTable.village,
    mainTable.district,
    mainTable.state
  FROM (
    SELECT
      DISTINCT cf.deviceId,
      cf.name,
      cf.husbandProfession,
      mp.formId,
      WorkingDuration.OnFieldDaysCount,
      WorkingDuration.SystemDaysCount,
      question.count AS Questionsinform,
  --     cf1.noOfAttempts,
      AVG(mp.timeTaken/1000) AS mean_time_per_form,
      AVG(mp.timeTaken/1000)/question.count AS meanTimePerQuestion,
      preass.mean_assscore*100/question.count AS percent_ass_marks,
      cf.engagedWithLivelihoodActivity,
      cf.otherLivelihoodActivity,
      cf.numberOfChildren,
      cf.educationalQualification,
      cf.age,
      cf.haveAadhaarCard,
      cf.haveAtmCard,
      m.name AS village,
      ds.name AS district,
      st.name AS state
    FROM (
      SELECT
        *,
        DATE_DIFF(max_crd,min_crd,day) AS work_duration
      FROM (
        SELECT
          a.deviceId,
          CAST(MAX(b.createdAt) AS date) AS max_crd,
          CAST(MIN(b.createdAt) AS date) AS min_crd
        FROM
         sp AS a
        INNER JOIN
          ccf AS b
        ON
          a.deviceId=b.deviceid
        GROUP BY
          a.deviceid)) AS k
    INNER JOIN
      sp AS cf
    ON
      cf.deviceId =k.deviceId
    LEFT JOIN
      `is_dashboard.villages` AS m
    ON
      m.id=cf.village
    LEFT JOIN
      `is_dashboard.districts` AS ds
    ON
      ds.id=m.district
    LEFT JOIN
      `is_dashboard.states` AS st
    ON
      st.id=m.state
    LEFT JOIN
      ccf AS cf1
    ON
      cf.deviceid=cf1.deviceid
    LEFT JOIN (
      SELECT
        deviceId,
        formId,
        timetaken
      FROM
        ccf
      WHERE
        deviceId!="null") AS mp
    ON
      cf.deviceId=mp.deviceId
    JOIN
      question
    ON
      question.formId = mp.formId
    LEFT JOIN
      preass
    ON
      preass.deviceId=cf.deviceId
    JOIN
      WorkingDuration
    ON
      WorkingDuration.deviceId = cf.deviceId
    GROUP BY
      deviceId,
      formId,
      cf.name,
      cf.numberOfChildren,
      cf.engagedWithLivelihoodActivity,
      cf.husbandProfession,
      cf.otherLivelihoodActivity,
      cf.educationalQualification,
      cf.age,
      m.name,
      ds.name,
      st.name,
      question.count,
      question.formId,
      preass.mean_assscore,
      WorkingDuration.OnFieldDaysCount,
      WorkingDuration.SystemDaysCount,
      cf.haveAadhaarCard,
      cf.haveAtmCard) AS mainTable
  JOIN
    vilblkCount
  ON
    vilblkCount.deviceId = mainTable.deviceId
  join forms on forms.formId = mainTable.formId
  GROUP BY
    deviceId,
    name,
    husbandprofession,
    engagedWithLivelihoodActivity,
    otherLivelihoodActivity,
    numberOfChildren,
    educationalQualification,
    age,
    haveAadhaarCard,
    haveAtmCard,
    village,
    district,
    state,
    vill_count,
    block_count) as mt join ccf on ccf.deviceId = mt.deviceId group by
  mt.deviceId, name,
    husbandprofession,
    NoOfFormsWorked,
     NoOfBeneficiaryToBeFilled,
    vill_count,
    block_count,
    OnFieldDaysCount,
    SystemDaysCount,
    meanTimePerQuestion,
    meantimeperform,
    percent_ass_score,
    engagedWithLivelihoodActivity,
    otherLivelihoodActivity,
    numberOfChildren,
    educationalQualification,
    age,
    haveAadhaarCard,
    haveAtmCard,
    mt.village,
    mt.district,
    mt.state""").result().to_dataframe()
  return print(original_data.head())



