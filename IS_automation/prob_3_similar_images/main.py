from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd, numpy as np
import scipy
import urllib
from scipy import spatial
import itertools
import cv2



credentials = service_account.Credentials.from_service_account_file('infra-service.json')
project_id = 'infra-211714'
client = bigquery.Client(credentials= credentials,project=project_id)

data=client.query('''SELECT nested_answer FROM `infra-211714.is_dashboard.clientformdatas_answer_googlebolo` 
where nested_question="16.017"''').result().to_dataframe().dropna()
data.columns[data.isnull().any()]
links=list(data.nested_answer)
input_dict={"deviceId":"abcd", "id":"ab","formId":34,"url":links[:5]}


def url_to_image(url):
  # download the image, convert it to a NumPy array, and then read
  # it into OpenCV format
  resp = urllib.request.urlopen(url)
  image = cv2.imdecode(np.asarray(bytearray(resp.read()), dtype="uint8"), cv2.IMREAD_COLOR)
  # image=cv2.resize(image, (int(image.shape[1] * 60/ 100),int(image.shape[0] * 60/ 100)),interpolation = cv2.INTER_AREA)
  return image


def sim_image(input_dict):

  credentials = service_account.Credentials.from_service_account_file(
      'infra-service.json')
  project_id = 'infra-211714'
  client = bigquery.Client(credentials= credentials,project=project_id)

  data=client.query('''SELECT nested_answer FROM `infra-211714.is_dashboard.clientformdatas_answer_googlebolo` 
  where nested_question="16.017"''').result().to_dataframe().dropna()
  data.columns[data.isnull().any()]
  links=list(data.nested_answer)
  


 
  def extract_features_from_url(image_path, vector_size=32):
    image = url_to_image(image_path) 
    try:
      # Using KAZE, cause SIFT, ORB and other was moved to additional module
      # which is adding addtional pain during install
      alg = cv2.KAZE_create()
      # Dinding image keypoints
      kps = alg.detect(image)
      # Getting first 32 of them. 
      # Number of keypoints is varies depend on image size and color pallet
      # Sorting them based on keypoint response value(bigger is better)
      kps = sorted(kps, key=lambda x: -x.response)[:vector_size]
      # computing descriptors vector
      kps, dsc = alg.compute(image, kps)
      # Flatten all of them in one big vector - our feature vector
      dsc = np.array(dsc).flatten()
      # print(type(dsc))
      # Making descriptor of same size
      # Descriptor vector size is 64
      needed_size = (vector_size * 64)
      if dsc.size < needed_size:
        # if we have less the 32 descriptors then just adding zeros at the
        # end of our feature vector
        dsc = np.concatenate([dsc, np.zeros(needed_size - dsc.size)])
    except cv2.error as e:
      print ('Error: ', e)
      return None

    return dsc

  sample_url=input_dict["url"]
  sim_image=[]
  feat=[]
  for i in range(0,len(sample_url),1):
    if len(feat)==0:
      feat.append(extract_features_from_url(sample_url[0]))
    else:
      ef=extract_features_from_url(sample_url[i]).reshape(1,-1)
      el=[]
      for m in range(0,np.array(feat).shape[0]):
        dis=scipy.spatial.distance.cdist(ef, feat[m].reshape(1,-1), metric="cosine").reshape(-1)
        if dis>0.2:
          sim_image.append(tuple([input_dict["deviceId"],input_dict["formId"],input_dict["id"],sample_url[i]]))
          el.append(dis)
          break
      if len(el)==0:
        feat.append(ef)
        # feat=np.vstack ((feat, ef))
  mat=tuple([input_dict["deviceId"],input_dict["formId"],input_dict["id"],feat])
  return sim_image,mat

print(sim_image(input_dict))
