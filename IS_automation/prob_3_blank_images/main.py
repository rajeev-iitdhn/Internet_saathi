def obj_list():
  import pandas as pd
  import numpy as np
  import requests 
  from PIL  import Image
  import matplotlib.pyplot as plt
  from urllib.request import urlopen
  import time
  from io import BytesIO
  from tqdm import tqdm_notebook
  from imageai.Detection import ObjectDetection
  import os
  os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'


  #Setting the model path and loading the
  new_detector = ObjectDetection() #Instantiating ImageAI object detection
  new_detector.setModelTypeAsRetinaNet() #Setting the model's type as RetinaNet
  new_detector.setModelPath('resnet50_coco_best_v2.0.1.h5') #Loading the weights from the path
  new_detector.loadModel()



  im_dict={"form_id":2, "id":3, "url":list(['https://storage.googleapis.com/internetsaathi-prod/8eec3a8f-2650-4cbc-b54f-91b92b764231.jpg',
 'https://storage.googleapis.com/internetsaathi-prod/bb76f325-f43d-4ce9-b5cc-5abfdee55a88.jpg'])}
  object_list=[]
  # f_output=[]
  for i in tqdm_notebook(im_dict["url"]):
    response=requests.get(i)
    img=np.array(Image.open(BytesIO(response.content)))
    ol=new_detector.detectObjectsFromImage(input_image=img,input_type='array',output_type='array')
    ind=[]
    ind.append(im_dict["form_id"])
    ind.append(im_dict["id"])
    ind.append(i)
    for obj in range(0,len(ol[1])):
      ml=[]
      ml.append(ol[1][obj]["name"])
    op=[]  
    if len(ml)==0:
      ind.append("Blank")
    else:
      ind.append("Not Blank")
    object_list.append(tuple(list(ind)))

  return object_list

print(obj_list())
