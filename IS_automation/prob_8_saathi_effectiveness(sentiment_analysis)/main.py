def saathi_effectiveness():
  import pandas as pd
  import numpy as np
  from google.cloud import bigquery
  from google.oauth2 import service_account
  import re
  import string
  import nltk
  import warnings
  from nltk.corpus import stopwords
  warnings.filterwarnings("ignore", category=DeprecationWarning)
  from nltk.tokenize import *
  nltk.download("popular")
  from nltk.stem import WordNetLemmatizer

  import textblob
  from textblob import TextBlob

  import nltk
  nltk.download('punkt')

  credentials = service_account.Credentials.from_service_account_file('infra-service.json')
  project_id = 'infra-211714'
  client = bigquery.Client(credentials= credentials,project=project_id)

  original_data= client.query('''SELECT * FROM `infra-211714.is_playstore.p_Reviews_IS` ''').result().to_dataframe()
  data=original_data[["Review_Text","Star_Rating"]].dropna()

  #Removing punctuation
  def remove_punctuation(text):
    no_punct="".join([c for c in text if c not in string.punctuation])
    return no_punct

  data["review_text"]=data.Review_Text.apply(lambda x:remove_punctuation(x))
  #tokenizing the words....
  tokenizer=RegexpTokenizer(r'\w+')
  data["review_text"]=data["review_text"].apply(lambda x:tokenizer.tokenize(x.lower()))
  #removing stopwords
  sw=stopwords.words("english")
  def remove_stopwords(text):
    a=[word for word in text if word not in sw]
    return a
  data["review_text"]=data["review_text"].apply(lambda x:remove_stopwords(x))
  #Lematization
  lematizer=WordNetLemmatizer()
  def lematizer_fun(text):
    lem=[lematizer.lemmatize(i) for i in text]
    return lem
  data["review_text"]=data["review_text"].apply(lambda x:lematizer_fun(x))
  #stemming
  from nltk.stem.porter import *
  stemmer = PorterStemmer()
  def stem(x):
    el=[]
    for m in (x):
      el.append(stemmer.stem(m))
    return " ".join(el)
  data["review_text"]=data["review_text"].apply(lambda x:stem(x))
  data["sentiment"]=data.Review_Text.apply(lambda x:TextBlob(x).sentiment[0])
  data["polarity"]=data.Review_Text.apply(lambda x:TextBlob(x).sentiment[1])
  mean_sentiment=data.sentiment.mean()
  return "mean sentiment of the feedback is "+mean_sentiment
saathi_effectiveness()
