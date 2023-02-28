import pandas as pd
from pymongo import MongoClient

#connect to mongo as a doctor and read data from a collection
myclient = MongoClient('mongodb://%s:%s@localhost:27017/Patient' % ('doctor', 'rotcod')) 

Patientdb=myclient['Patient']

medical_data=Patientdb['no_sepsisPatients']

df = pd.DataFrame(list(medical_data.find()))
print(df.head())   
