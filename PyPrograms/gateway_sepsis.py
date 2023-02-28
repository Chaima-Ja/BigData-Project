
from kafka import KafkaProducer
import json
import datetime
import pandas as pd
from time import sleep



def data_to_kafka(nb_patient=10,n=2): 
                                                       

        #create kafka producer
    producer = KafkaProducer(bootstrap_servers= 'localhost:9092',
                       value_serializer=lambda v: json.dumps(v).encode('utf-8'))
  
   
    data=pd.read_csv('../Data/Dataset.csv').iloc[:nb_patient,:]
    sleep(9)
    #replace missing values with the mean
    data.fillna(data.mean(), inplace=True)
    sleep(9)
      
    for i in data.index :
        X= data.iloc[[i],:]
        #classification is based on the clinical data : 
        #the patient is not stable and vital signs are out of normal limits => urgent topic
        #the patient is stable and vital signs are within normal limits => normal topic
        if (X['HR'].values[0] <60 or X['HR'].values[0] >100 or X['O2Sat'].values[0] <95 or X['O2Sat'].values[0] > 100 or
        X['Temp'].values[0] > 39
        or X['SBP'].values[0] >=150 or X['MAP'].values[0] < 70 or X['MAP'].values[0] > 100 or X['DBP'].values[0] >=89 or
        X['Resp'].values[0] < 12 or X['Resp'].values[0] > 16 or X['EtCO2'].values[0] < 35 or X['EtCO2'].values[0] > 45  ) :
        	type_record = 'urgent'
        	topic_name = "urgent_data"  
        else:                              
        	topic_name= "normal_data"
        	type_record = 'normal'
        
        if (X['SepsisLabel'].values[0]==1) :
        	#classification : sepsis vs no sepsis
        	sepsis_patient= X.to_dict('records')[0]
        	producer.send('sepsis_data', sepsis_patient)
        	#classification : sepsis before admission to ICU vs sepsis after admission to ICU
        	if(X['Hour'].values[0]==0) :
        		admitted_with_sepsis_patient = X[['Hour', 'Age', 'Gender','Patient_ID', 'SepsisLabel']].to_dict('records')[0]
        		producer.send('sepsis_before_admission', admitted_with_sepsis_patient)
        		sleep(n)
        	else:
        		sepsis_after_adm_patient = X[['Hour', 'Age', 'Gender','Patient_ID', 'SepsisLabel']].to_dict('records')[0]
        		producer.send('sepsis_after_admission', sepsis_after_adm_patient)
        		sleep(n)
        else:
        	no_sepsis_patient=X.to_dict('records')[0]
        	producer.send('no_sepsis_data', no_sepsis_patient)
        		
            
        
        doc= X[['Hour','HR', 'O2Sat','Temp', 'SBP',	'MAP',	'DBP', 'Resp', 'EtCO2', 'Patient_ID']].to_dict('records')[0]
        #classification : laboratpry data
        laboratory_doc = X[['BaseExcess', 'HCO3', 'FiO2','pH', 'PaCO2', 'SaO2', 'AST', 'BUN', 'Alkalinephos', 'Calcium', 'Chloride', 'Creatinine', 	'Bilirubin_direct', 'Glucose', 'Lactate', 'Magnesium', 'Phosphate', 'Potassium', 'Bilirubin_total', 'TroponinI', 'Hct', 'Hgb', 'PTT', 'WBC', 'Fibrinogen', 'Platelets','Patient_ID','SepsisLabel']].to_dict('records')[0]
        producer.send('laboratory_values_data', laboratory_doc)
        #classification : demographic data
        demographics_doc=X[['Age', 'Gender', 'Unit1','Unit2', 'HospAdmTime',	'ICULOS', 'SepsisLabel', 'Patient_ID']].to_dict('records')[0]
        producer.send('demographics_data', demographics_doc)
        doc['date']=str(datetime.datetime.now().date())
        doc['time']=str(datetime.datetime.now().time())[:5]
        producer.send(topic_name,doc)   
        sleep(n)
        print("this data is urgent !!!" if type_record=='urgent' else "this data is normal")
        print(doc)
        sleep(n)
    producer.flush()


                                                	
data_to_kafka(nb_patient=300) 




