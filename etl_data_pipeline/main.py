from prometheus_api_client import PrometheusConnect, MetricsList, MetricSnapshotDataFrame, MetricRangeDataFrame
from datetime import timedelta
from prometheus_api_client.utils import parse_datetime
from confluent_kafka import Producer

from flask import Flask
import pandas as pd
import xlsxwriter
import json
import sys
import time
import matplotlib.pyplot as plt
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.seasonal import seasonal_decompose
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller,kpss,coint,bds,q_stat,grangercausalitytests,levinson_durbin


app = Flask(__name__)
prom = PrometheusConnect(url="http://15.160.61.227:29090/", disable_ssl=True)

def prometheus_metrics():
    metric_names = [] 
    filtered_metrics = [] 

    metric_data = prom.get_metric_range_data(
        metric_name='',
        label_config={'job': 'ceph-metrics'}
    )
    print(len(metric_data))

    for metric in metric_data:
        for value1,value2 in metric["values"]:
            if value2 != '0' and metric["metric"]["__name__"] not in filtered_metrics:
                filtered_metrics.append(metric["metric"]["__name__"])

    print(len(filtered_metrics))
    #Se vogliamo cambiare il numero di metriche da prendere basta cambiare il ciclo for
    for k in range(1,45,3): #(2,6) prova, (45,48) 3 metriche, (2,22) per 20 metriche, (3,18) per 15 metriche, (1,45,3) per 15 metriche
        metric_names.append(filtered_metrics[k])
    return metric_names

def setting_parameters(metric_name, label_config, start_time_metadata, end_time, chunk_size):
    start = time.time()
    metric_data = prom.get_metric_range_data(
        metric_name=metric_name,
        label_config=label_config,
        start_time=start_time_metadata,
        end_time=end_time,
        chunk_size=chunk_size,
    )
    end = time.time()

    return metric_data, (end - start)

def creating_file_csv(metric_name, metric_object_list):
    xlxsname = metric_name + str('.xlsx')
    csvname = metric_name + str('.csv')

    workbook = xlsxwriter.Workbook("csvMetriche/"+xlxsname)
    worksheet = workbook.add_worksheet()
    
    row = 0
    col = 0
    format = workbook.add_format({'num_format': 'yyyy-mm-dd hh:mm:ss.ms'})

    for item in metric_object_list.metric_values.ds:
        worksheet.write(row, col, item, format)
        row += 1

    row = 0
     
    for item in metric_object_list.metric_values.y:
        worksheet.write(row, col + 1, item)
        row += 1

    workbook.close()

    read_file = pd.read_excel("csvMetriche/"+xlxsname)
    read_file.to_csv("csvMetriche/"+csvname, index=None)
    return
    
def calculate_values(metric_name, metric_df, start_time):
    max = metric_df['value'].max()
    min = metric_df['value'].min()
    avg = metric_df['value'].mean()
    std = metric_df['value'].std()
    # print(metric_name, "\nstart time: " + start_time, "\n max: ", max, "\n min: ", min, "\n avg: ", avg, "\n std: ", std)

    result_values[str(metric_name + "," + str(start_time))] = {"max": max, "min": min, "avg": avg, "std": std}
    return result_values

def stationarity(values):
    stationarityTest = adfuller(values,autolag='AIC')

    if(stationarityTest[1] <= 0.05):
        print("[p-value]:", stationarityTest[1])
        result_stationarity = 'stationary series'
    else:
        print("[p-value]:", stationarityTest[1])
        result_stationarity = 'no stationary series'
    return result_stationarity

def seasonality(values):
    result = seasonal_decompose(values, model='additive',period=5)
    serializable_result = {str(k): v for k, v in result.seasonal.to_dict().items()}
    return serializable_result

def autocorrelation(values):
    lags = len(values) 
    result_autocorrelation = sm.tsa.acf(values, nlags=lags-1).tolist()

    print("[lags]:", lags)

    return result_autocorrelation

def valuesCalculation(metric_df):
    dictValues = {}
    dictValues = {"autocorrelation": autocorrelation(metric_df['value']),
                  "stationarity": stationarity(metric_df['value']),
                  "seasonality": seasonality(metric_df['value'])
    }
    return dictValues

def predict(metric_df):
    prediction_list={}
    num_samples = 5 

    data = metric_df.resample(rule='10s').mean(numeric_only='True')
    tsmodel = ExponentialSmoothing(data.dropna(), trend='add', seasonal_periods=5).fit()
    prediction = tsmodel.forecast(steps=round((10*60)/num_samples))
    prediction_list = {"max": prediction.max(),
                       "min": prediction.min(),
                       "avg": prediction.mean()}

    return prediction_list

def predictFiveMetrics(metric_name, metric_df):
    prediction_list = {}

    data = metric_df.resample(rule='10s').mean(numeric_only='True')
    #Impostare i valori del 'tred', della 'seasonal' e del 'seasonal_periods' in base al modello trovato
    match metric_name:
        case "ceph_osd_op_rw_latency_sum":
            tsmodel = ExponentialSmoothing(data.dropna(), trend='add', seasonal='add', seasonal_periods=5).fit()
            prediction = tsmodel.forecast(6) 

        case "ceph_bluestore_read_lat_sum":
            tsmodel = ExponentialSmoothing(data.dropna(), trend='add', seasonal='add', seasonal_periods=5).fit()
            prediction = tsmodel.forecast(6) 

        case "ceph_bluefs_read_prefetch_bytes":
            tsmodel = ExponentialSmoothing(data.dropna(), trend='add', seasonal='add', seasonal_periods=5).fit()
            prediction = tsmodel.forecast(6) 
        
        case "ceph_osd_op_r_process_latency_sum":
            tsmodel = ExponentialSmoothing(data.dropna(), trend='add', seasonal='add', seasonal_periods=5).fit()
            prediction = tsmodel.forecast(6)  

        case "ceph_bluestore_submit_lat_count":
            tsmodel = ExponentialSmoothing(data.dropna(), trend='add', seasonal='add', seasonal_periods=5).fit()
            prediction = tsmodel.forecast(6)  

    prediction_list = {"max": prediction.max(), "min": prediction.min(), "avg": prediction.mean()}
    return prediction_list

def kakfaResultProducer(fileJson,keyword):
    #print("File json passato: ", fileJson)

    broker = "kafka:9092"
    topic = "prometheusdata"

    conf = {'bootstrap.servers': broker}
    #print("Configurazione del producer: ", conf)
    
    p = Producer(**conf)

    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))
    try:
        record_key = keyword
        record_value = json.dumps(fileJson)
        print("Producing record: {}\t{}".format(record_key, record_value))
        p.produce(topic, key=record_key, value=record_value, callback=delivery_callback)


    except BufferError:
        sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                         len(p))
    p.poll(0)

    print("\n Il producer totale: ", p)
    sys.stderr.write('%% Waiting for %d deliveries\n,' % len(p))
    p.flush()

@app.route('/')
def menu():
    menu = {"execution_time_p1": "http://127.0.0.1:5000/execution_time_p1",
            "execution_time_p2": "http://127.0.0.1:5000/execution_time_p2",
            "execution_time_p3": "http://127.0.0.1:5000/execution_time_p3"}
    return menu

@app.get('/execution_time_p1')
def get_execution_time_p1():    
    return execution_time_p1

@app.get('/execution_time_p2')
def get_execution_time_p2():    
    return execution_time_p2

@app.get('/execution_time_p3')
def get_execution_time_p3():    
    return execution_time_p3

if __name__ == "__main__":
    result_metadata = {} 
    result_values = {} 
    result_predict = {}  
    execution_time_p1 = {} 
    execution_time_p2 = {} 
    execution_time_p3 = {} 

    #Punto1    
    metric_name = prometheus_metrics()

    #Punto 3 Set di 5 metriche
    predict_metric_name = ['ceph_osd_op_r_process_latency_sum','ceph_osd_op_rw_latency_sum','ceph_bluestore_read_lat_sum', 'ceph_bluestore_submit_lat_count', 'ceph_bluefs_read_prefetch_bytes']
   
    label_config = {'job': 'ceph-metrics'}
    start_time = ["1h","3h","12h"] 
    
    start_time_metadata = parse_datetime("48h")  
    start_time_meta_name="48h"
    end_time = parse_datetime("now")

    chunk_size = timedelta(minutes=10) #minutes=10. minutes =1
    print("Parametri impostati\n")
 
    for name in range(0, len(metric_name)):
        #print(metric_name[name])

        metric_data, execution_time = setting_parameters(metric_name[name], label_config, start_time_metadata, end_time,
                                                    chunk_size)
        

        execution_time_p1[metric_name[name]] = {"time_sec": str(execution_time)}
        # print(metric_data)
        metric_object_list = MetricsList(metric_data)

        metric_df = MetricRangeDataFrame(metric_data)

        #Scommentare se si vuole salvare il file csv di tutte le n metriche
        #creating_file_csv(metric_name[name],metric_object_list[0])

        #Punto 1 calcolo di stagionalità, stazionarietà e autocorellazione
        result_metadata[metric_name[name]] = valuesCalculation(metric_df)
        print(result_metadata)
        
        #Scommentare se si vuole fare la predizione di tutte le n metriche
        #result_predict[metric_name[name]] = predict(metric_df)
        #print(result_predict)
        
        #Punto 2
        # Calcoli il valore di max, min, avg, dev_std della metriche per 1h,3h, 12h;
        for h in range(0, len(start_time)):
            metric_data, execution_time = setting_parameters(metric_name[name], label_config,
                                                          parse_datetime(start_time[h]), end_time,
                                                          chunk_size)
            execution_time_p2[metric_name[name] + "," + start_time[h]] = {"time_sec": str(execution_time)}
            metric_object_list = MetricsList(metric_data)
            my_metric_object = metric_object_list[0]
            metric_df = MetricRangeDataFrame(metric_data)

            result_values = calculate_values(metric_name[name], metric_df, start_time[h])
    
    #Punto 3
    #Predizione del set di 5 metriche
    for name in range(0,len(predict_metric_name)):
        metric_data, execution_time = setting_parameters(predict_metric_name[name], label_config, start_time_metadata, end_time,
                                                    chunk_size)
        metric_object_list = MetricsList(metric_data)
        creating_file_csv(predict_metric_name[name],metric_object_list[0])
        metric_df = MetricRangeDataFrame(metric_data)
        execution_time_p3[predict_metric_name[name] + "," + start_time_meta_name] = {"time_sec": str(execution_time)}
        result_predict[predict_metric_name[name]] = predictFiveMetrics(predict_metric_name[name], metric_df)


    print(result_predict)

    print("[execution_time_p1]:" + str(execution_time_p1) + " sec\n")
    print("[execution_time_p2]:" + str(execution_time_p2) + " sec\n")
    print("[execution_time_p3]:" + str(execution_time_p3) + " sec\n")
    
    print("\n\n")

    kakfaResultProducer(result_metadata,'etl_dp#1')
    kakfaResultProducer(result_values,'etl_dp#2')
    kakfaResultProducer(result_predict,'etl_dp#3')

    kakfaResultProducer(execution_time_p1,'etl_dp#4')
    kakfaResultProducer(execution_time_p2,'etl_dp#5')
    kakfaResultProducer(execution_time_p3,'etl_dp#6')

    app.run(port=5000, debug=False)
       

