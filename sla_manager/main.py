from prometheus_api_client import PrometheusConnect, MetricsList, MetricSnapshotDataFrame, MetricRangeDataFrame
from datetime import timedelta
from prometheus_api_client.utils import parse_datetime
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from flask import Flask
import sys
sys.path.append('..')
import mysql.connector
import time
import json
from confluent_kafka import Producer

prom = PrometheusConnect(url="http://15.160.61.227:29090/", disable_ssl=True)
app = Flask(__name__)

mydb = mysql.connector.connect(
        host="db",
        user="root",
        database="DataStorage"
    )

mycursor = mydb.cursor()

def get_all_metrics():
    sql = """SELECT * FROM metadata;"""
    # print(sql)
    mycursor.execute(sql)
    all_metrics = {}
    list_metrics = []
    y = 0
    for x in mycursor:
        #print(x)
        list_metrics.append(x)
        all_metrics[y] = list_metrics[y][1]
        y = y + 1

    return all_metrics

def enterMetricCodes(number_metric):
    print("select the code of 5 metrics: \n")
    print("Len:")
    print(len(get_all_metrics()))
    if len(get_all_metrics())>=5:
        for key in get_all_metrics():
            print(str(key) + " - " + get_all_metrics().get(key) + "\n")
            code_list.append(str(key))


        while len(metrics_sla) < number_metric:
            code_metric = input('\nInsert metric: ')
                
            if code_metric in code_list:
                if code_metric not in metrics_sla:
                        metrics_sla.append(code_metric)
                else:
                    print("metric already inserted, please enter another code\n")
            else:
                print("the entered code does not exist, please enter a valid code\n")
    else:
        print("there are less than 5 metrics in the database\n")
    return metrics_sla

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

def enterAllowedValues(number_metric, metrics_sla):
    k=0
    while k < number_metric:
            #print("\nkey: "+str(k))
            index = int(metrics_sla[k])
            #print("\nindex: "+str(index))
            print(str(get_all_metrics().get(index)+": "))
            x = input('\nyou want to enter max and min? y/n \n')

            if x=='y' or x=='n':
                if x == 'y':
                    min = input("enter the min: ")
                    max = input("enter the max: ")

                    if min.isalpha() or max.isalpha():
                        print("Invalid max and min values, please enter numerical values\n")
                    else:
                        allowed_values[get_all_metrics().get(index)] = {'min': min, 'max': max}
                        k = k + 1
                else:
                    metric_data, t = setting_parameters(get_all_metrics().get(index),
                                                                            label_config, start_time_range,
                                                                            end_time, chunk_size)
                    metric_df = MetricRangeDataFrame(metric_data)
                    allowed_values[get_all_metrics().get(index)] = {'min': metric_df['value'].min(),'max': metric_df['value'].max()}
                    k = k + 1
            else:
                print("Invalid answer entered, enter 'y' or 'n'\n")
    return allowed_values

def pastViolations(metrics_sla, allowed_values):
    for key in range(0, len(metrics_sla)):
        index = int(metrics_sla[key])
        metric_name =get_all_metrics().get(index)
        print("Key:") 
        print(key)
        print("index")
        print(index)
        rangeMin = int(allowed_values[get_all_metrics().get(index)]['min'])
        rangeMax = int(allowed_values[get_all_metrics().get(index)]['max'])
        num_violazioni = 0

        for h in range(0, len(start_time)):
            violazione=0
            metric_data, execution_time = setting_parameters(get_all_metrics().get(index),
                                                                       label_config, parse_datetime(start_time[h]),
                                                                       end_time, chunk_size)
            metric_df = MetricRangeDataFrame(metric_data)

            for k in metric_df['value']:
                if not rangeMin < k < rangeMax:
                    violazione =1
                    num_violazioni = num_violazioni + 1

            if violazione == 1:
                sla_status[metric_name+"_"+str(start_time[h])+"_past"]={"violations": "yes", "execution_time_sec":execution_time}
            else:
                sla_status[metric_name+"_"+str(start_time[h])+"_past"]={"violations": "no", "execution_time_sec":execution_time}
            past_violation_list[metric_name+"_"+str(start_time[h])]={"rangeMin": rangeMin, "rangeMax": rangeMax,"violations":num_violazioni}
            
            print(past_violation_list)
    return past_violation_list,sla_status

def futureViolations(metrics_sla, allowed_values):
    for key in range(0, len(metrics_sla)):
        index = int(metrics_sla[key])
        metric_name = get_all_metrics().get(index)
        rangeMin = int(allowed_values[get_all_metrics().get(index)]['min'])
        rangeMax = int(allowed_values[get_all_metrics().get(index)]['max'])
        start_time= '1h'
        num_violazioni = 0
        violazione=0
    
        metric_data, execution_time = setting_parameters(get_all_metrics().get(index), 
                                                                   label_config, 
                                                                   parse_datetime(start_time),
                                                                   end_time, chunk_size)
        metric_df = MetricRangeDataFrame(metric_data)
        num_samples = 5
        data = metric_df.resample(rule='10s').mean(numeric_only='True')
        tsmodel = ExponentialSmoothing(data.dropna(), trend='add', seasonal='add', seasonal_periods=5).fit()
        prediction = tsmodel.forecast(steps=round((10 * 60) / num_samples))
        
        for k in prediction.values:
            if not rangeMin < k < rangeMax:
                violazione=1
                num_violazioni = num_violazioni + 1

        if violazione == 1:
            sla_status[metric_name+"_10m_future"]={"violations": "yes", "execution_time_sec":execution_time}
        else:
            sla_status[metric_name+"_10m_future"]={"violations": "no","execution_time_sec":execution_time}
        future_violation_list[metric_name+"_10m"]={"rangeMin": rangeMin, "rangeMax": rangeMax,"violations":num_violazioni}
    return future_violation_list, sla_status

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
    menu = {"execution_time_p1": "http://127.0.0.1:5003/past_violation",
            "execution_time_p2": "http://127.0.0.1:5003/future_violation",
            "sla_status": "http://127.0.0.1:5003/sla_status"}
    return menu

@app.get('/past_violation')
def get_past_violation():    
    return past_violation_list

@app.get('/future_violation')
def get_future_violation():    
    return future_violation_list

@app.get('/sla_status')
def get_sla_status():    
    return sla_status

if __name__ == "__main__":
    label_config = {'job': 'ceph-metrics'}
    start_time = ["1h", "3h", "12h"]
    start_time_range = parse_datetime("30m")
    end_time = parse_datetime("now")
    chunk_size = timedelta(minutes=10) #minutes=1
    code_list=[]
    metrics_sla = []
    allowed_values = {}
    past_violation_metric = {}
    past_violation_list = {}
    future_violation_list = {}
    sla_status={}
    result_predict = {}
    number_metric = 5 
    
    print("\n***SLA MANAGER***")
    metrics_sla = enterMetricCodes(number_metric)
    
    if len(metrics_sla) !=0:
        print(metrics_sla)

        allowed_values = enterAllowedValues(number_metric, metrics_sla)
        print(allowed_values)
        
        #Punto 1
        past_violation_list,sla_status = pastViolations(metrics_sla, allowed_values)
        print(past_violation_list)
        
        #Punto 2
        future_violation_list,sla_status = futureViolations(metrics_sla, allowed_values)
        print(future_violation_list)

        print("\nSla status:")
        print(sla_status)

        kakfaResultProducer(past_violation_list,'sla_m#1')
        kakfaResultProducer(future_violation_list,'sla_m#2')
        kakfaResultProducer(sla_status,'sla_m#3')

        app.run(port=5003, debug=False)
    
