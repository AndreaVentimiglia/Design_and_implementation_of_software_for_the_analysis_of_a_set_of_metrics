from confluent_kafka import Consumer
import json
import mysql.connector

#collegamento del database
mydb = mysql.connector.connect(
    host="db",
    user="root",
    database="DataStorage"
)

#creazione del consumer
c = Consumer({
    'bootstrap.servers': 'kafka:9092', 
    'group.id': 'mygroup',
    'auto.offset.reset': 'latest' #earliest o latest
})

#sottoscrizione al topic kafka
c.subscribe(['prometheusdata'])

mycursor = mydb.cursor()

key_vector = []

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    #fileJson = json.loads(msg.value())
    #print(fileJson)
    #print("\nCHIAVI: "+ str(msg.key())+"\n\n")

    #Richieste dall'ETL_DATA_PIPELINE 1
    #Salvataggio dei dati (Stagionalità, Autocorrelazione e Stazionarietà)
    if str(msg.key()) == "b'etl_dp#1'":
        resultsP1 = json.loads(msg.value())
        key_vector.append(msg.key())
        print("Sto stampando etlDP1:\n")
        print(resultsP1)

        sql = """INSERT INTO metadata (metric_name, autocorrelation, stationarity, seasonality) VALUES (%s,%s,%s,%s);"""
        # print(sql)

        for key in resultsP1:
            metric_name = key
            autocorrelation = str(resultsP1[key]["autocorrelation"])
            stationarity = str(resultsP1[key]["stationarity"])
            seasonality = str(resultsP1[key]["seasonality"])
            val = (metric_name, autocorrelation, stationarity, seasonality)
            
            print(sql, val)
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "was inserted.")
            mycursor.execute("SELECT * FROM metadata;")

            for x in mycursor:
                print(x)

    #Richieste dall'ETL_DATA_PIPELINE 2
    #Salvataggio dei dati (max, min, avd, std)
    elif str(msg.key()) == "b'etl_dp#2'":
        resultsP2 = json.loads(msg.value())
        key_vector.append(msg.key())
        print("Sto stampando etlDP2:\n")
        print(resultsP2)
         
        sql = """INSERT INTO metrics (metric_info, max, min, avg, std) VALUES (%s,%s,%s,%s,%s);"""
        # print(sql)

        for key in resultsP2:
            metric_info = key
            max_metric = resultsP2[key]["max"]
            min_metric = resultsP2[key]["min"]
            avg_metric = resultsP2[key]["avg"]
            std_metric = resultsP2[key]["std"]
            val = (metric_info, max_metric, min_metric, avg_metric, std_metric)

            print(sql, val)
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "was inserted.")
            mycursor.execute("SELECT * FROM metrics;")

            for x in mycursor:
                print(x)

    #Richieste dall'ETL_DATA_PIPELINE 3
    #Salvataggio dei dati (max, min, avd, std) delle predizioni
    elif str(msg.key()) == "b'etl_dp#3'":
        resultsP3 = json.loads(msg.value())
        key_vector.append(msg.key())
        print("Sto stampando etlDP3:\n")
        print(resultsP3)

        sql = """INSERT INTO prediction (metric_name, max, min, avg) VALUES (%s,%s,%s,%s);"""
        # print(sql)

        for key in resultsP3:
            metric_name = key
            max_metric = resultsP3[key]["max"]
            min_metric = resultsP3[key]["min"]
            avg_metric = resultsP3[key]["avg"]
            val = (metric_name, max_metric, min_metric, avg_metric)
            
            print(sql, val)
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "was inserted.")
            mycursor.execute("SELECT * FROM prediction;")

            for x in mycursor:
                print(x)

    #Richieste dall'ETL_DATA_PIPELINE 4
    #Salvataggio dei tempi di esecuzione del punto 1
    elif str(msg.key()) == "b'etl_dp#4'":
        resultsT1 = json.loads(msg.value())
        key_vector.append(msg.key())
        print("Sto stampando etlDP4:\n")
        print(resultsT1)

        sql = """INSERT INTO executiontimep1 (metric_name, execution_time) VALUES (%s,%s);"""
        # print(sql)

        for key in resultsT1:
            metric_name = key
            execute_time = resultsT1[key]["time_sec"]
            val = (metric_name, execute_time)
            
            print(sql, val)
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "was inserted.")
            mycursor.execute("SELECT * FROM executiontimep1;")

            for x in mycursor:
                print(x)
    
    #Richieste dall'ETL_DATA_PIPELINE 5
    #Salvataggio dei tempi di esecuzione del punto 2
    elif str(msg.key()) == "b'etl_dp#5'":
        resultsT2 = json.loads(msg.value())
        key_vector.append(msg.key())
        print("Sto stampando etlDP5:\n")
        print(resultsT2)

        sql = """INSERT INTO executiontimep2 (metric_name, execution_time) VALUES (%s,%s);"""
        # print(sql)

        for key in resultsT2:
            metric_name = key
            execute_time = resultsT2[key]["time_sec"]
            val = (metric_name, execute_time)
            
            print(sql, val)
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "was inserted.")
            mycursor.execute("SELECT * FROM executiontimep2;")

            for x in mycursor:
                print(x)

    #Richieste dall'ETL_DATA_PIPELINE 6
    #Salvataggio dei tempi di esecuzione del punto 3
    elif str(msg.key()) == "b'etl_dp#6'":
        resultsT3 = json.loads(msg.value())
        key_vector.append(msg.key())
        print("Sto stampando etlDP6:\n")
        print(resultsT3)

        sql = """INSERT INTO executiontimep3 (metric_name, execution_time) VALUES (%s,%s);"""
        # print(sql)

        for key in resultsT3:
            metric_name = key
            execute_time = resultsT3[key]["time_sec"]
            val = (metric_name, execute_time)
            
            print(sql, val)
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "was inserted.")
            mycursor.execute("SELECT * FROM executiontimep3;")

            for x in mycursor:
                print(x)

    #Richieste dallo SLA_MANAGER 1
    #Salvataggio dei dati (rangeMin, rangeMax, violations) delle predizioni passate
    elif str(msg.key()) == "b'sla_m#1'":
        pastValue = json.loads(msg.value())
        key_vector.append(msg.key())
        print("Sto stampando sla_m#2:\n")
        print(pastValue)

        sql = """INSERT INTO pastviolation (metric_name, rangeMin, rangeMax, violations) VALUES (%s,%s,%s,%s);"""
        # print(sql)

        for key in pastValue:
            metric_name = key
            rangeMin = pastValue[key]["rangeMin"]
            rangeMax = pastValue[key]["rangeMax"]
            violations = pastValue[key]["violations"]
            val = (metric_name, rangeMin, rangeMax, violations)
            
            print(sql, val)
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "was inserted.")
            mycursor.execute("SELECT * FROM pastviolation;")

            for x in mycursor:
                print(x)

    #Richieste dallo SLA_MANAGER 2
    #Salvataggio dei dati (rangeMin, rangeMax, violations) delle predizioni future
    elif str(msg.key()) == "b'sla_m#2'":
        futureValue = json.loads(msg.value())
        key_vector.append(msg.key())
        print("Sto stampando sla_m#2:\n")
        print(futureValue)

        sql = """INSERT INTO futureviolation (metric_name, rangeMin, rangeMax, violations) VALUES (%s,%s,%s,%s);"""
        #print(sql)

        for key in futureValue:
            metric_name = key
            rangeMin = futureValue[key]["rangeMin"]
            rangeMax = futureValue[key]["rangeMax"]
            violations = futureValue[key]["violations"]
            val = (metric_name, rangeMin, rangeMax, violations)
            
            print(sql, val)
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "was inserted.")
            mycursor.execute("SELECT * FROM futureviolation;")

            for x in mycursor:
                print(x)

    #Richieste dallo SLA_MANAGER 3
    #Salvataggio delle informazioni sullo stato dello sla manager
    elif str(msg.key()) == "b'sla_m#3'":
        statusValue = json.loads(msg.value())
        key_vector.append(msg.key())
        print("Sto stampando sla_m#3:\n")
        print(statusValue)

        sql = """INSERT INTO slastatus (metric_name, violations, execution_time) VALUES (%s,%s,%s);"""
        #print(sql)

        for key in statusValue:
            metric_name = key
            violations = statusValue[key]["violations"]
            execute_time = statusValue[key]["execution_time_sec"]
            val = (metric_name, violations, execute_time)
            
            print(sql, val)
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "was inserted.")
            mycursor.execute("SELECT * FROM slastatus;")

            for x in mycursor:
                print(x)

c.close()
