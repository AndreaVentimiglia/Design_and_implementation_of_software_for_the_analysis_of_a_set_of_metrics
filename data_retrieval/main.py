from flask import Flask
import mysql.connector

# mysql connection
mydb = mysql.connector.connect(
        host="db",
        user="root",
        database="DataStorage"
    )

mycursor = mydb.cursor()

app = Flask(__name__)

@app.get('/')
def hello():
    menu = {"Metrics": "http://127.0.0.1:5002/metrics",
            "Metadata": "http://127.0.0.1:5002/metadata", 
            "Predict": "http://127.0.0.1:5002/predict",
            "All metrics":"http://127.0.0.1:5002/all_metrics",
            "pastviolation":"http://127.0.0.1:5002/pastviolation",
            "futureviolation":"http://127.0.0.1:5002/futureviolation",
            "sla_status":"http://127.0.0.1:5002/sla_status",
            "executetimep1":"http://127.0.0.1:5002/executiontimep1",
            "executetimep2":"http://127.0.0.1:5002/executiontimep2",
            "executetimep3":"http://127.0.0.1:5002/executiontimep3"}
    return menu

@app.get('/metrics')
def get_values_metrics():
    sql = """SELECT * FROM metrics;"""
    # print(sql)

    mycursor.execute(sql)
    fileJson_metrics = {}
    list_metrics = []
    y = 0
    for x in mycursor:
        #print(x)
        list_metrics.append(x)
        max_metric = list_metrics[y][2]
        min_metric = list_metrics[y][3]
        avg_metric = list_metrics[y][4]
        std_metric = list_metrics[y][5]
        fileJson_metrics[list_metrics[y][1]] = {"max": max_metric, "min": min_metric, "avg": avg_metric, "std": std_metric}
        y = y + 1
        #print(fileJson_metrics)
    return fileJson_metrics

@app.get('/metadata')
def get_values_metadata():
    sql = """select * FROM metadata;"""
    # print(sql)

    mycursor.execute(sql)
    fileJson_metrics = {}
    list_metrics = []
    y = 0
    for x in mycursor:
        #print(x)
        list_metrics.append(x)
        autocorrelation = list_metrics[y][2]
        stationarity = list_metrics[y][3]
        seasonarity = list_metrics[y][4]
        
        fileJson_metrics[list_metrics[y][1]] = {"autocorrelation": autocorrelation, "stationarity": stationarity, "seasonarity": seasonarity}
        y = y + 1
        #print(fileJson_metrics)
    return fileJson_metrics

@app.get('/predict')
def get_values_predict():
    sql = """SELECT * FROM prediction;"""
    # print(sql)

    mycursor.execute(sql)
    fileJson_metrics = {}
    list_metrics = []
    y = 0
    for x in mycursor:
        #print(x)
        list_metrics.append(x)
        max_metric = list_metrics[y][2]
        min_metric = list_metrics[y][3]
        avg_metric = list_metrics[y][4]
        fileJson_metrics[list_metrics[y][1]] = {"max": max_metric, "min": min_metric, "avg": avg_metric}
        y = y + 1
        #print(fileJson_metrics)
    return fileJson_metrics

@app.get('/all_metrics')
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

@app.get('/pastviolation')
def get_pastviolation():
    sql = """SELECT * FROM pastviolation;"""
    # print(sql)

    mycursor.execute(sql)
    fileJson_metrics = {}
    list_metrics = []
    y = 0
    for x in mycursor:
        #print(x)
        list_metrics.append(x)
        rangeMin = list_metrics[y][2]
        rangeMax = list_metrics[y][3]
        violations = list_metrics[y][4]
        fileJson_metrics[list_metrics[y][1]] = {"rangeMin": rangeMin, "rangeMax": rangeMax, "violations	": violations}
        y = y + 1
        #print(fileJson_metrics)
    return fileJson_metrics

@app.get('/futureviolation')
def get_futureviolation():
    sql = """SELECT * FROM futureviolation;"""
    # print(sql)

    mycursor.execute(sql)
    fileJson_metrics = {}
    list_metrics = []
    y = 0
    for x in mycursor:
        #print(x)
        list_metrics.append(x)
        rangeMin = list_metrics[y][2]
        rangeMax = list_metrics[y][3]
        violations = list_metrics[y][4]
        fileJson_metrics[list_metrics[y][1]] = {"rangeMin": rangeMin, "rangeMax": rangeMax, "violations": violations}
        y = y + 1
        #print(fileJson_metrics)
    return fileJson_metrics

@app.get('/sla_status')
def get_sla_status():
    sql = """SELECT * FROM slastatus;"""
    # print(sql)

    mycursor.execute(sql)
    fileJson_metrics = {}
    list_metrics = []
    y = 0
    for x in mycursor:
        #print(x)
        list_metrics.append(x)
        violations = list_metrics[y][2]
        execution_time = list_metrics[y][3]
        fileJson_metrics[list_metrics[y][1]] = {"violations": violations, "execution_time_sec": execution_time}
        y = y + 1
        #print(fileJson_metrics)
    return fileJson_metrics

@app.get('/executiontimep1')
def get_executiontimep1():
    sql = """SELECT * FROM executiontimep1;"""
    # print(sql)

    mycursor.execute(sql)
    fileJson_metrics = {}
    list_metrics = []
    y = 0
    for x in mycursor:
        #print(x)
        list_metrics.append(x)
        executetime = list_metrics[y][2]
        fileJson_metrics[list_metrics[y][1]] = {"execute_time_sec": executetime}
        y = y + 1
        #print(fileJson_metrics)
    return fileJson_metrics

@app.get('/executiontimep2')
def get_executiontimep2():
    sql = """SELECT * FROM executiontimep2;"""
    # print(sql)

    mycursor.execute(sql)
    fileJson_metrics = {}
    list_metrics = []
    y = 0
    for x in mycursor:
        #print(x)
        list_metrics.append(x)
        executetime = list_metrics[y][2]
        fileJson_metrics[list_metrics[y][1]] = {"execute_time_sec": executetime}
        y = y + 1
        #print(fileJson_metrics)
    return fileJson_metrics

@app.get('/executiontimep3')
def get_executiontimep3():
    sql = """SELECT * FROM executiontimep3;"""
    # print(sql)

    mycursor.execute(sql)
    fileJson_metrics = {}
    list_metrics = []
    y = 0
    for x in mycursor:
        #print(x)
        list_metrics.append(x)
        executetime = list_metrics[y][2]
        fileJson_metrics[list_metrics[y][1]] = {"execute_time_sec": executetime}
        y = y + 1
        #print(fileJson_metrics)
    return fileJson_metrics

if __name__ == "__main__":
    app.run(port=5002, debug=False)

