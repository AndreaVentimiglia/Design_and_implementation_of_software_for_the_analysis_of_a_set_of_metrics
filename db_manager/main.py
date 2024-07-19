import mysql.connector
import time

# Ritardo per permettere a MySQL di avviarsi
time.sleep(10)

# Connessione al database MySQL
db = mysql.connector.connect(
    host="db",
    user="root"
)

cursor = db.cursor()

# Creazione del database e delle tabelle
cursor.execute("CREATE DATABASE IF NOT EXISTS datastorage")
cursor.execute("USE datastorage")

# Creazione delle tabelle
tables = [
    """CREATE TABLE IF NOT EXISTS metadata (
        ID int AUTO_INCREMENT PRIMARY KEY, 
        metric_name varchar(255), 
        autocorrelation varchar(6000), 
        stationarity varchar(250), 
        seasonality varchar(6000)
    )""",
    """CREATE TABLE IF NOT EXISTS metrics (
        ID int AUTO_INCREMENT PRIMARY KEY, 
        metric_name varchar(255), 
        max double, 
        min double, 
        avg double, 
        std double
    )""",
    """CREATE TABLE IF NOT EXISTS prediction (
        ID int AUTO_INCREMENT PRIMARY KEY, 
        metric_name varchar(255), 
        max double, 
        min double, 
        avg double
    )""",
    """CREATE TABLE IF NOT EXISTS executiontimep1 (
        ID int AUTO_INCREMENT PRIMARY KEY, 
        metric_name varchar(255), 
        execution_time varchar(255)
    )""",
    """CREATE TABLE IF NOT EXISTS executiontimep2 (
        ID int AUTO_INCREMENT PRIMARY KEY, 
        metric_name varchar(255), 
        execution_time varchar(255)
    )""",
    """CREATE TABLE IF NOT EXISTS executiontimep3 (
        ID int AUTO_INCREMENT PRIMARY KEY, 
        metric_name varchar(255), 
        execution_time varchar(255)
    )""",
    """CREATE TABLE IF NOT EXISTS futureviolation (
        ID int AUTO_INCREMENT PRIMARY KEY, 
        metric_name varchar(255), 
        rangeMin double,
        rangeMax double, 
        violations double
    )""",
    """CREATE TABLE IF NOT EXISTS pastviolation (
        ID int AUTO_INCREMENT PRIMARY KEY, 
        metric_name varchar(255), 
        rangeMin double,
        rangeMax double, 
        violations double
    )""",
    """CREATE TABLE IF NOT EXISTS slastatus (
        ID int AUTO_INCREMENT PRIMARY KEY, 
        metric_name varchar(255), 
        violations varchar(25), 
        execution_time varchar(255)
    )"""
]

for table in tables:
    cursor.execute(table)

# Chiudere la connessione
cursor.close()
db.close()

print("Database and tables created successfully.")
