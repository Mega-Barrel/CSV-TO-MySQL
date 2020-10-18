import csv
import pymysql

file_name = 'C:/Users/saura/Desktop/SQL/Sql_for_datascience/Dataset/dsv/events.csv'

create_table = '''CREATE TABLE IF NOT EXISTS events (
    event_id        VARCHAR(100) NOT NULL PRIMARY KEY
  , event_time      VARCHAR(30) NOT NULL
  , user_id         NUMERIC(8,1) NOT NULL
  , event_name      VARCHAR(15) NOT NULL
  , platform        VARCHAR(15) NOT NULL
  , parameter_name  VARCHAR(10) NOT NULL
  , parameter_value INTEGER  NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; '''


mydb = pymysql.connect(host="localhost",
                    user="root",
                    passwd="joshi240301",
                    db="dsv",
                    port=3306)

cursor = mydb.cursor()

try:
    cursor.execute(create_table)
    mydb.commit()
except:
    print('Error Occured while creating Table!')

with open(file_name, newline='\n') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',')
    next(csvreader)
    for row in csvreader:
        
        print('\n')
        print(row)
        print('Started Inserting......')

        event_id = row[0]
        event_time = row[1]
        user_id = row[2]
        event_name = row[3]
        platform = row[4]
        parameter_name = row[5]
        parameter_value = row[6]
        
        sql_query = "INSERT INTO dsv.events (event_id, event_time, user_id, event_name, platform, parameter_name, parameter_value) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (event_id, event_time, user_id, event_name, platform, parameter_name, parameter_value)
        try:
            cursor.execute(sql_query)
            mydb.commit()
        except:
            print('Error: Unable to Insert The Data!')

mydb.close()