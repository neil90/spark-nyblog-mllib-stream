from cassandra.cluster import Cluster
import csv

#Initialize Cluster Connection
try:
    print("Establishing connection to default cluster 127.0.0.1:9024")
    cluster = Cluster()
    session = cluster.connect()
except:
    print("Can't establish connection.")
    exit()

#drop Keyspace if exists
print("Dropping Keyspace sparkdb if exists.")
session.execute("DROP KEYSPACE IF EXISTS sparkdb;")


#Creates KEYSPACE
print("Creating sparkdb Keyspace")
session.execute("""
				CREATE KEYSPACE sparkdb 
					WITH REPLICATION = { 'class' : 'SimpleStrategy' , 
                        'replication_factor' : 1};
				""")


#Create Table to store train data
print("Creating table ny_times_train in sparkdb Keyspace")
session.execute('USE sparkdb;')
session.execute("""
					CREATE TABLE ny_times_train (
						newsdesk varchar,
						sectionname varchar,
						subsectionname varchar,
						headline varchar,
						snippet varchar,
						abstract varchar,
						wordcount int,
            			pubdate timestamp,
            			popular int,
            			uniqueid int,
						PRIMARY KEY (uniqueid)
						);
				""")


#Create table to store stream test data
print("Create table to store stream test data")
session.execute("""
					CREATE TABLE ny_times_test_stream (
						newsdesk varchar,
						sectionname varchar,
						subsectionname varchar,
						headline varchar,
						snippet varchar,
						abstract varchar,
						wordcount int,
            			pubdate timestamp,
            			popular_probability float,
            			uniqueid int,
						PRIMARY KEY (uniqueid)
						);
				""")




#Read CSV and Insert into Table
print("Load CSV NYTimesBlogTrain and Insert into Table...\n")
insert_qry = r"""INSERT INTO sparkdb.ny_times_train (
                        newsdesk, sectionname, subsectionname, 
                        headline, snippet, abstract, wordcount, 
                        pubdate, popular, uniqueid) 
                        VALUES ('%s', '%s', '%s', '%s', '%s', 
                                '%s', %s, '%s', %s, %s);"""

try:
    csvfile = open('NYTimesBlogTrain.csv', 'r') 

except IOError:
    print("Error: can\'t find file or read data")

else:
    ny_data = csv.reader(csvfile, delimiter=',')
    next(ny_data)

    for row in ny_data:
		#Replace escape ' with '' for inserting
        row = [i.replace("'","''") for i in row ]		
        session.execute(insert_qry % (row[0], row[1],
                                row[2], row[3],
                                row[4], row[5],
                                row[6], row[7],
                                row[8], row[9]))

    print("Data successfully Inserted.")
