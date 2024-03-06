
# Objective 

This project focuses on building a data processing and analysis system from log data from users of a recruitment website. The main goal is to store, process, analyze that log data and decide the next step of the business development. 
Tech stack: PySpark, Kafka, Cassandra, MySQL, Python 
# Architecture

![[Pasted image 20240306103338.png]]

# Raw data 

- Log data from the website is processed real-time into Cassandra.
```
root 
	|-- create_time: string (nullable = false) 
	|-- bid: integer (nullable = true) 
	|-- campaign_id: integer (nullable = true)
	|-- custom_track: string (nullable = true) 
	|-- group_id: integer (nullable = true) 
	|-- job_id: integer (nullable = true) 
	|-- publisher_id: integer (nullable = true) 
	|-- ts: timestamp (nullable = true)
```

# Processing Data

- Filter actions with analytical value in column ["custom_track"] include: "click", "conversion", "qualified", "unqualified"
- Calculate the basic values of data for in-depth analysis.
- Use PySpark to write Spark jobs and process data.
- The processed data goes through Kafka.
- Data after processing is saved to Data Warehouse is MySQL for storage and in-depth analysis.

# Clean data

- Clean Data 

```
root
 |-- job_id: integer (nullable = true)
 |-- dates: timestamp (nullable = true)
 |-- hours: integer (nullable = true)
 |-- disqualified_application: integer (nullable = true)
 |-- qualified_application: integer (nullable = true)
 |-- conversion: integer (nullable = true)
 |-- company_id: integer (nullable = true)
 |-- group_id: integer (nullable = true)
 |-- campaign_id: integer (nullable = true)
 |-- publisher_id: integer (nullable = true)
 |-- bid_set: double (nullable = true)
 |-- clicks: integer (nullable = true)
 |-- impressions: string (nullable = true)
 |-- spend_hour: double (nullable = true)
 |-- sources: string (nullable = true)
 |-- latest_update_time: timestamp (nullable = true)
```
