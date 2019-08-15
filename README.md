# Project: Data Lake

## Executive Summary
In this project, I applied what I learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, I loaded data from S3, processed the data into analytics tables using Spark, and loaded them back into S3. I deployed this Spark process on a cluster using AWS.

The etl.py script contains both Spark Python logic (aka pyspark) and Spark SQL logic.

For the sake of expediency, after populating the songplays fact table I am using the key values from this table to determine what key values will remain in the related dimension tables. In essence I am getting rid of dimension records where the key value does not exist on the fact table. This will reduce the number of rows in the dimension tables and thus reduce the amount of time that it takes to create parquet files etc.

**NOTE** I only tested this in a local project workspace or personal local workstation (i.e. single server). I will look at what is required to run the etl.py script in an EMR cluster where I can use multiple servers.

###  Background
#### source files
* I took the zip files from the "Project Workspace" "data" folder and downloaded and unzipped to my local machine
* I then created the applicable folder structure in s3
* I then uploaded the applicable files to s3 folder structure
* based on my initial review...
  * `REQUIRED` The input_data should be kept as "s3a://udacity-dend/"
#### target files
* on s3 i created a target folder with the applicable table subfolders

## ETL pipeline
consists of 3 file(s):
1. dl.cfg - data lake configuration file
2. etl.py -  The script reads song_data and load_data from S3, transforms them to create five different tables, and writes them to partitioned parquet files in table directories on S3.
3. README.md - markdown file which is this file

## How To Execute
### To Parquet or Not To Parquet...That Is The Question...

In the "Project Workspace" "Launcher" tab click on the "Terminal" square. 

`To Parquet`

If you want to `create or overwrite parquet files` then at the command prompt type the following and press the `enter` key:

```
python etl.py true
```

`or Not To Parquet...`

If you want to `bypass the creation or overwriting of parquet files` then at the command prompt type the following and press the `enter` key:

```
python etl.py false
```

I have put a few print statements into the code to provide actual results as follows:

## Actual Results
```
songplays info...
count(s)
+----+-----+-----+
|year|month|count|
+----+-----+-----+
|2018|   11|  301|
+----+-----+-----+

root
 |-- songplay_id: long (nullable = false)
 |-- start_time: timestamp (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- user_id: string (nullable = true)
 |-- level: string (nullable = true)
 |-- song_id: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- session_id: long (nullable = true)
 |-- location: string (nullable = true)
 |-- user_agent: string (nullable = true)

+-----------+--------------------+----+-----+-------+-----+------------------+------------------+----------+--------------------+--------------------+
|songplay_id|          start_time|year|month|user_id|level|           song_id|         artist_id|session_id|            location|          user_agent|
+-----------+--------------------+----+-----+-------+-----+------------------+------------------+----------+--------------------+--------------------+
|          0|2018-11-15 01:31:...|2018|   11|     49| paid|SOCUITT12AB0187A32|ARKS2FE1187B99325D|       606|San Francisco-Oak...|Mozilla/5.0 (Wind...|
|          1|2018-11-15 04:44:...|2018|   11|     80| paid|SOSDZFY12A8C143718|AR748W61187B9B6AB8|       611|Portland-South Po...|"Mozilla/5.0 (Mac...|
|          2|2018-11-15 04:48:...|2018|   11|     80| paid|SOTNWCI12AAF3B2028|ARS54I31187FB46721|       611|Portland-South Po...|"Mozilla/5.0 (Mac...|
|          3|2018-11-15 04:52:...|2018|   11|     80| paid|SOBONKR12A58A7A7E0|AR5E44Z1187B9A1D74|       611|Portland-South Po...|"Mozilla/5.0 (Mac...|
|          4|2018-11-15 05:39:...|2018|   11|     30| paid|SOULTKQ12AB018A183|ARKQQZA12086C116FC|       324|San Jose-Sunnyval...|Mozilla/5.0 (Wind...|
+-----------+--------------------+----+-----+-------+-----+------------------+------------------+----------+--------------------+--------------------+
only showing top 5 rows

users info...
count(s)
56
root
 |-- user_id: string (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- gender: string (nullable = true)

+-------+----------+---------+------+
|user_id|first_name|last_name|gender|
+-------+----------+---------+------+
|     52|  Theodore|    Smith|     M|
|     25|    Jayden|   Graves|     M|
|     10|    Sylvie|     Cruz|     F|
|     71|    Ayleen|     Wise|     F|
|     82|     Avery| Martinez|     F|
+-------+----------+---------+------+
only showing top 5 rows

songs info...
count(s)
+----+-----+
|year|count|
+----+-----+
|   0|   48|
|1957|    1|
|1968|    2|
|1972|    1|
|1973|    1|
|1974|    1|
|1978|    1|
|1979|    1|
|1980|    2|
|1981|    1|
|1982|    3|
|1984|    1|
|1985|    2|
|1986|    2|
|1987|    1|
|1988|    3|
|1989|    2|
|1990|    2|
|1991|    3|
|1992|    2|
+----+-----+
only showing top 20 rows

root
 |-- year: long (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- duration: double (nullable = true)

+----+------------------+------------------+--------------------+---------+
|year|         artist_id|           song_id|               title| duration|
+----+------------------+------------------+--------------------+---------+
|2005|AR3FYKL1187FB44945|SODVXIB12AF72A37F3|Settle For A Slow...|  223.242|
|2000|ARQUMH41187B9AF699|SOEKSGJ12A67AE227E|Crawling (Album V...|208.95302|
|1994|AR4JOZD11C8A421F6A|SOQIHQO12AB018A35C|Wax on Tha Belt (...| 35.81342|
|2008|ARTYXEZ1187FB54560|SOYRFUE12AB0183E5C|Sometime Around M...|303.56853|
|2004|ARFSZGT1187B9B1E44|SOBBHVN12A6702162D|More Adventurous ...|205.26975|
+----+------------------+------------------+--------------------+---------+
only showing top 5 rows

artists info...
count(s)
187
root
 |-- artist_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- location: string (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)

+------------------+-----------+--------------------+--------+---------+
|         artist_id|       name|            location|latitude|longitude|
+------------------+-----------+--------------------+--------+---------+
|ARKS2FE1187B99325D|      Mia X|     New Orleans, LA|29.95369|-90.07771|
|AR7SPB81187B9A068F| Stray Cats|                null|    null|     null|
|ARRB3GQ1187FB52D14|Angie Stone|                null|    null|     null|
|ARHQBRZ1187FB3BDA2|    MC Lars|Berkeley, California|    null|     null|
|ARCPYBD1187B9A56CE| The Format|     Peoria, Arizona|33.57731|-112.2409|
+------------------+-----------+--------------------+--------+---------+
only showing top 5 rows

time info...
count(s)
+----+-----+-----+
|year|month|count|
+----+-----+-----+
|2018|   11|  301|
+----+-----+-----+

root
 |-- start_time: timestamp (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- dayofmonth: integer (nullable = true)
 |-- hour: integer (nullable = true)
 |-- minute: integer (nullable = true)
 |-- second: integer (nullable = true)
 |-- dayofweek: integer (nullable = true)
 |-- dayofyear: integer (nullable = true)
 |-- weekofyear: integer (nullable = true)

+--------------------+----+-----+----------+----+------+------+---------+---------+----------+
|          start_time|year|month|dayofmonth|hour|minute|second|dayofweek|dayofyear|weekofyear|
+--------------------+----+-----+----------+----+------+------+---------+---------+----------+
|2018-11-21 14:50:...|2018|   11|        21|  14|    50|    20|        4|      325|        47|
|2018-11-16 11:00:...|2018|   11|        16|  11|     0|    36|        6|      320|        46|
|2018-11-23 22:34:...|2018|   11|        23|  22|    34|    19|        6|      327|        47|
|2018-11-08 08:27:...|2018|   11|         8|   8|    27|     8|        5|      312|        45|
|2018-11-02 20:12:...|2018|   11|         2|  20|    12|    26|        6|      306|        44|
+--------------------+----+-----+----------+----+------+------+---------+---------+----------+
only showing top 5 rows
```

### CONCEPTS
can be found in the `Project-Data-Lake-CONCEPTS.md` file.

### RUBRIC
can be found in the `Project-Data-Lake-RUBRIC.md` file.