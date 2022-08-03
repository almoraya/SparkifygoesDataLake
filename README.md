# Sparkify shifts from Data Warehouse to Data Lake

Sparkify, a startup with a new streaming app, has recently seen an increase in its customer base and its song database system. After discussions with an IT consulting firm, the company has decided to not only move its operations to the cloud, but also replace its data warehouse with a data lake. Sparkify hopes this will not only reduce costs and speed up response times, but also provide flexible scaling to meet business needs.


### Table of Contents

- [Project Summary](#project)
- [Data Warehouse vs Data Lake](#lake)
- [Spark and EMR](#spark)
- [How to run the Python Scripts](#python)
- [Getting started with Amazon EMR](#EMR)
- [Sparkify Data Files](#files)
- [List result files](#result)


<a name="project"/>

## Project Summary


After a steady growth of its user base, Sparkify decided to move its data warehouse to a data lake. The company chose AWS as its cloud provider and currently uses S3 as its central repository to store all structured and unstructured data. Currently, all files are stored in JSON format. To extract, load and transform the data, Sparkify will use Spark as the ELT engine for its data lake. Once all the data is collected, processed and loaded back into S3, the analytics team will be able to access it to gain valuable insights into how users are using and consuming the service. 


<a name="lake"/>

## Data Warehouse vs Data Lake

Data lakes and data warehouses are both commonly used for storing large amounts of data, but are used in different scenarios. A data lake is a "storage vessel" that can store a large amount of structured, semi-structured and unstructured data. The purpose of the stored data is not yet defined. A data warehouse, on the other hand, is a repository for structured, filtered data that has already been processed for a specific purpose. Data warehouse stores data in files or folders that help organize and use the data for strategic decision-making.

The following is a tabular summary of its main differences:

|   |Data Warehouse|Data Lake|
|---|---|---|
|Nature of Data|Is already processed|In its original form|
|Intended use of the data|In use at the moment|To be defined|
|Users of data|Business Analysts, Data Analysts|Data Engineers, Data Scientist|
|Level of accessibility|Low|High|



<a name="spark"/>

## Spark and EMR

Spark:

Apache Spark is an open source data processing engine that uses simple programming constructs to quickly perform processing tasks on very large datasets across multiple computer clusters. Apache Spark supports various programming languages such as R, Python, Java, and Scala.

EMR:

EMR stands for Elastic MapReduce and is a managed cluster platform that facilitates the execution of Big Data frameworks such as Apache Hadoop and Apache Spark on AWS to process and analyze data that is more diverse and arriving in ever-increasing volumes and at higher velocities.



<a name="python"/>

## How to run the Python scripts

In order to be able to run the provided python scripts, the following criteria needs to be fulfilled:
- Access to an AWS account with permission to access S3 buckets and to create an EMR cluster.
- Save all required string connections data in the *dl.cfg* file

In this case, we will deploy our Spark job using only two files: a configuration file and a Python script etl.py. 

These two files are:

1. a *dl.cfg* file: this file contains our access key and secret key. These key are necessary to connect to our S3 bucket.
2. a *etl.py* script: the main purpose of this script is to temporarily load data, transform it and upload it back to our S3 data store. In the end, our raw data will be used to create 5 usable tables.


<a name="EMR"/>

## Getting started with Amazon EMR

Below are some steps to consider when working with Amazon EMR. [("Getting started with Amazon EMR")](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html)

<p align="center">
<img  width="50%" height= "50%" src=/images/awsemr.png alt="EMR Workflow">
</p>

In order to be able to run the etl.py script on Amazon EMR you need to consider the following:

- Step 1: Plan and configure an Amazon EMR Cluster
  - Choose a storage solution like S3.
  - Choose a processing engine like Spark
  - Prepare the etl.py Pyspark script for EMR
  - Launch the cluster

- Step 2: Manage your Amazon EMR Cluster
  - Submit your etl.py script to cluster
  - Wait for tasks to finish
  - View results of your job

- Step 3: Clean Up your EMR Cluster
  - Terminate your cluster
  - If necessary, delete your resources


<a name="files"/>

## Sparkify Data Files

The files used in this repository are all stored in two **S3 buckets**. These buckets contain the song dataset and the log dataset, both in json format.
```
Song data: s3://<udacity-bucket>/song_data

Log data: s3://<udacity-bucket>/log_data
```

**The song dataset**

The song dataset is a subset of the [MillionDataSet](http://millionsongdataset.com/). These files contain real metadata about songs and artists. As mentioned earlier, it is a subset that contains only the first three letters of the track ID of each song; hence the partitioning.

The song files have the following structure:
```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

Below we can see what attributes make up a song file:
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```



**The log Dataset**

Unlike the song files, which are real data, the log files are simulated files. They were created using an external tool ([event simulator](https://github.com/Interana/eventsim)). These files are divided by month; however, only generated data for the month of November was used.

The log files have the following structure:
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

**Output Datasets**

The output files (dimensions and fact tables for our model) are also stored in an S3 bucket as *parquet files*. The files here can be used later for further processing and conversion.
Our output bucket has the following structure:

```
Output data: s3://<my-bucket>/<dim name>/<table name.parquet>/
```


<a name="result"/>

## List result files

**My output Data S3 Bucket**

At the end of our Spark job, according to the specification, we received our 5 folders with files partitioned by different attributes. These main folders are:
1. artists/artists_table.parquet/
2. songs/songs_table.parquet/
3. time/time_table.parquet/
4. users/user_table.parquet/
5. songplays/songplays_table.parquet/

These files can later be used to create a star schema model like the one below:

<p align="center">
<img  width="50%" height= "50%" src=/images/sparkify_er.png alt="Sparkify ER Diagram">
</p>
