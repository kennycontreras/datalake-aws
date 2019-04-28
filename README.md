# Data Lake with Spark on AWS

The project focus on create a data-lake using AWS. This is a good exercise if you want to migrate your data-warehouse to a data-lake.
The idea is extracts all datasets from a S3 bucket (Each file is in JSON format), process the information using `Spark` and loads the data back into S3.
Spark will be useful to process all the information and create analytics tables.

The data will be transformed and process using Spark `DataFrame`.

You can run this project in local mode or a cluster using AWS. Just make sure is you want to run this project in local mode, you may not be able to load all datasets. Instead, I recommend you to test the code with a small datasets and then move to AWS.

_This project was tested using Jupyter Notebooks on Amazon EMR Cluster._

## Pre-requisites

* Spark --Version >=2.0
* pyspark

**NOTE**: If you want to run this project on AWS cluster, make sure to have an authorized account. AWS is not free, but you can get a free-tier and execute this project successfully.

## Getting Started

The ETL pipeline is a Python file called `etl.py`. To run this Python file we need to make some configurations first.

##### Running the project on Local Mode

1. Configure your credentials file: If you noticed, after created `ConfigParser()` instance, config is reading a file called `credentials.cfg`. This file is where I have all my AWS keys. To succesfully connect to an S3 bucket and obtain all datasets, you need to have valid keys for your AWS account. This keys will never be exposed to the public or uploaded to your repository.
2. After all your configuration, you can run the project executing the Python File:
    ``` python etl.py```

##### Running the project on AWS Cluster

1. If you want to run this project on AWS Cluster, the most easy and straightforward way is create a Jupyter Notebook on Amazon EMR (Elastic Map Reduce)
2. For this steep, you don't need to create a SparkSession because it's already created for you and there's no necessity to declare your AWS keys too.
3. You can find a good tutorial about how to run a Jupyter Notebook on EMR in the next links:
    * [Pyspark with a Jupyter Notebook in an AWS EMR cluster](https://towardsdatascience.com/use-pyspark-with-a-jupyter-notebook-in-an-aws-emr-cluster-e5abc4cc9bdd)
    * [EMR Notebooks Documentation](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-managed-notebooks.html)

## Aditional Information

#### Start-schema

![Log Data](https://i.imgur.com/Yjowv8Y.png "Log Dataset")

#### Song dataset example

```sh
song_data/A/B/C/TRABCEI128F424C983.json
```
```json
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

#### Log dataset example

```
log_data/2018/11/2018-11-13-events.json
```
![Log Data](https://i.imgur.com/GLHfF6M.png "Log Dataset")
