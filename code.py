from pyspark.sql import SparkSession
from pyspark import SparkContext, SQLContext
from pyspark.sql import *
import json

### Constants for Configurations ###
appName = "CovidDataAnalysis"
config = "spark.sql.warehouse.dir"
path = "file:///C:/Users/User/Desktop/Spark-WareHouse"
wareHousePath = "file:///C:/Users/User//Desktop/Spark-WareHouse/DataWareHouse"
dataBaseName = "covid"
tableName = "covidpatients"
dbTable = dataBaseName + "." + tableName

### Initializing Spark Session , SparkContext and SQLContext ###
spark = SparkSession.builder.appName(appName).config(config, path).config("hive.exec.dynamic.partition", "true").config(
    "hive.exec.dynamic.partition.mode", "nonstrict").config("hive.exec.max.dynamic.partitions",
                                                            "2048").enableHiveSupport().getOrCreate()
sparkContext = spark.sparkContext
sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)

### Checking the Existence of database and Tables ### 
database = sqlContext.sql(" SHOW DATABASES LIKE '%s' " % (dataBaseName))
if database.rdd.isEmpty():
    sqlContext.sql("CREATE DATABASE %s " % (dataBaseName))
    databases = sqlContext.sql("show databases")
    databases.show()
print("Database Is Present....")
table = sqlContext.sql(" SHOW TABLES FROM %s LIKE '%s' " % (dataBaseName, tableName))
if table.rdd.isEmpty():
    sqlContext.sql(
        "create table %s.%s(patientnumber string,agebracket string,gender string,contractedfromwhichpatientsuspected string,currentstatus string,estimatedonsetdate string,statepatientnumber string,detecteddistrict string,detectedcity string,source1 string,source2 string,source3 string,typeoftransmission string,entryid string,statuschangedate string,backupnotes string,notes string,nationality string,numcases string,statecode string) partitioned by(dateannounced string,detectedstate string) location '%s'" % (
        dataBaseName, tableName, wareHousePath))
    print("Completed TableCreation....")
    ### Loading the data from File System and sample Data Cleaning###
    with open("CovidDataset.json", "r") as read_file:
        CovidData = json.load(read_file)
    dataFrame = sparkContext.parallelize(CovidData).map(lambda data: json.dumps(data))
    dataFrame = spark.read.json(dataFrame)
    dataFrame.rdd.map(lambda data: data.replace("'", ""))
    dataFrame.rdd.map(lambda data : if(len(data["statuschangedate"]) == 0) data["statuschangedate"] = data["dateannounced"] )
    dataFrame.printSchema()
    ### Loading data into partition SPARK-SQL Table ###
    dataFrame.select("patientnumber", "agebracket", "gender", "contractedfromwhichpatientsuspected", "currentstatus",
                     "estimatedonsetdate", "statepatientnumber", "detecteddistrict", "detectedcity", "source1",
                     "source2", "source3", "typeoftransmission",
                     "entryid", "statuschangedate", "backupnotes", "notes", "nationality", "numcases", "statecode",
                     "dateannounced",
                     "detectedstate").write.mode('append').insertInto(dbTable)
### Data Analysis - Seperating Data for each date,state with number of cases, recovered and dead ###
data = sqlContext.sql(""" select data.dateannounced,data.detectedstate,data.cases, (select count(*) from covid.covidpatients where statuschangedate = data.dateannounced and detectedstate = data.detectedstate  and currentstatus="Recovered") as Recovered ,(select count(*) from covid.covidpatients where statuschangedate = data.dateannounced and data.detectedstate = detectedstate and currentstatus="Deceased") as Deceased  from covid.covidpatients join (SELECT dateannounced,detectedstate,count(currentstatus) as cases  FROM covid.covidpatients group by dateannounced,detectedstate) as data on data.dateannounced = covid.covidpatients.dateannounced group by data.dateannounced,data.detectedstate,data.cases,Recovered,Deceased """)
data.write.csv("output.csv")
