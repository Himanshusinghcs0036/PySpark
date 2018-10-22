from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

columnDetails=["User-ID;IntegerType","Location;StringType","Age;IntegerType","_corrupt_record;StringType"]
columnSchemaList=[]

mapCol= lambda x : IntegerType() if x=="IntegerType" else StringType()
parseColDetails=lambda x : columnSchemaList.append(StructField(x.split(";")[0], mapCol(x.split(";")[1]), True))

for item in columnDetails:
  parseColDetails(item)

DFSchema=StructType(columnSchemaList)  
sparksession= SparkSession.builder.appName("Test Python Spark").getOrCreate()
dimaondData=sparksession.read.format("csv").option("delimiter",';').option("header","true").option("nullValue","NULL").schema(DFSchema).option("mode","PERMISSIVE").load("/FileStore/tables/BX_Users-53089.csv")
#DROPMALFORMAD