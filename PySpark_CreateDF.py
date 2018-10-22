from pyspark.sql import *
from pyspark.sql.types import *

def mapColumnType(InputType):
  dataType=StringType()
  if(InputType=="IntegerType"):
    dataType=IntegerType()
  return dataType

columnDetails=["User-ID;IntegerType","Location;StringType","Age;IntegerType"]
columnSchemaList=[]
for column in columnDetails:
  columnName=column.split(";")[0]
  CoulmDataType=column.split(";")[1]
  print(columnName+" "+CoulmDataType)
  columnSchemaList.append(StructField(columnName,mapColumnType(CoulmDataType),True))

DFSchema=StructType(columnSchemaList)

#read Data With defined Schema. Row not satisfying the Schema will be dropped. Column Name is first Row in DataFile. 
sparksession= SparkSession.builder.appName("Test Python Spark").getOrCreate()
dimaondData=sparksession.read.format("csv").option("delimiter",';').option("header","true").schema(DFSchema).option("mode", "DROPMALFORMED").load("/FileStore/tables/BX_Users-53089.csv")


================================================================================================================================
********************************************************************************************************************************
================================================================================================================================
#read Data With header information. apply new schema DFSchema to DataFrame.

sparksession= SparkSession.builder.appName("Test Python Spark").getOrCreate()
print(sparksession)
dimaondData=sparksession.read.format("csv").option("delimiter",';').option("header","true").load("/FileStore/tables/BX_Users-53089.csv")

dimaondDataWithSchema=sparksession.createDataFrame(dimaondData.rdd,DFSchema)
dimaondDataWithSchema.printSchema()
================================================================================================================================
********************************************************************************************************************************
================================================================================================================================


