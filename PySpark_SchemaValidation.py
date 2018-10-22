================================================================================================================================
BELOW Code Will Read The Data From File With Defined Schema And Rows Which Fail Schema Check Will be Moved to new Column named as _corrupt_record
================================================================================================================================
from pyspark.sql import *
from pyspark.sql.types import *

def mapColumnType(InputType):
  dataType=StringType()
  if(InputType=="IntegerType"):
    dataType=IntegerType()
  return dataType
  
  
columnDetails=["User-ID;IntegerType","Location;StringType","Age;IntegerType","_corrupt_record;StringType"]
columnSchemaList=[]
for column in columnDetails:
  columnName=column.split(";")[0]
  CoulmDataType=column.split(";")[1]
  print(columnName+" "+CoulmDataType)
  columnSchemaList.append(StructField(columnName,mapColumnType(CoulmDataType),True))

DFSchema=StructType(columnSchemaList)


sparksession= SparkSession.builder.appName("Test Python Spark").getOrCreate()
print(sparksession)
dimaondData=sparksession.read.format("csv").option("delimiter",';').option("header","true").schema(DFSchema).load("/FileStore/tables/BX_Users_With_Corrupt-8bb29.csv")

dimaondData.show()

================================================================================================================================********************************** Equivalent Lambda Code  To Create Schema ****************************************************
================================================================================================================================

from pyspark.sql import *
from pyspark.sql.types import *

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
dimaondData.show()

>>> DFSchema 

Output:
StructType(List(StructField(User-ID,IntegerType,true),StructField(Location,StringType,true),StructField(Age,IntegerType,true),StructField(_corrupt_record,StringType,true)))
