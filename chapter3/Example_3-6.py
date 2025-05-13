from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import findspark
findspark.init()

# DDL을 사용하여 스키마 정의
schema = "'Id' INT, 'First' STRING, 'Last' STRING, 'Url' STRING, 'Published' STRING, 'Hits' INT, 'Campaigns' ARRAY<STRING>"

# 프로그래밍 스키마 정의
schema = StructType([
   StructField("Id", IntegerType(), False),
   StructField("First", StringType(), False),
   StructField("Last", StringType(), False),
   StructField("Url", StringType(), False),
   StructField("Published", StringType(), False),
   StructField("Hits", IntegerType(), False),
   StructField("Campaigns", ArrayType(StringType()), False)])

#기본 데이터 생성
data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
       [2, "Brooke","Wenig","https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
       [3, "Denny", "Lee", "https://tinyurl.3","6/7/2019",7659, ["web", "twitter", "FB", "LinkedIn"]],
       [4, "Tathagata", "Das","https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
       [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
       [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
      ]

# main program
if __name__ == "__main__":
   #SparkSession 생성
   spark = (SparkSession
       .builder
       .appName("Example-3_6")
       .getOrCreate())
   
   # 위에 정의했던 스키마로 데이터 프레임 생성
   blogs_df = spark.createDataFrame(data, schema)

   # 데이터 프레임 내용을 보여준다. 위에서 만든 데이터를 보여주게 된다
   blogs_df.show()
   print()
   
   # 데이터 프레임 처리에 사용된 스키마를 출력한다
   print(blogs_df.printSchema())