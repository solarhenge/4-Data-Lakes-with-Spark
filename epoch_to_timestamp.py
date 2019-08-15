from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime

get_timestamp = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 

from pandas import DataFrame
epoch = {'ts': [1542795222796,1542798065796,1542825241796,1542833066796,1542166672796]}
df = DataFrame(epoch,columns=['ts'])
print(df)
#df = df.withColumn("timestamp", get_timestamp(df.ts))
df = df.withColumn('timestamp', get_timestamp(df[ts]))
#print(df)
