import sys
import matplotlib.pyplot as plt
from visualization import visulation_df
from age_crash_analysis import age_crash_analysis

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as f
    from pyspark.sql.types import IntegerType
    print("Successfully imported Spark Modules")

except ImportError as e:
    print("Can not import Spark Modules", e)
    sys.exit(1)


def main():
    spark = SparkSession.\
        builder.\
        appName(" Chicago Chrash Analysis").\
        getOrCreate()
    people_crash_df = spark.read.csv(path="dataset/Traffic_Crashes_-_People.csv",
                                     header=True)
    crash_df = spark.read.csv(path="dataset/Traffic_Crashes_-_Crashes.csv",
                                     header=True)
    vehicle_crash_df = spark.read.csv(path="dataset/Traffic_Crashes_-_Vehicles.csv",
                                     header=True)
    # print(people_crash_df.select('AGE').where(people_crash_df["AGE"].isNotNull()).count())
    # print(crash_df.count())
    # print(vehicle_crash_df.count())
    # print(crash_df.join(vehicle_crash_df, crash_df['RD_NO'] == vehicle_crash_df['RD_NO']).count())

    # model vs fatal
    # crash_df.join(vehicle_crash_df, crash_df['RD_NO'] == vehicle_crash_df['RD_NO']).select('MODEL', 'INJURIES_FATAL',
    #                                                                                        'INJURIES_TOTAL').show()

    # 1. accident vs age
    # 2. number of total driver by age
    age_crash_analysis(crash_df, people_crash_df)





if __name__ == '__main__':
    main()
