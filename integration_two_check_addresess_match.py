import uuid
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.functions import expr

options = {
    "sep": "\u0002",
    "header": "true",
    "lineSep": "\u0001",
    "inferSchema": "true",
    "nullValue": "NULL",
    "treatEmptyValuesAsNulls": "true"
}

logical_date = datetime.datetime.strptime("20230722", "%Y%m%d")

dateList = [logical_date + datetime.timedelta(days=x) for x in range(7)]
dateStrings = [date.strftime("%Y%m%d") for date in dateList]
processed = spark.read.options(**options).csv(
    ["/integrations/days/" + x + "_processed.csv" for x in dateStrings])

week_number = logical_date.strftime("%W")
weekSku = spark.read.options(**options).csv(f"/integrations/weeks/{logical_date.year}{week_number}_week_sku.csv")

# if one week contains two different months
start_date = logical_date
end_date = start_date + datetime.timedelta(days=6)
x = []
if start_date.month == end_date.month:
    x.append(start_date.replace(month=start_date.month - 1).strftime("%Y%m"))
else:
    x.append(start_date.replace(month=start_date.month - 1).strftime("%Y%m"))
    x.append(end_date.replace(month=end_date.month - 1).strftime("%Y%m"))


catalog = (spark.read.options(**options).csv(["/integrations/months/" + i + ".csv" for i in x])
                .withColumn("date_catalog",regexp_extract(input_file_name(), r"(\d+).csv", 1))
                .withColumn("full_address", lower(col("FullAddress"))))


bricks = (spark.read.options(**options).csv(["/integrations/months/" + i + "_bricks.csv" for i in x]) 
               .withColumn("date_brick", regexp_extract(input_file_name(), r"(\d+)_bricks.csv", 1)))


cheques = (spark.read.parquet("/data/OfdCheques/date=2023.07.{22,23,24,25,26,27,28}")
                .filter(col("content.calculationType") == "Sell")
                .withColumn("product", explode("content.products"))
                .withColumn("nameLower", lower(col("product.name")))
                .join(processed, col("nameLower") == lower(col("CheckItem")), "inner")
                .join(weekSku, ["SKUId"], "inner")
                .withColumn("date", to_date(col("content.timestamp")))
                .withColumn("date_bricks_catalog", date_format(add_months(to_date(col("content.timestamp"), "yyyy-MM-dd"), -1), "yyyyMM"))
                .dropDuplicates()
                .select(["date_bricks_catalog", "cashboxNumber", "SKUDI"]))


cashboxParsedAddres = (spark.read.format('com.mongodb.spark.sql.DefaultSource')
                            .option('uri', f'mongodb://{mongo_cd.login}:{mongo_cd.pwd}@host/Content.CashboxParsed?authSource=admin')
                            .load()
                            .filter(col("cashboxId.fromTime") <= "2023-07-22")
                            .withColumn("AddressCandidate", col("AddressCandidates").getItem(0))
                            .drop("AddressCandidates"))


cashboxRegAddresses = cashboxParsedAddres \
                      .withColumn("maxFromTime", max("cashboxId.fromTime").over(Window.partitionBy("cashboxId.cashboxNumber"))) \
                      .where(col("cashboxId.fromTime") == col("maxFromTime")) \
                      .withColumn("distr_name", lower(col("AddressCandidate.District.Name"))) \
                      .withColumn("street_name", lower(regexp_replace(col("AddressCandidate.Street.Name"), "ё", "е"))) \
                      .withColumn("house", lower(col("AddressCandidate.House"))) \
                      .withColumn("region_name", lower(col("AddressCandidate.Region.Name"))) \
                      .withColumn("city_name", lower(col("AddressCandidate.City.Name"))) \
                      .select(["cashboxId.cashboxNumber", "ClientId", "house", "distr_name", "street_name", "region_name", "city_name"])


CashboxPharId = cashboxRegAddresses \
                .join(catalog, (col("ClientId") == col("EntityINN"))
                             & (col("house").isNotNull() & col("full_address").contains(col("house")))
                             & (col("street_name").isNotNull() & col("full_address").contains(col("street_name")))

                             & (col("distr_name").isNotNull() & col("full_address").contains(col("distr_name")))
                             | (col("distr_name").isNotNull() & lower(col("City")).contains(col("distr_name")))

                             | (col("city_name").isNotNull() & lower(col("City")).contains(col("city_name")))
                             | (col("city_name").isNotNull() & col("full_address").contains(col("city_name")))
                             | (col("city_name").isNotNull() & lower(col("District")).contains(col("city_name")))
                     
                             | (col("region_name").isNotNull() & col("full_address").contains(col("region_name")))
                             | (col("region_name").isNotNull() & lower(col("City")).contains(col("region_name")))
                , "inner") \
                .join(bricks, ["PharId"], "inner") \
                .filter(col("date_catalog") == col("date_brick")) \
                .select(["cashboxNumber", "PharId", "BrickIdDistribution", "date_brick"]) \
                .dropDuplicates()


report = cheques \
         .join(CashboxPharId, ["cashboxNumber"], "inner") \
         .filter(col("date_bricks_catalog") == col("date_brick")) \
         .groupBy("BrickIdDistribution", "SKUDI") \
         .agg(countDistinct("PharId").cast("integer").alias("Distribution")) \
         .select("BrickIdDistribution", "SKUDI", "Distribution")
