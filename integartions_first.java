import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType, DoubleType}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.time.format.DateTimeFormatter


// ============================================================================================
// Mapping cashboxNumbers -> parsed address
val cashboxParsedUri = f"mongodb://{mongo_crd.login}:{mongo_crd.pwd}@mp-park/Content.CashboxParsed?authSource=admin"
val cashboxParsedReadConfig = ReadConfig(Map("uri" -> CashboxParsedUri))

var cashboxParsed = MongoSpark
    .load(spark, cashboxParsedReadConfig)
    .withColumn("address", $"AddressCandidate".getItem(0))
    .drop($"AddressCandidate")
    .select($"ChangeId",
            $"address")

val addresses = cashboxParsed
    .withColumn("maxFromTime", max("cashboxChangeId.fromTime").over(Window.partitionBy("cashboxChangeId.cashboxRegNumber")))
    .where($"cashboxChangeId.fromTime" === $"maxFromTime")
    .cache()


val yesterday = java.time.LocalDate.now.minusDays(1).format(DateTimeFormatter.ofPattern("yyyy.MM.dd"))
val yesterdayFileName = java.time.LocalDate.now.minusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
var dataPath = f"/data/Cheques/date=$yesterday%s"


var products = spark
    .read
    .parquet(dataPath)
    .withColumn("chequeId", concat($"content.cashboxNumber", lit("_"), $"content.number", lit("_"), col("content.timestamp")))
    .dropDuplicates("chequeId")
    .withColumn("product", explode($"content.products"))
    .drop($"content.products")
    .filter(($"content.calculationType" === "Sell") && ($"type" === "Receipt"))
    .withColumn("nameLower", lower($"product.name"))
    .withColumn("skuCategory", when($"nameLower".rlike("acuvu|а[кk]ув.?[юу]") && $"nameLower".rlike("revit|ревит"), "ACUVUE® RevitaLens")
                              .when($"nameLower".rlike("acuvu|а[кk]ув.?[юу]") && !$"nameLower".rlike("revit|ревит"), "ACUVUE®")
                              .otherwise(null))
    .filter("skuCategory IS NOT NULL")
    // join address
    .join(addresses, $"content.cashboxNumber"===$"cashbox.cashboxNumber", "left_outer")
    .withColumn("regionCode", substring($"address.region.code", 0, 2))
    .withColumn("regionName", $"address.region.name")
    .withColumn("cityName", $"address.city.name")
    .withColumn("salesPointId",  concat_ws("_", coalesce($"address.street.fiasGuid", lit("")),
                                                coalesce($"address.house", lit("")),
                                                coalesce($"content.clientInn", lit(""))
                                            ))
    .withColumn("date", to_date($"content.timestamp"))
    .withColumn("sumInRub", round($"product.sum" / 100, 2))
    .select($"date",
            $"content.client",
            $"regionCode",
            $"regionName",
            $"cityName",
            $"salesPointAddress",
            $"skuCategory",
            $"nameLower",
            $"product.quantity")

var overallStat = products
    .groupBy($"date",
             $"client",
             $"regionCode",
             $"regionName",
             $"cityName",
             $"salesPointAddress",
             $"skuCategory",
             $"nameLower")
    .agg(round(sum($"quantity"),2) as "quantity")

var reportStatPath = f"/reports/contact_lenses/daily_report/contact_lenses_sales_$yesterdayFileName%s.csv"

overallStat
    .coalesce(1)
    .write
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("escape", "\"")
    .mode("overwrite")
    .csv(reportStatPath)

spark.sparkContext.stop()

