import org.apache.spark.sql.{DataFrame, SparkSession}

// To see less warnings
import org.apache.log4j._

object Pipeline {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("test App")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .format("csv")
      .option("sep", "|")
      .option("quote", "\"")
      .option("escape", "\\")
      .option("endian", "little")
      .option("multiLine", "true")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("encoding", "UTF-16")
      .option("charset", "UTF-16")
      .load("src/main/resources/sb_utf16.csv")
      .withColumn("Date", $"Date".cast("timestamp"))
      .withColumn("ClosingDate", $"ClosingDate".cast("timestamp"))
      .withColumn("SettlementDate", $"SettlementDate".cast("timestamp"))
      .withColumn("LastUpdated", $"LastUpdated".cast("timestamp"))

    df.printSchema()
    df.show(10)

    
  }
}