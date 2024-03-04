import org.apache.spark.sql.{SparkSession, DataFrame}

trait DatabaseProcessor {
  def loadData(listOption: Map[String, String]): DataFrame

  def writeData(dataFrame: DataFrame, listOption: Map[String, String],saveMode:String): Unit
}

object DatabaseProcessor {
  private class PostgresProcessor(spark: SparkSession) extends DatabaseProcessor {
    private val _spark: SparkSession = spark

    override def loadData(listOption: Map[String, String]): DataFrame = {
      _spark.read.format("jdbc")
        //.option("driver", "org.postgresql.Driver")
        .options(listOption).load()
    }

    override def writeData(dataFrame: DataFrame, listOption: Map[String, String],saveMode:String): Unit = {
      dataFrame.write.mode(saveMode)
        .format("jdbc")
        .options(listOption)
        .save()
    }
  }

  private class OracleProcessor(spark: SparkSession) extends DatabaseProcessor {
    private val _spark: SparkSession = spark

    override def loadData(listOption: Map[String, String]): DataFrame = {
      _spark.read
        .format("jdbc")
        //.option("driver", "oracle.jdbc.driver.OracleDriver")
        .options(listOption).load()
    }

    override def writeData(dataFrame: DataFrame, listOption: Map[String, String],saveMode:String): Unit = {
      dataFrame.write.mode(saveMode)
        .format("jdbc")
        .options(listOption)
        .save()
    }
  }

  def apply(spark: SparkSession, typeDatabase: String): DatabaseProcessor = {
    typeDatabase match {
      case "postgres" => new PostgresProcessor(spark)
      case "oracle" => new OracleProcessor(spark)
    }
  }
}
