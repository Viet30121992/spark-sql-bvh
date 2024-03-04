import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

import java.util.UUID

object Main {
  def main(args: Array[String]): Unit = {
    runMain(args)
//    try{
//      Success(runMain(args))
//    } catch {
//      case e:Exception => println(s"[ERROR_PROGRAM] "+e)
//    }

  //  test()
  }

  def runMain(args: Array[String]):Unit =  {
    val config = ConfigFactory.load()
    val sparkConfig = config.getConfig("spark")

    // Create a Spark session
    val spark: SparkSession = SparkSession.builder
      .appName(sparkConfig.getString("appName"))
      .master(sparkConfig.getString("master"))
      .config("spark.sql.warehouse,dir","/hive/warehouse")
      .getOrCreate()

    val stringRandom: String = UUID.randomUUID().toString
    val argsInput:Array[String]=Array(stringRandom)

    val test=new JobControl(spark,1,argsInput)
    test.controlListTask()
  }
}
