import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.time.{Duration, LocalDateTime}
import java.time.format.DateTimeFormatter
import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.util.{Failure, Success, Try}

/**
 * Doi tuong thong tin task
 *
 * @param jobId             ma job
 * @param taskId            id cua task
 * @param taskName          ten task
 * @param dataSourceName    thong tin source
 * @param dataDesName       thong tin dich
 * @param query             cau lenh lay du lieu hoac tranform du lieu neu typeTask la L
 * @param typeTask          co 3 loai task :E(Lay du lieu),T(tranform du lieu),L(luu du lieu)
 * @param typeSource        hien tai xu ly cho 2 loai "postgres" va "oracle"
 * @param typeDestination   hien tai xu ly cho 2 loai "postgres" va "oracle"
 * @param taskOrder         thu tu chay cua task
 * @param listOptionSource    cac tham so tuy chinh cho task doc
 * @param listOptionDes    cac tham so tuy chinh cho task ghi
 * @param modeSave          che do luu doi voi typeTask la L(2 mode la "append" va "overwrite")
 * @param sparkViewType     loai view trong sparkSQL(2 loai :"tempview" va "global")
 * @param sparkTempViewName ten tempview trong sparkSQL
 * @param taskLevel         level cua task , level 1 la task chinh, level2 la task con
 * @param taskGroup         'Y' neu co task con,mac dinh la 'N'
 * @param taskIdParent      task id cha neu task group la 'Y'
 */
case class TaskInfo(jobId: Long, taskId: Long, taskName: String, dataSourceName: String,
                    dataDesName: String, query: String, typeTask: String, typeSource: String,
                    typeDestination: String, taskOrder: Int, listOptionSource: Map[String, String],
                    modeSave: String, sparkViewType: String, sparkTempViewName: String,
                    taskLevel: Int, taskGroup: String, taskIdParent: Long,listOptionDes:Map[String,String]
                   )

/**
 * Chua doi tuong thong tin ket noi cac moi truong
 *
 * @param sourceName ten moi truong
 * @param sourceType
 * @param sourceValue
 */
case class DataSourceInfo(sourceName: String, sourceType: String, sourceValue: String)

/**
 * Chua doi tuong thong tin log
 *
 * @param id          id event se do airflow day vao khi submit
 * @param startTime   thoi gian bat dau task
 * @param endTime     thoi gian ket thuc task
 * @param duration    thoi gian thuc hien
 * @param logLevel    loai log:ERROR,WARNING,INFO
 * @param logMessage  noi dung log
 * @param statusTask  trang thai cua task: SUCCESS or ERROR
 * @param resultCount ket qua so dong thuc hien
 */
case class LogObject(id: String, jobId: Long, taskId: Long, taskName: String, startTime: String,
                     endTime: String, duration: Double,
                     logLevel: String, logMessage: String,
                     statusTask: String, resultCount: Long
                    )

/** Lop lay du lieu cac task cua job va dieu khien cac task
 *
 * @param id_job        job id to get infomation of job
 * @param listParameter parameter duoc truyen tu ben ngoai nhu spark submit or airflow
 */
class JobControl(spark: SparkSession, id_job: Int, listParameter: Array[String]) {
  private val _spark: SparkSession = spark
  private val _id_job: Int = id_job
  private val _listParameter: Array[String] = listParameter
  private val config = ConfigFactory.load()
  private val sparkDbConfig = config.getConfig("db_config")
  val listDbConfig: Map[String, String] = Map.apply(
    "url" -> sparkDbConfig.getString(("url")),
    "user" -> sparkDbConfig.getString("user"),
    "password" -> sparkDbConfig.getString("password")
  )
  val formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")

  def getInfoDataSource: DataFrame = {
    val query: String = sparkDbConfig.getString("queryDataSource")
    val listOptionDbConfig = listDbConfig ++ Map("query" -> query)

    DatabaseProcessor.apply(_spark, "postgres").loadData(listOptionDbConfig)
  }

  /**
   * lay ra thong tin cac task cua job
   *
   * @return danh sach cac task can thuc hien
   */
  def getListTask: DataFrame = {
    val query: String = sparkDbConfig.getString("queryConfigTask") + _id_job
    val listOptionDbConfig: Map[String, String] = Map.apply(
      "url" -> sparkDbConfig.getString(("url")),
      "user" -> sparkDbConfig.getString("user"),
      "password" -> sparkDbConfig.getString("password"),
      "query" -> query
    )
    DatabaseProcessor.apply(_spark, "postgres").loadData(listOptionDbConfig)
  }

  /**
   * chia task de chay, quan ly cac task, ghi log thoi gian chay cua task, loi cua task
   */
  def controlListTask(): Unit = {

    //lay cac thong tin can de chay
    val idEvent: String = _listParameter(0) //ID cua he thong airflow sinh ra va duoc day vao khi submit
    val dataListTask = getListTask.collect().map(x => mapRowToTask(x))
    val maxOrderTask = dataListTask.map(x => x.taskOrder).max

    for (stt <- 1 to maxOrderTask) {

      dataListTask.filter(x => x.taskOrder == stt && x.taskLevel == 1)
        .toList.par.foreach(x => {

          //kiem tra xem buoc nay co phai group task khong, neu phai thi chay cac danh sach subtask
          if (x.taskGroup == "Y") {
            val dataListSubTask = dataListTask.filter(y => y.taskLevel == 2 && y.taskIdParent == x.taskId)
            val maxOrSubTask = dataListSubTask.map(z => z.taskOrder).max
            for (sttSub <- 1 to maxOrSubTask) {
              dataListSubTask.filter(s => s.taskOrder == sttSub)
                .toList.par.foreach(subtask => {
                  controlTask(idEvent, subtask)
                })
            }
          }
          else {
            controlTask(idEvent, x)
          }

        })
    }
  }

  /**
   * ham lay thong tin source, chay task, ghi log cua task va subtask
   *
   * @param idEvent        id he thong
   * @param dataSourceInfo du lieu cac nguon datasource
   * @param taskI          Thong tin task
   */
  def controlTask(idEvent: String, taskI: TaskInfo): Unit = {

    //    val sourceI = dataSourceInfo.filter(y => y.sourceName == taskI.dataSourceName)
    //      .map(info => info.sourceType -> info.sourceValue).toMap
    val dataSourceInfo = getInfoDataSource.collect().map(x => mapRowToSource(x))

    //ghi lai thoi gian bat dau chay cua task
    val startDateTime = LocalDateTime.now()

    val resultTask = runTask(taskI, dataSourceInfo) match {
      case Success(i) => {
        //ghi log thanh cong
        val endDateTime = LocalDateTime.now() //.format(formatter)
        writeLog(new LogObject(idEvent, taskI.jobId, taskI.taskId, taskI.taskName,
          startDateTime.format(formatter), endDateTime.format(formatter),
          Duration.between(startDateTime, endDateTime).getSeconds(), "INFO",
          "", "SUCCESS", i
        ))
        i
      }
      case Failure(exception) => {
        //ghi log loi
        writeLog(new LogObject(idEvent, taskI.jobId, taskI.taskId, taskI.taskName,
          startDateTime.format(formatter), "",
          0, "ERROR", exception.getMessage, "FAILED"
          , -1
        ))
        -1
      }
    }
  }

  /**
   * ham chay cac task
   *
   * @return doi voi typeTask la E thi la so dong lay ra tu source
   *         doi voi typeTask la T thi la so dong cua tempview ket qua
   *         doi voi typeTask la L thi la so dong se duoc luu
   */
  def runTask(taskInfo: TaskInfo, dataSourceInfo: Array[DataSourceInfo]): Try[Long] = Try {
    //phan loai task
    val queryCount = "select count(*) from "

    //lay thong tin source
    val sourceInfo = dataSourceInfo.filter(y => y.sourceName == taskInfo.dataSourceName)
      .map(info => info.sourceType -> info.sourceValue).toMap
    //lay thong tin Des
    val desInfo = dataSourceInfo.filter(y => y.sourceName == taskInfo.dataDesName)
      .map(info => info.sourceType -> info.sourceValue).toMap

    taskInfo.typeTask match {
      //case lay du lieu
      case "E" => {
        //task lay du lieu
        val dataTask = DatabaseProcessor.apply(spark, taskInfo.typeSource)
          .loadData(sourceInfo ++ taskInfo.listOptionSource ++ Map("query" -> taskInfo.query))

        //tao tempview
        taskInfo.sparkViewType match {
          case "global" => dataTask.createGlobalTempView(taskInfo.sparkTempViewName)
          case "tempview" => dataTask.createTempView(taskInfo.sparkTempViewName)
        }
        val queryCountResult = queryCount + taskInfo.sparkTempViewName
        spark.sql(queryCountResult).first().getLong(0)
      }

      //case tranform du lieu trong cac tempview
      case "T" => {
        taskInfo.sparkViewType match {
          case "global" => spark.sql(taskInfo.query).createTempView(taskInfo.sparkTempViewName)
          case "tempview" => spark.sql(taskInfo.query).createTempView(taskInfo.sparkTempViewName)
        }

        val queryCountResult = queryCount + taskInfo.sparkTempViewName
        spark.sql(queryCountResult).first().getLong(0)
      }

      //case luu du lieu vao source
      case "L" => {
        taskInfo.typeSource match {
          case "sparksql" => {
            val rsQuery = spark.sql(taskInfo.query)
            DatabaseProcessor.apply(spark, taskInfo.typeDestination)
              .writeData(rsQuery, desInfo ++ taskInfo.listOptionDes, taskInfo.modeSave)
            rsQuery.count()
          }
          case _ => {
            val dataTask = DatabaseProcessor.apply(spark, taskInfo.typeSource)
              .loadData(sourceInfo ++ taskInfo.listOptionSource ++ Map("query" -> taskInfo.query))
            DatabaseProcessor.apply(spark, taskInfo.typeDestination)
              .writeData(dataTask, desInfo ++ taskInfo.listOptionDes, taskInfo.modeSave)
            dataTask.count()
          }
        }

      }
    }
  }

  /**
   *
   * @param data du lieu truyen vao
   * @return object TaskInfo
   */
  def mapRowToTask(taskFirst: Row): TaskInfo = {
    new TaskInfo(taskFirst.getLong(0),
      taskFirst.getLong(1),
      taskFirst.getString(2),
      taskFirst.getString(3),
      taskFirst.getString(4),
      taskFirst.getString(5),
      taskFirst.getString(6),
      taskFirst.getString(7),
      taskFirst.getString(8),
      taskFirst.getInt(9),
      convertStringToListOption(taskFirst.getString(10)),
      taskFirst.getString(11),
      taskFirst.getString(12),
      taskFirst.getString(13),
      taskFirst.getInt(14),
      taskFirst.getString(15),
      taskFirst.getLong(16),
      convertStringToListOption(taskFirst.getString(17))
    )
  }

  def mapRowToSource(sourceIn: Row): DataSourceInfo = {
    new DataSourceInfo(sourceIn.getString(0),
      sourceIn.getString(1),
      sourceIn.getString(2)
    )
  }

  def convertStringToListOption(stringOption: String): Map[String, String] = {
    var listOptions: Map[String, String] = Map.empty[String, String]
    if (stringOption == null || stringOption.isEmpty) {
      listOptions
    }
    else {
      listOptions = stringOption.split(",").map { pair =>
        val Array(key, value) = pair.split(":")
        key -> value
      }.toMap
    }

    listOptions
  }

  def writeLog(logInfo: LogObject) = {
    val data = Seq(logInfo)
    val columns = Seq("id", "jobid", "taskid", "taskname", "starttime", "endtime", "duration", "loglevel", "logmessage", "statustask", "resultcount")
    val df: DataFrame = _spark.createDataFrame(data).toDF(columns: _*)

    val listOptionDbConfig = listDbConfig ++ Map("dbtable" -> "bvh.spark_log_job")
    DatabaseProcessor.apply(_spark, "postgres").writeData(df, listOptionDbConfig, "append")
  }
}