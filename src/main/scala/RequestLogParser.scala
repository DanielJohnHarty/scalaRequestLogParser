import java.io.{BufferedWriter, File, FileWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import spray.json.DefaultJsonProtocol.{LongJsonFormat, StringJsonFormat, mapFormat, seqFormat, tuple2Format}
import spray.json.enrichAny


object RequestLogParser {

  case class IpReport(ip: String, ipAccesses:Long)
  val ipReportEncoder = Encoders.product[IpReport]

  case class UriReport(uri: String, uriAccesses:Long)
  val uriReportEncoder = Encoders.product[UriReport]

  case class FilteredDate(datestamp: String)
  val filteredDateEncoder = Encoders.product[FilteredDate]

  case class AccessLog( ip: String,
                        ident:String,
                        user:String,
                        timestamp:String,
                        request:String,
                        status:String,
                        size :String,
                        referrer:String,
                        userAgent:String,
                        unk:String,
                        method:String,
                        uri:String,
                        http:String,
                        datestamp:String,
                      )
  val accessLogEncoder = Encoders.product[AccessLog]


  def main(args: Array[String]): Unit = {
    val compressedLogPath = "./src/main/resources/access.log.gz" // Relative from project root
    createReport(compressedLogPath)
  }

  private def toAccessLog(params: List[String]): AccessLog =
      AccessLog(params(0),
                params(1),
                params(2),
                params(3),
                params(4),
                params(5),
                params(6),
                params(7),
                params(8),
                params(9),
                params(9),
                params(9),
                params(9),
                ""
                )

  private def writeJsonReportToFile(jsonString:String): Unit = {
    val reportPath = "./jsonReport_"+getCurrentDateStamp()+".json"
    val file = new File(reportPath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(jsonString)
    bw.close()
  }

  private def getCurrentDateStamp():String = {
    DateTimeFormatter.ofPattern("yyyyMMdd_HHmm").format(LocalDateTime.now)
  }

  private def GetDatesSurpassingAccessThreshold(threshold: Long, ss:SparkSession): Seq[String] = {
    val sql: String = s"""select datestamp, count(*) as accessCount from AccessLogExt group by datestamp having count(*) > $threshold order by accessCount desc"""
    ss.sql(sql).as[FilteredDate](filteredDateEncoder)
      .rdd
      .collect
      .map((fd:FilteredDate)=>fd.datestamp)
      .toSeq
  }

  private def getDateIpJsonReport(datestamp: String, ss: SparkSession): Array[(String, Long)] = {
    val sql = s"""select datestamp, ip, ipAccesses from IpAccessData where datestamp = '$datestamp' """
    ss.sql(sql).as[IpReport](ipReportEncoder)
      .rdd.collect
      .map((ip:IpReport)=> (ip.ip -> ip.ipAccesses))
  }

  private def getDateUriJsonReport(datestamp: String, ss: SparkSession): Array[(String, Long)] = {
    val sql = s"""select datestamp, uri, uriAccesses from UriAccessData where datestamp = '$datestamp' """
    ss.sql(sql).as[UriReport](uriReportEncoder)
      .rdd.collect
      .map((ur:UriReport)=> (ur.uri -> ur.uriAccesses))
  }

  private def createReport(gzPath: String ): Unit = {
    val ss = getSparkSession()
    val dsExtended: Dataset[AccessLog] = getDsExtended(gzPath, ss)
    dsExtended.createOrReplaceTempView("AccessLogExt")
    val ipAccessData: DataFrame = {
      val sql: String = "select datestamp, ip, count(ip) as ipAccesses from AccessLogExt group by datestamp, ip"
      ss.sql(sql)
    }
    val uriAccessData: DataFrame = {
      val sql: String = "select datestamp, uri, count(uri) as uriAccesses from AccessLogExt group by datestamp, uri"
      ss.sql(sql)
    }
    ipAccessData.createOrReplaceTempView("IpAccessData")
    uriAccessData.createOrReplaceTempView("UriAccessData")
    val theDates: Seq[String] = GetDatesSurpassingAccessThreshold(20000, ss)
    val reports = theDates.map((datestamp: String) => getDateReport(ss, datestamp))
    writeJsonReportToFile(reports.toJson.toString)
    ss.close()
  }

  private def getDateReport(ss: SparkSession, datestamp: String): Map[String, (Map[String, Map[String,Long]],Map[String, Map[String,Long]])]= {
    val ipReport = Map[String, Map[String, Long]]("ipAccessCount" -> getDateIpJsonReport(datestamp, ss).toMap)
    val uriReport = Map[String, Map[String, Long]]("uriAccessCount" -> getDateUriJsonReport(datestamp, ss).toMap)
    val dateReport = Map[String, (Map[String, Map[String, Long]], Map[String, Map[String, Long]])](datestamp -> (ipReport, uriReport))
    return dateReport
  }

  private def getDsExtended(gzPath: String, sparkSession: SparkSession) = {
    val ds: Dataset[AccessLog] = generateDatasetOfAccesslogs(gzPath, sparkSession)
    val dsWithTime: Dataset[AccessLog] = addNormalisedDatetime(ds)
    val dsExtended: Dataset[AccessLog] = addRequestComponents(dsWithTime)
    dsExtended
  }

  private def addRequestComponents(dsWithTime: Dataset[AccessLog]): Dataset[AccessLog] = {
    // Regex to split the request string into its component parts
    val REQ_EX = "([^ ]+)[ ]+([^ ]+)[ ]+([^ ]+)".r
    // Add columns to the dataset for the individual components of the request string. Drop request string column afterwards.
    val dsExtended = dsWithTime
      .withColumn("method", regexp_extract(dsWithTime("request"), REQ_EX.toString, 1))
      .withColumn("uri", regexp_extract(dsWithTime("request"), REQ_EX.toString, 2))
      .withColumn("http", regexp_extract(dsWithTime("request"), REQ_EX.toString, 3))
    dsExtended.as[AccessLog](accessLogEncoder)
  }

  private def addNormalisedDatetime(ds: Dataset[AccessLog]) = {
    val dsWithTimestamp: DataFrame = ds.withColumn("timestamp", to_timestamp(ds("timestamp"), "dd/MMM/yyyy:HH:mm:ss X"))
    val dsWithTimestampAndDatestamp: DataFrame = dsWithTimestamp.withColumn("datestamp", split(col("timestamp")," ").getItem(0))
    dsWithTimestampAndDatestamp.as[AccessLog](accessLogEncoder)
  }

  private def generateDatasetOfAccesslogs(gzPath: String, ss: SparkSession) = {
    import ss.implicits._
    val R = """^(?<ip>[0-9.]+) (?<identd>[^ ]) (?<user>[^ ]) \[(?<datetime>[^\]]+)\] \"(?<request>[^\"]*)\" (?<status>[^ ]*) (?<size>[^ ]*) \"(?<referer>[^\"]*)\" \"(?<useragent>[^\"]*)\" \"(?<unk>[^\"]*)\"""".r
    val x: DataFrame = ss.read.text(gzPath) // Read from source
    val y: Dataset[String] = x.map(_.getString(0)) // Cast as strings
    val z: Dataset[List[String]] = y.flatMap(x => R.unapplySeq(x)) // Convert strings to Array[String] of regex extracted parameters
    val ds: Dataset[AccessLog] = z.map(toAccessLog _) // Convert items to access log instances
    ds
  }

  private def getSparkSession(master: String="local[*]"): SparkSession = {
    val ss: SparkSession = SparkSession.builder().appName("RequestLogParser")
      .master(master).getOrCreate()
    return ss
  }
}

