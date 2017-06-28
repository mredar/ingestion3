package dpla.ingestion3

import java.io.File

import dpla.ingestion3.utils.Utils
import com.databricks.spark.avro._
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


/**
  * Entry point for running an OAI harvest
  *
  * args Output directory: String
  *             OAI URL: String
  *             Metadata Prefix: String oai_dc oai_qdc, mods, MODS21 etc.
  *             OAI Verb: String ListRecords, ListSets etc.
  *             Provider: Provider name (we need to standardize this)
  */
object OaiHarvesterMain {

  val schemaStr =
    """{
        "namespace": "la.dp.avro",
        "type": "record",
        "name": "OriginalRecord.v1",
        "doc": "",
        "fields": [
          {"name": "id", "type": "string"},
          {"name": "document", "type": "string"},
          {"name": "provider", "type": "string"},
          {"name": "mimetype", "type": { "name": "MimeType",
           "type": "enum", "symbols": ["application_json", "application_xml", "text_turtle"]}
           }
        ]
      }
    """//.stripMargin // TODO we need to template the document field so we can record info there

  val logger = LogManager.getLogger(OaiHarvesterMain.getClass)

  def main(args: Array[String]): Unit = {

    validateArgs(args)
    println(schemaStr)

    val outputFile = args(0)
    val endpoint = args(1)
    val metadataPrefix = args(2)
    val verb = args(3)
    val provider = args(4)

    /*
      If ListSets is supported by the endpoint and sets are not passed in then
      call some method that will return a String(?) how the fuck is the supposed to work?
      Shouldn't sets be an array or list? Why is this a Option[String] value?

      Okay, the answer is the String is expected to contain set names separated by a comma
      and then in OaiRelation it is split on the comma to create a list and then that list
      is used to harvest sets in parallel

      So my function needs to call ListSets until depleted and return a List of Strings


      To harvest all sets the fif parameter should be provided but empty, "".

     */
    val sets: Option[String] = if (args.isDefinedAt(5)) Some(args(5)) else None

    Utils.deleteRecursively(new File(outputFile))

    val sparkConf = new SparkConf().setAppName("Oai Harvest").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val start = System.currentTimeMillis()


    /**
      * Figure out wtf to do with sets
    */

    val setsToHarvest = sets match {
      // To indicate that all sets should be harvested an empty string must
      // be passed in via command line args. This will 
      case Some("") => {
        val setOptions = Map( "metadataPrefix" -> metadataPrefix, "verb" -> "ListSets")

        val setResults = spark.read
          .format("dpla.ingestion3.harvesters.oai")
          .options(setOptions)
          .load(endpoint)

        import spark.implicits._

        val setArr: Array[String] = setResults.map(f => f.getString(0)).collect()

        setArr.foreach( println(_) )

      }
      case Some(sets) => {
        println("Get sets from param")
      }
      case _ => println("Use listRecords")
    }

    val baseOptions = Map( "metadataPrefix" -> metadataPrefix, "verb" -> verb)

    val readerOptions = getReaderOptions(baseOptions, sets)

    val results = spark.read
      .format("dpla.ingestion3.harvesters.oai")
      .options(readerOptions)
      .load(endpoint)

    results.persist(StorageLevel.DISK_ONLY)

    val dataframe = results.withColumn("provider", lit(provider))
      .withColumn("mimetype", lit("application_xml"))

    val recordsHarvestedCount = dataframe.count()

    // This task may require a large amount of driver memory.
    dataframe.write
      .format("com.databricks.spark.avro")
      .option("avroSchema", schemaStr)
      .avro(outputFile)

    sc.stop()

    val end = System.currentTimeMillis()

    Utils.printResults((end-start),recordsHarvestedCount)
  }

  def validateArgs(args: Array[String]) = {
    // Complains about not being typesafe...
    if(args.length < 5 || args.length > 6) {
      logger.error("Bad number of arguments passed to OAI harvester. Expecting:\n" +
        "\t<OUTPUT AVRO FILE>\n" +
        "\t<OAI URL>\n" +
        "\t<METADATA PREFIX>\n" +
        "\t<OAI VERB>\n" +
        "\t<PROVIDER>\n" +
        "\t<SETS> (optional)")
      sys.exit(-1)
    }
  }

  def getReaderOptions(baseOptions: Map[String, String],
                       sets: Option[String]): Map[String, String] = {
    sets match {
      case Some(sets) => baseOptions + ("sets" -> sets)
      case None => baseOptions
    }
  }
}
