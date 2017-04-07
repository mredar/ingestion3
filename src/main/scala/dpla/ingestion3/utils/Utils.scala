package dpla.ingestion3.utils

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.io.Text

import scala.xml.Node
import java.util.concurrent.TimeUnit

import scala.xml.XML

/**
  * Created by scott on 1/18/17.
  */
object Utils {

  /**
    *
    * @param fs
    * @param seqFilePath
    * @param conf
    * @return
    */
  def printSeqFile(fs: FileSystem, seqFilePath: Path, conf: Configuration): Unit = {
    val reader: Reader = new Reader(fs, seqFilePath, conf)


    val k: Text = new Text()
    val v: Text = new Text()

    try {
      while (reader.next(k, v)) {
        println(k);
      }
    } finally {
      reader.close()
    }
  }

  /**
    * Count the number of files in the given directory, outDir.
    *
    * @param outDir Directory to count
    * @param ext File extension to filter by
    * @return The number of files that match the extension
    */
 def countFiles(outDir: File, ext: String): Long = {
   outDir.list()
     .par
     .count(fileName => fileName.endsWith(ext))
 }

  /**
    * Writes an xml tree to string without any pretty formatting
    *
    * @param node An XML node
    * @return String representation of the node
    */


  def xmlToString(node: Node): String = {
    val w = new java.io.StringWriter()
    XML.write(w, node, "utf-8", xmlDecl = false, null)
    w.toString
  }

  /**
    * Formats the Node in a more human-readable form
    *
    * @param xml An XML node
    * @return Formatted String representation of the node
    */
  def formatXml(xml: Node): String ={
    val prettyPrinter = new scala.xml.PrettyPrinter(80, 2)
    prettyPrinter.format(xml).toString
  }

  /**
    * Print the results of an activity
    *
    * Example:
    *   Harvest count: 242924 records harvested
    *   Runtime: 4 minutes 24 seconds
    *   Throughput: 920 records/second
    *
    * @param runtime Runtime in milliseconds
    * @param recordCount Number of records output
    */
  def printResults(runtime: Long, recordCount: Long): Unit = {
    val formatter = java.text.NumberFormat.getIntegerInstance
    val minutes: Long = TimeUnit.MILLISECONDS.toMinutes(runtime)
    val seconds: Long = TimeUnit.MILLISECONDS.toSeconds(runtime) -
      TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(runtime))
    val runtimeInSeconds: Long = TimeUnit.MILLISECONDS.toSeconds(runtime) + 1
    // add 1 to avoid divide by zero error
    val recordsPerSecond: Long = recordCount/runtimeInSeconds

    println(s"File count: ${formatter.format(recordCount)}")
    println(s"Runtime: $minutes:$seconds")
    println(s"Throughput: ${formatter.format(recordsPerSecond)} records/second")
  }


  /**
    * Delete a directory
    * Taken from http://stackoverflow.com/questions/25999255/delete-directory-recursively-in-scala#25999465
    * @param file
    */
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

}
