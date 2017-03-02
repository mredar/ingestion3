package la.dp.ingestion3.indexer

import java.net.URL

import la.dp.ingestion3.utils.HttpDeleteWithBody
import org.apache.http.HttpEntity
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder

/**
  * Created by Scott on 3/2/17.
  */
object DeleterMain {

  /**
    * Accepts a timestamp and provider.@id value
    * Deletes all records in ES of that provider with an
    * earlier timestamp
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val date = args(0)
    val provider = args(1)
    val endpoint = new URL(args(2))

    // Build the URL
    val urlParams = new URIBuilder()
      .setScheme(endpoint.getProtocol)
      .setHost(endpoint.getHost)
      .setPort(endpoint.getPort)
      .setPath(endpoint.getPath)

    val post = new HttpDeleteWithBody(urlParams.build())
    val client = HttpClientBuilder.create().build()
    val deleteQuery: HttpEntity = new StringEntity(
      s"""
        |{
        |  "range": {
        |    "index_timestamp": {
        |      "lt": ${date}
        |    }
        |  },
        |  "term": {
        |     "provider.@id": ${provider}
        |  }
        |}
      """.stripMargin)

    post.setEntity(deleteQuery)

    try {
      client.execute(post)
    } finally {
      client.close()
    }
  }
}
