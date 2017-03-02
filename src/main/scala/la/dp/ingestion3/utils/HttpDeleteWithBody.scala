package la.dp.ingestion3.utils

/**
  * Created by Scott on 3/2/17.
  * Taken from Stackoverflow on needing to pass body to HTTP DELETE call
  */

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase
import java.net.URI
import org.apache.http.annotation.NotThreadSafe

@NotThreadSafe object HttpDeleteWithBody {
  val METHOD_NAME = "DELETE"
}

@NotThreadSafe class HttpDeleteWithBody() extends HttpEntityEnclosingRequestBase {
  override def getMethod = HttpDeleteWithBody.METHOD_NAME

  def this(uri: String) {
    this()
    setURI(URI.create(uri))
  }

  def this(uri: URI) {
    this()
    setURI(uri)
  }
}