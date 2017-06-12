package dpla.ingestion3.mappers.rdf

import java.net.URI

import dpla.ingestion3.model.DplaMapData.ZeroToMany
import dpla.ingestion3.model.DplaMapData.ZeroToOne
import org.eclipse.rdf4j.model.IRI
import org.eclipse.rdf4j.model.Model
import org.eclipse.rdf4j.model.Resource
import org.eclipse.rdf4j.model.Value
import org.eclipse.rdf4j.model.util.ModelBuilder

trait RdfBuilderUtils extends RdfValueUtils with DefaultVocabularies {

  private val builder = new ModelBuilder()

  def registerNamespaces(vocabularies: Seq[Vocabulary]): Unit =
    for (vocabulary <- vocabularies)
      builder.setNamespace(vocabulary.ns)

  //sugar to set the subject as the resource but return a reference to the resource
  private def innerCreateResource(resource: Resource): Resource = {
    builder.subject(resource)
    resource
  }

  //convenience method for type coercion
  def createResource(uri: URI): Resource =
    innerCreateResource(iri(uri.toString))

  //convenience method to avoid passing None
  def createResource(): Resource =
    innerCreateResource(bnode())

  //convenience method for type coercion
  def createResource(option: ZeroToOne[URI]): Resource = {
    option match {
      case Some(uri) => innerCreateResource(iri(uri))
      case None => innerCreateResource(bnode())
    }
  }

  def setType(iri: IRI): Unit =
    builder.add(rdf.`type`, iri)

  def map(predicate: IRI, value: Value): Unit =
    builder.add(predicate, value)

  def map(predicate: IRI, list: ZeroToMany[_]): Unit =
    for (item <- list) item match {
      case value: URI => builder.add(predicate, iri(value))
      case value: String => builder.add(predicate, literal(value))
    }

  def map(predicate: IRI, option: ZeroToOne[_]): Unit =
    option match {
      case Some(value: URI) => builder.add(predicate, iri(value))
      case Some(value: String) => builder.add(predicate, literal(value))
      case _ => Unit
    }

  def build(): Model = builder.build()
}
