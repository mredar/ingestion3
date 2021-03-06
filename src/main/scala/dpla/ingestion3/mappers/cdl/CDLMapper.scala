package dpla.ingestion3.mappers.cdl


import dpla.ingestion3.mappers.rdf._
import org.eclipse.rdf4j.model._


class CDLMapper(document: CDLDocument) extends MappingUtils {

  def map(): Model = {
    assert(document.urlItem.isDefined)

    val itemUrlPrefix = "https://thumbnails.calisphere.org/clip/150x150/"
    val providerLabel = "California Digital Library"
    val cdlUri = iri("http://dp.dpla/api/contributor/cdl")

    val thumbnail = document.imageMD5 match {
      case Some(md5) => Some(iri(itemUrlPrefix + md5))
      case _ => None
    }

    registerNamespaces(defaultVocabularies)
    mapItemWebResource(iri(document.urlItem.get))
    mapContributingAgent(cdlUri, providerLabel)

    if (thumbnail.isDefined)
      mapThumbWebResource(thumbnail.get)

    val aggregatedCHO = mapAggregatedCHO(ChoData(
      dates = mapDates(document.dates.distinct),
      titles = mapStrings(document.titles.distinct),
      identifiers = mapStrings(document.identifiers.distinct),
      rights = mapStrings(document.rights.distinct),
      collections = mapStrings(document.collectionNames.distinct),
      contributors = mapStrings(document.contributors.distinct),
      creators = mapStrings(document.creators.distinct),
      publishers = mapStrings(document.publishers.distinct),
      types = mapStrings(document.types.distinct)
    ))

    mapAggregation(AggregationData(
      aggregatedCHO = aggregatedCHO,
      isShownAt = iri(document.urlItem.get),
      preview = thumbnail,
      provider = cdlUri,
      originalRecord = mapOriginalRecord(),
      dataProvider = mapDataProvider(providerLabel)
    ))

    build()
  }
}
