package dpla.ingestion3.enrichments

import dpla.ingestion3.mappers.rdf.{DCMIType, ISO_639_3}

import scala.io.Source


trait VocabEnforcer[T] {
  val enforceVocab: (T, Set[T]) => Boolean = (value, vocab) => {
    vocab.contains(value)
  }

  val matchAbbvToTerm: (T, Map[T,T]) => String = (value, vocab) => {
    vocab.getOrElse(value, value)
  }
}

object DcmiTypeEnforcer extends VocabEnforcer[String] {
  val dcmiType = DCMIType()

  val DcmiTypeStrings = Set(
    dcmiType.Collection,
    dcmiType.Dataset,
    dcmiType.Event,
    dcmiType.Image,
    dcmiType.InteractiveResource,
    dcmiType.MovingImage,
    dcmiType.PhysicalObject,
    dcmiType.Service,
    dcmiType.Software,
    dcmiType.Sound,
    dcmiType.StillImage,
    dcmiType.Text
  ).map(_.getLocalName)

  val enforceDcmiType: (String) => Boolean = enforceVocab(_, DcmiTypeStrings)
}

object LexvoEnforcer extends VocabEnforcer[String] {
  val iso_639_3 = ISO_639_3()

  val lexvoStrings = {
    // TODO make this a config property
    val bufferedSource = Source.fromFile("./data/iso-639-3/iso-639-3.tab")
    val lines = bufferedSource.getLines
    bufferedSource.close

    lines
      .map(_.split("\t"))
      .map(f => (f(0), f(1)))
      .toMap
  }

  val enforceLexvoType: (String) => String = matchAbbvToTerm(_, lexvoStrings)


}