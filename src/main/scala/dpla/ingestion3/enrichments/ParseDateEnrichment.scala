package dpla.ingestion3.enrichments

import java.text.SimpleDateFormat

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import ParseDateEnrichment._

class ParseDateEnrichment {

  //TODO ranges
  def parse(dateString: String, allowInterval: Boolean = false): Option[String] = {
    val str = preprocess(dateString)
    lazy val interval = if (allowInterval) parseInterval(str) else None
    lazy val parseDateVal = parseDate(str)
    //    date ||= Date.edtf(str.gsub('.', '-')) //todo
    lazy val partialEdtfVal = partialEdtf(str)
    lazy val decadeHyphenVal = decadeHyphen(str)
    lazy val monthYearVal = monthYear(str)
    lazy val decadeStrVal = decadeString(str)
    lazy val hyphenatedPartialRangeVal = hyphenatedPartialRange(str)

    lazy val circaVal = circa(str)

    Seq(
      interval,
      parseDateVal,
      partialEdtfVal,
      decadeHyphenVal,
      monthYearVal,
      decadeStrVal,
      hyphenatedPartialRangeVal,
      circaVal
    ).collectFirst({case Some(x: String) => x})
  }

  private def preprocess(str: String): String = {

    val removedLateAndEarly =
      str.replaceAll("[lL]ate", "").replaceAll("[eE]arly", "")
        .trim
        .replaceAll("\\s+", " ")

    val removedDecades =
      if (removedLateAndEarly.matches("""^[1-9]+0s$"""))
        removedLateAndEarly.replaceAll("0s", "x")
      else removedLateAndEarly

    val removedRanges =
      if (removedDecades.matches("""^[1-9]+\-+$"""))
        removedDecades.replaceAll("-", "x")
      else removedDecades

    removedRanges
  }

  private def rangeMatch(str: String): Option[(String, String)] = {
    val cleanedString = str.replace("to", "-").replace("until", "-")
    rangeMatchRexp.findFirstMatchIn(cleanedString) match {
      case Some(matched) => Some((matched.group(1), matched.group(2)))
      case None => None
    }
  }

  private def circa(str: String): Option[String] = {
    val cleaned = str.replaceAll(""".*[cC]{1}[irca\.]*""", "").replaceAll(""".*about""", "")
    parseDate(cleaned) match { //todo i removed recusrion by changing parse() to parseDate().
      case Some(date) => Some(cleaned)
      case None => None
    }
    //todo EDTF stuff
  }


  private def parseInterval(str: String): Option[(String, String)] = {
    //todo parse the dates from the range?
    rangeMatch(str) match {
      case Some((begin, end)) =>
        (parseDate(begin), parseDate(end)) match {
          case (Some(b), Some(e)) => Some(b, e)
          case _ => None
        }
      case None => None
    }
  }

  //todo why is this getting called twice
  private def parseDate(str: String): Option[String] = {
    @tailrec
    def innerParseDate(str: String, formats: List[SimpleDateFormat]): Option[String] =
      formats match {
        case head :: rest =>
          Try(head.parse(str)) match {
            case Success(date) => Some(responseFormat.format(date))
            case Failure(exception) => innerParseDate(str, rest)
          }
        case Nil => None
    }

    innerParseDate(str, trialFormats)
  }

  private def monthYear(str: String): Option[String] = {
    monthYearRexp.findFirstMatchIn(str) match {
      case Some(matched) =>
        Some("%s-%s".format(matched.group(2), matched.group(1)))
      case None => None
    }
  }

  private def hyphenatedPartialRange(str: String): Option[String] = {
    hyphenatedPartialRangeRegexp.findFirstMatchIn(str) match {
      case Some(matched) =>
        Some("%s-%s/%s-%s".format(matched.group(1), matched.group(2), matched.group(1), matched.group(3)))
      case None => None
    }
  }

  private def partialEdtf(str: String): Option[String] = {
    partialEdtfRegexp.findFirstMatchIn(str) match {
      case Some(matched) =>
        Some("%s-%s/%s-%s".format(matched.group(1), matched.group(3), matched.group(1), matched.group(4)))
      case None => None
    }
  }

  private def decadeString(str: String): Option[String] = {
    decadeStringRegexp.findFirstMatchIn(str) match {
      case Some(matched) => Some(matched.group(1) + "x")
      case None => None
    }
  }

  private def decadeHyphen(str: String): Option[String] = {
    decadeHyphenRegexp.findFirstMatchIn(str) match {
      case Some(matched) => Some(matched.group(1) + "x")
      case None => None
    }
  }
}

object ParseDateEnrichment {
  val responseFormat = new SimpleDateFormat("yyyy-MM-dd")

  val trialFormats = List(
    new SimpleDateFormat("yyyy-MM-dd"),
    new SimpleDateFormat("MMM dd, yyyy"),
    new SimpleDateFormat("MM/dd/yyyy"),
    new SimpleDateFormat("MM.dd.yyyy"),
    new SimpleDateFormat("MM-dd-yyyy"),
    new SimpleDateFormat("MMM, yyyy")
  )
  trialFormats.foreach(_.setLenient(false))

  val rangeMatchRexp = """([a-zA-Z]{0,3}\s?[\d\-\/\.xu\?\~a-zA-Z]*,?\s?\d{3}[\d\-xs][s\d\-\.xu\?\~]*)\s*[-\.]+\s*([a-zA-Z]{0,3}\s?[\d\-\/\.xu\?\~a-zA-Z]*,?\s?\d{3}[\d\-xs][s\d\-\.xu\?\~]*)""".r

  val monthYearRexp = """^(\d{2})-(\d{4})$""".r

  val hyphenatedPartialRangeRegexp = """^(\d{2})(\d{2})-(\d{2})$""".r

  val partialEdtfRegexp = """^(\d{4}(-\d{2})*)-(\d{2})\/(\d{2})$""".r

  val decadeStringRegexp = """^(\d{3})0s$""".r

  val decadeHyphenRegexp = """^(\d{3})-$""".r
}
