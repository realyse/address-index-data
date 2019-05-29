package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row

case class HybridAddressSkinnyNisraEsDocument(uprn: Long,
                                              parentUprn: Long,
                                              lpi: Seq[Map[String, Any]],
                                              paf: Seq[Map[String, Any]],
                                              nisra: Seq[Map[String, Any]],
                                              classificationCode: Option[String],
                                              postcode: String,
                                              fromSource: String)

object HybridAddressSkinnyNisraEsDocument extends EsDocument {

  def rowToLpi(row: Row): Map[String, Any] = Map(
    "uprn" -> row.getLong(0),
    "postcodeLocator" -> row.getString(1),
    "addressBasePostal" -> row.getString(2),
    "location" -> row.get(3),
    "easting" -> row.getFloat(4),
    "northing" -> row.getFloat(5),
    "parentUprn" -> (if (row.isNullAt(6)) null else row.getLong(6)),
    "paoStartNumber" -> (if (row.isNullAt(16)) null else row.getShort(16)),
    "paoStartSuffix" -> row.getString(17),
    "saoStartNumber" -> (if (row.isNullAt(21)) null else row.getShort(21)),
    "lpiLogicalStatus" -> row.getByte(27),
    "streetDescriptor" -> splitAndCapitalise(row.getString(30)),
    "lpiStartDate" -> row.getDate(34),
    "lpiEndDate" -> row.getDate(36),
    "nagAll" -> concatNag(
      if (row.isNullAt(21)) "" else row.getShort(21).toString,
      if (row.isNullAt(23)) "" else row.getShort(23).toString,
      row.getString(24), row.getString(22), row.getString(20), row.getString(11),
      if (row.isNullAt(16)) "" else row.getShort(16).toString,
      row.getString(17),
      if (row.isNullAt(18)) "" else row.getShort(18).toString,
      row.getString(19), row.getString(15), row.getString(30),
      row.getString(31), row.getString(32), row.getString(1)
    ),
    "mixedNag" -> generateFormattedNagAddress(
      if (row.isNullAt(21)) "" else row.getShort(21).toString,
      row.getString(22),
      if (row.isNullAt(23)) "" else row.getShort(23).toString,
      row.getString(24),
      splitAndCapitalise(row.getString(20)),
      splitAndCapitalise(row.getString(11)),
      if (row.isNullAt(16)) "" else row.getShort(16).toString,
      row.getString(17),
      if (row.isNullAt(18)) "" else row.getShort(18).toString,
      row.getString(19),
      splitAndCapitalise(row.getString(15)),
      splitAndCapitalise(row.getString(30)),
      splitAndCapitalise(row.getString(32)),
      splitAndCapitaliseTowns(row.getString(31)),
      row.getString(1)
    )
  )

  def rowToPaf(row: Row): Map[String, Any] = Map(
    "uprn" -> row.getLong(3),
    "startDate" -> row.getDate(25),
    "endDate" -> row.getDate(26),
    "pafAll" -> concatPaf(Option(row.getString(23)).getOrElse(""),
      if (row.isNullAt(9)) "" else row.getShort(9).toString,
      Option(row.getString(10)).getOrElse(""),
      Option(row.getString(18)).getOrElse(""),
      Option(row.getString(11)).getOrElse(""),
      Option(row.getString(19)).getOrElse(""),
      Option(row.getString(6)).getOrElse(""),
      Option(row.getString(5)).getOrElse(""),
      Option(row.getString(7)).getOrElse(""),
      Option(row.getString(8)).getOrElse(""),
      Option(row.getString(12)).getOrElse(""),
      Option(row.getString(20)).getOrElse(""),
      Option(row.getString(13)).getOrElse(""),
      Option(row.getString(21)).getOrElse(""),
      Option(row.getString(14)).getOrElse(""),
      Option(row.getString(22)).getOrElse(""),
      Option(row.getString(15)).getOrElse("")),
    "mixedPaf" -> generateFormattedPafAddress(
      Option(row.getString(23)).getOrElse(""),
      if (row.isNullAt(9)) "" else row.getShort(9).toString,
      Option(row.getString(10)).getOrElse(""),
      splitAndCapitalise(Option(row.getString(11)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(6)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(5)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(7)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(8)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(12)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(13)).getOrElse("")),
      splitAndCapitaliseTowns(Option(row.getString(14)).getOrElse("")),
      Option(row.getString(15)).getOrElse("")
    ),
    "mixedWelshPaf" -> generateWelshFormattedPafAddress(
      Option(row.getString(23)).getOrElse(""),
      if (row.isNullAt(9)) "" else row.getShort(9).toString,
      splitAndCapitalise(Option(row.getString(18)).getOrElse(Option(row.getString(10)).getOrElse(""))),
      splitAndCapitalise(Option(row.getString(19)).getOrElse(Option(row.getString(11)).getOrElse(""))),
      splitAndCapitalise(Option(row.getString(6)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(5)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(7)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(8)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(20)).getOrElse(Option(row.getString(12)).getOrElse(""))),
      splitAndCapitalise(Option(row.getString(21)).getOrElse(Option(row.getString(13)).getOrElse(""))),
      splitAndCapitaliseTowns(Option(row.getString(22)).getOrElse(Option(row.getString(14)).getOrElse(""))),
      Option(row.getString(15)).getOrElse("")
    )
  )

  def rowToNisra(row: Row): Map[String, Any] = {
    val nisraFormatted: Array[String] = generateFormattedNisraAddresses(
      Option(row.getString(1)),
      Option(row.getString(2)),
      Option(row.getString(3)),
      Option(row.getString(4)),
      Option(row.getString(5)),
      Option(row.getString(6)),
      Option(row.getString(7)),
      Option(row.getString(8)),
      Option(row.getString(9)),
      Option(row.getString(10)),
      Option(row.getString(11)))

    Map(
      "uprn" -> row.getLong(0),
      "buildingNumber" -> row.getString(4),
      "easting" -> row.getFloat(12),
      "northing" -> row.getFloat(13),
      "location" -> row.get(14),
      "creationDate" -> row.getDate(15),
      "commencementDate" -> row.getDate(16),
      "archivedDate" -> row.getDate(17),
      "mixedNisra" -> nisraFormatted(0),
      "mixedAltNisra" -> nisraFormatted(1),
      "nisraAll" -> nisraFormatted(2),
      "postcode" -> row.getString(11)
    )
  }

  def generateFormattedNisraAddresses(organisationName: Option[String], subBuildingName: Option[String],
                                      buildingName: Option[String], buildingNumber: Option[String],
                                      thoroughfare: Option[String], altThoroughfare: Option[String],
                                      dependentThoroughfare: Option[String], locality: Option[String],
                                      townLand: Option[String], townName: Option[String], postcode: Option[String]
                                     ): Array[String] = {
    val numberAndStreetDesc =
      buildingNumber.flatMap(num =>
        dependentThoroughfare.map(depTho =>
          s"${num.toUpperCase} ${splitAndCapitalise(depTho)}"))

    val names = Seq(
      organisationName.map(splitAndCapitalise),
      subBuildingName.map(splitAndCapitalise),
      buildingName.map(splitAndCapitalise),
      numberAndStreetDesc
    ).flatten.filter(_.nonEmpty)

    val area = Seq(
      locality.map(splitAndCapitaliseTowns),
      townLand.map(splitAndCapitaliseTowns),
      townName.map(splitAndCapitaliseTowns),
      postcode.map(_.toUpperCase)
    ).flatten.filter(_.nonEmpty)

    val mixedNisra = (names.iterator ++ thoroughfare.map(splitAndCapitalise).iterator ++ area.iterator).mkString(", ")
    val mixedAltNisra = altThoroughfare
      .map(_ => (names.iterator ++ altThoroughfare.map(splitAndCapitalise).iterator ++ area.iterator).mkString(", "))
      .getOrElse("")
    val nisraAll = Seq(organisationName, subBuildingName, buildingName, buildingNumber, dependentThoroughfare,
      thoroughfare, altThoroughfare, locality, townLand, townName, postcode)
      .flatten.filter(_.nonEmpty).mkString(" ")

    Array(mixedNisra, mixedAltNisra, nisraAll)
  }
}
