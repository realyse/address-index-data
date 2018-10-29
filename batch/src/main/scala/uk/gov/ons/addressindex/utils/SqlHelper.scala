package uk.gov.ons.addressindex.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import uk.gov.ons.addressindex.models.HybridAddressEsDocument
import uk.gov.ons.addressindex.readers.AddressIndexFileReader

/**
  * Join the Csv files into single DataFrame
  */
object SqlHelper {

  def joinCsvs(blpu: DataFrame, lpi: DataFrame, organisation: DataFrame, street: DataFrame,
               streetDescriptor: DataFrame, historical: Boolean = true): DataFrame = {

    val blpuTable =
      if (historical) {
        SparkProvider.registerTempTable(blpu, "blpu")
      } else {
        val blpuNoHistory = SparkProvider.registerTempTable(blpu, "blpuNoHistory")
        val blpuNoHistoryDF = SparkProvider.sqlContext.sql(s"""SELECT b.* FROM $blpuNoHistory b WHERE b.logicalStatus != 8""")
        SparkProvider.registerTempTable(blpuNoHistoryDF, "blpu")
      }
    val organisationTable = SparkProvider.registerTempTable(organisation, "organisation")
    val lpiTable =
      if (historical) {
        SparkProvider.registerTempTable(lpi, "lpi")
      } else {
        val lpiNoHistory = SparkProvider.registerTempTable(lpi, "lpiNoHistory")
        val lpiNoHistoryDF = SparkProvider.sqlContext.sql(s"""SELECT l.* FROM $lpiNoHistory l WHERE l.logicalStatus != 8""")
        SparkProvider.registerTempTable(lpiNoHistoryDF, "lpi")
      }
    val streetTable = SparkProvider.registerTempTable(street, "street")
    val streetDescriptorTable = SparkProvider.registerTempTable(streetDescriptor, "street_descriptor")

    SparkProvider.sqlContext.sql(
      s"""SELECT
        $blpuTable.uprn,
        $blpuTable.postcodeLocator,
        $blpuTable.addressbasePostal as addressBasePostal,
        array($blpuTable.longitude, $blpuTable.latitude) as location,
        $blpuTable.xCoordinate as easting,
        $blpuTable.yCoordinate as northing,
        $blpuTable.parentUprn,
        $blpuTable.multiOccCount,
        $blpuTable.logicalStatus as blpuLogicalStatus,
        $blpuTable.localCustodianCode,
        $blpuTable.rpc,
        $organisationTable.organisation,
        $organisationTable.legalName,
        $lpiTable.usrn,
        $lpiTable.lpiKey,
        $lpiTable.paoText,
        $lpiTable.paoStartNumber,
        $lpiTable.paoStartSuffix,
        $lpiTable.paoEndNumber,
        $lpiTable.paoEndSuffix,
        $lpiTable.saoText,
        $lpiTable.saoStartNumber,
        $lpiTable.saoStartSuffix,
        $lpiTable.saoEndNumber,
        $lpiTable.saoEndSuffix,
        $lpiTable.level,
        $lpiTable.officialFlag,
        $lpiTable.logicalStatus as lpiLogicalStatus,
        $lpiTable.usrnMatchIndicator,
        $lpiTable.language,
        $streetDescriptorTable.streetDescriptor,
        $streetDescriptorTable.townName,
        $streetDescriptorTable.locality,
        $streetTable.streetClassification,
        $lpiTable.startDate as lpiStartDate,
        $lpiTable.lastUpdateDate as lpiLastUpdateDate,
        $lpiTable.endDate as lpiEndDate
      FROM $blpuTable
      LEFT JOIN $organisationTable ON $blpuTable.uprn = $organisationTable.uprn
      LEFT JOIN $lpiTable ON $blpuTable.uprn = $lpiTable.uprn
      LEFT JOIN $streetTable ON $lpiTable.usrn = $streetTable.usrn
      LEFT JOIN $streetDescriptorTable ON $streetTable.usrn = $streetDescriptorTable.usrn
      AND $lpiTable.language = $streetDescriptorTable.language""").na.fill("")
  }

  /**
    * Aggregates data forming lists of siblings and their parents per level of the hierarchy
    * (grouped by root uprn)
    * @param hierarchy hierarchy data
    * @return dataframe containing layers/levels of hierarchy
    */
  def aggregateHierarchyInformation(hierarchy: DataFrame): DataFrame ={
    val hierarchyTable = SparkProvider.registerTempTable(hierarchy, "hierarchy")

    SparkProvider.sqlContext.sql(
      s"""SELECT
            primaryUprn,
            thisLayer as level,
            collect_list(uprn) as siblings,
            collect_list(parentUprn) as parents
          FROM
            $hierarchyTable
          GROUP BY primaryUprn, thisLayer
       """
    )
  }

  def aggregateCrossRefInformation(crossRef: DataFrame): DataFrame = {
    val crossRefTable = SparkProvider.registerTempTable(crossRef, "crossRef")

    SparkProvider.sqlContext.sql(
      s"""SELECT
            uprn,
            crossReference,
            source
          FROM
            $crossRefTable
          GROUP BY uprn, crossReference, source
       """
    )
  }

  def aggregateClassificationsInformation(classifications: DataFrame): DataFrame = {
    val classificationsTable = SparkProvider.registerTempTable(classifications, "classifications")

    SparkProvider.sqlContext.sql(
      s"""SELECT
            uprn,
            classificationCode,
            classScheme
          FROM
            $classificationsTable
          WHERE
            classScheme = 'AddressBase Premium Classification Scheme'
          GROUP BY uprn, classificationCode, classScheme
       """
    )
  }

  /**
    * Constructs a hybrid index from nag and paf dataframes
    */
  def aggregateHybridIndex(paf: DataFrame, nag: DataFrame, historical: Boolean = true): RDD[HybridAddressEsDocument] = {

    // If non-historical there could be zero lpis associated with the PAF record since historical lpis were filtered
    // out at the joinCsvs stage. These need to be removed.
    val pafGrouped =
      if (historical) {
        paf.groupBy("uprn").agg(functions.collect_list(functions.struct("*")).as("paf"))
      } else {
        paf.join(nag, Seq("uprn"), joinType = "leftsemi")
          .select("recordIdentifier", "changeType", "proOrder", "uprn", "udprn", "organisationName", "departmentName",
            "subBuildingName", "buildingName", "buildingNumber", "dependentThoroughfare", "thoroughfare",
            "doubleDependentLocality", "dependentLocality", "postTown", "postcode", "postcodeType", "deliveryPointSuffix",
            "welshDependentThoroughfare", "welshThoroughfare", "welshDoubleDependentLocality", "welshDependentLocality",
            "welshPostTown", "poBoxNumber", "processDate", "startDate", "endDate", "lastUpdateDate", "entryDate")
          .groupBy("uprn").agg(functions.collect_list(functions.struct("*")).as("paf"))
      }

    // DataFrame of lpis by uprn
    val nagGrouped = nag
      .groupBy("uprn")
      .agg(functions.collect_list(functions.struct("*")).as("lpis"))

    // DataFrame of paf and lpis by uprn
    val pafNagGrouped = nagGrouped.join(pafGrouped, Seq("uprn"), "left_outer")

    // DataFrame of CrossRefs by uprn
    val crossRefGrouped = aggregateCrossRefInformation(AddressIndexFileReader.readCrossrefCSV())
      .groupBy("uprn")
      .agg(functions.collect_list(functions.struct("crossReference", "source")).as("crossRefs"))

    // DataFrame of Classifications by uprn
    val classificationsGrouped = aggregateClassificationsInformation(AddressIndexFileReader.readClassificationCSV())
      .groupBy("uprn")
      .agg(functions.collect_list(functions.struct("classificationCode", "classScheme")).as("classifications"))

    // Construct Hierarchy DataFrame
    val hierarchyDF = AddressIndexFileReader.readHierarchyCSV()

    val hierarchyGrouped = aggregateHierarchyInformation(hierarchyDF)
      .groupBy("primaryUprn")
      .agg(functions.collect_list(functions.struct("level", "siblings", "parents")).as("relatives"))

    val hierarchyJoined = hierarchyDF
      .join(hierarchyGrouped, Seq("primaryUprn"), "left_outer")
      .select("uprn", "parentUprn", "relatives")

    val pafNagCrossHierGrouped = pafNagGrouped
      .join(crossRefGrouped, Seq("uprn"), "left_outer")
      .join(hierarchyJoined, Seq("uprn"), "left_outer")
      .join(classificationsGrouped, Seq("uprn"), "left_outer")

    pafNagCrossHierGrouped.rdd.map {
      row =>
        val uprn = row.getAs[Long]("uprn")
        val paf = Option(row.getAs[Seq[Row]]("paf")).getOrElse(Seq())
        val lpis = Option(row.getAs[Seq[Row]]("lpis")).getOrElse(Seq())
        val crossRefs = Option(row.getAs[Seq[Row]]("crossRefs")).getOrElse(Seq())
        val relatives = Option(row.getAs[Seq[Row]]("relatives")).getOrElse(Seq())
        val classifications = Option(row.getAs[Seq[Row]]("classifications")).getOrElse(Seq())
        val parentUprn = Option(row.getAs[Long]("parentUprn"))

        val outputLpis = lpis.map(row => HybridAddressEsDocument.rowToLpi(row))
        val outputPaf = paf.map(row => HybridAddressEsDocument.rowToPaf(row))
        val outputCrossRefs = crossRefs.map(row => HybridAddressEsDocument.rowToCrossRef(row))
        val outputRelatives = relatives.map(row => HybridAddressEsDocument.rowToHierarchy(row))
        val outputClassifications = classifications.map(row => HybridAddressEsDocument.rowToClassification(row))

        val classificationCode: Option[String] = outputClassifications.headOption.flatMap(_.get("classificationCode").map(_.toString))

        val lpiPostCode: Option[String] = outputLpis.headOption.flatMap(_.get("postcodeLocator").map(_.toString))
        val pafPostCode: Option[String] = outputPaf.headOption.flatMap(_.get("postcode").map(_.toString))

        val postCode = if (pafPostCode.isDefined) pafPostCode.getOrElse("")
        else lpiPostCode.getOrElse("")

        val splitPostCode = postCode.split(" ")
        val (postCodeOut, postCodeIn) =
          if (splitPostCode.size == 2 && splitPostCode(1).length == 3) (splitPostCode(0), splitPostCode(1))
          else ("", "")

        HybridAddressEsDocument(
          uprn,
          postCodeIn,
          postCodeOut,
          parentUprn.getOrElse(0L),
          outputRelatives,
          outputLpis,
          outputPaf,
          outputCrossRefs,
          classificationCode
        )
    }
  }
}