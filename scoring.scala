

case class Params(
                  ltvHorizonLength: Int,
                  testPeriodLength: Int,
                  trainPeriodLength: Int,
                  ltvMethod: String = "one",
                  audienceSample: Double = 1.0,
                  outputPath: String,
                  dateEnd: String = LocalDate.now.minusDays(1).toString)

extends RedshiftAccess {

require(Vector("one", "two").contains(ltvMethod), s"ltvMethod param input
must be either `one` or `two`; `${ltvMethod}` is an invalid input")
// output paths
val outputPathReportingError = s"${outputPath}/reporting_error"
val outputPathModelBuilder = s"${outputPath}/model_builder"
// date variables for reporting error
val (testBdayMax, testBdayMin, trainBdayMax, trainBdayMin) =

  if (ltvMethod == "one")
  (
  LocalDate.parse(dateEnd).minusDays(ltvHorizonLength).toString,
  LocalDate.parse(dateEnd).minusDays(ltvHorizonLength +
  testPeriodLength).toString,
  LocalDate.parse(dateEnd).minusDays(ltvHorizonLength + testPeriodLength
  + ltvHorizonLength).toString,
  LocalDate.parse(dateEnd).minusDays(ltvHorizonLength + testPeriodLength
  + ltvHorizonLength + trainPeriodLength).toString)
  else
  (
  LocalDate.parse(dateEnd).minusDays(14).toString,
  LocalDate.parse(dateEnd).minusDays(14 + testPeriodLength).toString,
  LocalDate.parse(dateEnd).minusDays(14 + testPeriodLength + 14).toString,
  LocalDate.parse(dateEnd).minusDays(14 + testPeriodLength + 14 +
  trainPeriodLength).toString)

// date variables for model builder
val (modelBuilderBdayMax, modelBuilderBdayMin) =
  if (ltvMethod == "one")
    (
    LocalDate.parse(dateEnd).minusDays(ltvHorizonLength).toString,
    LocalDate.parse(dateEnd).minusDays(ltvHorizonLength +
    trainPeriodLength).toString)
  else
  (
    LocalDate.parse(dateEnd).minusDays(14).toString,
    LocalDate.parse(dateEnd).minusDays(14 + trainPeriodLength).toString)
  }







//Part 1) Create Functions
//Util Functions
def generateDateListStr(dateStart: String, dateEnd: String): Vector[String] = {
  val start = LocalDate.parse(dateStart)
  val end = LocalDate.parse(dateEnd)
  val daysDiff = DAYS.between(start, end).toInt + 1
  Vector
  .tabulate(daysDiff)(identity)
  .map(i => start.plusDays(i).toString)
}


val generateDateListCol = udf((dateStart: String, dateEnd: String) =>
generateDateListStr(dateStart, dateEnd))

val getOldUserId = udf((sid: String, account_guid: String, postal_code: String,
device_platform: String) => UserId.parse(sid, account_guid, postal_code,
device_platform))

def getCountry = udf((postal_code: String) =>
GeoMapping.inferCountryFromPostalCode(postal_code))

def unionWithRepartition(dfOne: DataFrame, dfTwo: DataFrame, resetPartitions:
Int = 600, maxPartitions: Int = 6000): DataFrame = {
  dfOne
  .union(dfTwo)
  .transform( dfUnion => {
  if (dfUnion.rdd.partitions.size >= maxPartitions)
    dfUnion.repartition(resetPartitions)
  else
    dfUnion
  })
  }



def pullUsersWithBday(bdayMin: String, bdayMax: String, ltvHorizonLength: Int):
DataFrame = {
  spark
  .read
  .parquet("s3a://flipp-datalake/data/hive_datamarts/user_info")
  .withColumn("user_id", getOldUserId(col("sid"), col("account_guid"),
  col("first_postal_code"), col("device_platform")))
  .filter(
  col("device_platform").isin("iOS", "Android") &&
  !col("user_id").isin("00000000-0000-0000-0000-000000000000",
  "daa_opt_out_sid", "unknown", "", "%3cnull%3e", "unknown_sid", "<null>",
  "null") && col("birth_date").between(lit(bdayMin), lit(bdayMax)))
  .select(col("user_id"), col("birth_date").cast(DateType).as("birth_date"))
  .distinct
  .withColumn("birth_date_plus_horizon", date_add(col("birth_date"),
  ltvHorizonLength - 1).cast(DateType))
}







implicit class DataFrameBlownUp(df: DataFrame) {

def blowUp(bdayMin: String, dateEnd: String =
LocalDate.now.minusDays(1).toString): DataFrame = {

  val dates = array(generateDateListStr(bdayMin, dateEnd).map(dateStr =>
  lit(dateStr)): _*)
  df
  .withColumn("window_end_date", explode(dates))
  .withColumn("window_end_date", col("window_end_date").cast(DateType))
  .transform( df => {
  if (df.columns.contains("rev_prediction_period_end"))
  df.filter(col("window_end_date") < col("rev_prediction_period_end")
  && col("window_end_date").between(col("birth_date"),
  date_add(col("birth_date_plus_horizon"), -1)))
  else
  df.filter(col("window_end_date").between(col("birth_date"),
  date_add(col("birth_date_plus_horizon"), -1)))
  })
  .withColumn("days_since_birth", datediff(col("window_end_date"),
  col("birth_date")))
  .withColumn("days_between_window_end_2015",
  datediff(col("window_end_date"), lit("2015-01-01")))
  }
}




implicit class DataFrameWithLTV(df: DataFrame) {
  def createRevenueDF(metricDateMin: String = null, metricDateMax: String =
    LocalDate.now.minusDays(1).toString): DataFrame = {
      spark
      .read
      .format("delta")
      .load("s3://flipp-silver-layer/pipeline/delta/user_daily_summary")
      .transform(
      df => {
        if (metricDateMin == null)
          df.filter(col("date") <= lit(metricDateMax))
        else
          df.filter(col("date").between(lit(metricDateMin),
          lit(metricDateMax)))
      }
)
    .withColumn("user_id", getOldUserId(col("sid"), col("account_guid"),
    col("postal_code"), col("device_platform")))
    .filter(
    col("device_platform").isin("iOS", "Android") &&
    !col("user_id").isin("00000000-0000-0000-0000-000000000000",
    "daa_opt_out_sid", "unknown", "", "%3cnull%3e", "unknown_sid", "<null>",
    "null"))
    .withColumn("country", getCountry(col("postal_code")))
    .withColumn("revenue", when(col("country") === lit("US"),
    col("premium_unique_engaged_visits") *
    lit(0.29)).otherwise(col("premium_unique_engaged_visits") * lit(0.31)))
    .groupBy(col("user_id"), col("date").cast(DateType).as("metric_date"))
    .agg(sum(col("revenue")).as("revenue"))
}




/**
*
*
*/

def withLTVMethodOne(metricDateMin: String = null, metricDateMax: String =
LocalDate.now.minusDays(1).toString): DataFrame = {
  val revenueDf = createRevenueDF(metricDateMin, metricDateMax)
  val dfColumns = df.columns.map(columnString =>
  col(columnString).as(columnString))
  df
  .join(revenueDf, Seq("user_id"), "left")
  .groupBy(dfColumns: _*)
  .agg(
  sum(when(col("metric_date").between(col("birth_date"),
  col("birth_date_plus_horizon")), col("revenue"))).as("label")
  )
  .withColumn("label", coalesce(col("label"), lit(0)))
  }




def withLTVMethodTwo(metricDateMin: String = null, metricDateMax: String =
LocalDate.now.minusDays(1).toString): DataFrame = {
  val revenueDf = createRevenueDF(metricDateMin, metricDateMax)
  val dfColumns = df.columns.map(columnString =>
  col(columnString).as(columnString))
  val periodsVector = Vector.range(0, 13).map(i => i.toString)
  df
  .withColumn("rev_prediction_periods", array(periodsVector.map(lit(_)):
  _*))
  .withColumn("rev_prediction_period",
  explode(col("rev_prediction_periods")))
  .withColumn("rev_prediction_period",
  col("rev_prediction_period").cast(IntegerType))
  .drop("rev_prediction_periods")
  .withColumn("rev_prediction_period_start", expr("date_add(birth_date,
  rev_prediction_period * 14)").cast(DateType))
  .withColumn(
  "rev_prediction_period_end",
  when(col("rev_prediction_period") === lit(13),
  date_add(col("rev_prediction_period_start"), 28).cast(DateType))
  .otherwise(date_add(col("rev_prediction_period_start"),
  27).cast(DateType)))
  .transform(df => if (df.columns.contains("window_end_date"))
  df.filter(col("window_end_date") < col("rev_prediction_period_end")) else df)
  .join(revenueDf, Seq("user_id"), "inner")
  .filter(col("metric_date").between(col("rev_prediction_period_start"),
  col("rev_prediction_period_end"))) // no user_ids should get filtered out
  .groupBy((df.columns.map(col(_)) ++ Array(col("rev_prediction_period"),
  col("rev_prediction_period_start"), col("rev_prediction_period_end"))) : _*)
  .agg(sum(col("revenue")).as("label"))
  .withColumn("label", coalesce(col("label"), lit(0)))
  }
}










// uds columns to pull with CUMUDS
val udsCols =
  Vector(
  "app_opens",
  "engaged_visits",
  "premium_engaged_visits",
  "unique_engaged_visits",
  // "premium_unique_engaged_visits",
  // "flyer_opens",
  // "unique_flyer_opens",
  "merchants_read",
  "favourites_count",
  "flyer_item_details",
  "flyer_item_ttm",
  "flyer_item_shares",
  "flyer_item_clippings",
  "ecom_item_details",
  "ecom_item_ttm",
  // "ecom_item_shares",
  "ecom_item_clippings",
  "coupon_listings",
  "coupon_adds",
  "coupon_opens",
  // "coupon_emails",
  "coupon_shares",
  "coupon_load_to_card",
  "shopping_list_adds",
  "shopping_list_checks",
  "shopping_list_removes",
  // "shopping_list_photo_uploads",
  // "aisle_mode_entered",
  "shopping_list_searches",
  "shopping_list_usage",
  "searches",
  "search_results",
  "search_result_clicks",
  "location_services_available",
  "cards_added"
  // "widget_view_barcode"
  )



/****** Helper Methods *********/
def aggColOnDayX(udsCol: String, i: Int): (Column, String) = {
  val name = s"uds_${udsCol}_day_${i}"
  val column = sum(when(col("metric_date") === date_add(col("birth_date"), i),
  col(udsCol))).as(name)

  (column, name)
}



def aggColBetweenBdayAndDayX(udsCol: String, i: Int): (Column, String) = {
  val name = s"uds_${udsCol}_first_days_${i}"
  val birthDatePlusX = date_add(col("birth_date"), i)
  val minDate = least(birthDatePlusX, col("window_end_date"))
  val column = sum(when(col("metric_date") <= minDate, col(udsCol))).as(name)
  (column, name)
}




def aggColBetweenLastXDays(udsCol: String, i: Int): (Column, String) = {
  val name = s"uds_${udsCol}_last_days_${i}"
  val birthDateMinusX = date_add(col("window_end_date"), -i)
  val maxDate = greatest(birthDateMinusX, col("birth_date"))
  val column = sum(when(col("metric_date") >= maxDate, col(udsCol))).as(name)
  (column, name)
}



def aggByRangeByColumn(
                      range: Vector[Int],
                      udsCols: Vector[String],
                      aggFunc: (String, Int) => (Column, String)):
  Vector[(Column, String)] = {
  udsCols
  .flatMap(
  udsCol => range.map(i => aggFunc(udsCol, i))
  )
}



def aggCumByColumn(udsCols: Vector[String]): Vector[(Column, String)] = {

  def aggFuncCum(udsCol: String): Column =
    sum(col(udsCol)).as(s"uds_${udsCol}_cum")
    udsCols.map(udsCol => (aggFuncCum(udsCol), s"uds_${udsCol}_cum"))
    }


def pullPostalToStateDF(jdbcUrl: String, tempDir: String): DataFrame = {

  val query =
  """
  select
  upper(concat(left(postal_code, 3), right(postal_code, 3))) as
  postal_code,
  province as state
  from
  public.postal_codes
  union
  select
  zip_code as postal_code,
  state
  from
  public.zip_codes
  """
  spark
  .read
  .format("com.databricks.spark.redshift")
  .option("url", jdbcUrl)
  .option("tempdir", tempDir)
  .option("forward_spark_s3_credentials", true)
  .option("query", query)
  .load()
  .withColumn("state", coalesce(col("state"), lit("Unknown")))
  .distinct
  }







implicit class DataFrameWithDimensions(df: DataFrame) {

def withStationaryUIDimensions(bdayMin: String = null, bdayMax: String =
LocalDate.now.minusDays(1).toString, jdbcUrl: String, tempDir: String):
DataFrame = {

  val postalToStateDF = pullPostalToStateDF(jdbcUrl, tempDir)
  val stationaryUIDimensions =
    spark
    .read
    .parquet("s3a://flipp-datalake/data/hive_datamarts/user_info")
    .withColumn("user_id", getOldUserId(col("sid"), col("account_guid"),
    col("first_postal_code"), col("device_platform")))
    .withColumn("country", coalesce(getCountry(col("first_postal_code")),
    lit("Unknown")))
    .withColumn("birth_year", year(col("birth_date")))
    .withColumn("birth_month", month(col("birth_date")))
    .withColumn("birth_day_of_month", dayofmonth(col("birth_date")))
    .withColumn("birth_day_of_week", dayofweek(col("birth_date")))
    .withColumn("birth_day_of_year", dayofyear(col("birth_date")))
    .withColumn("days_between_birth_2015", datediff(col("birth_date"),
    lit("2015-01-01")))
      .transform(
      df => {
        if (bdayMin == null)
          df.filter(col("birth_date") <= lit(bdayMax))
        else
          df.filter(col("birth_date").between(lit(bdayMin), lit(bdayMax)))
          }
          )
          .filter(
          col("device_platform").isin("iOS", "Android") &&
          !col("user_id").isin("00000000-0000-0000-0000-000000000000",
          "daa_opt_out_sid", "unknown", "", "%3cnull%3e", "unknown_sid", "<null>",
          "null"))
          .withColumn(
          "first_postal_code",
          when(col("country").isin("US", "Unknown"), col("first_postal_code"))
          .otherwise(expr("upper(concat(left(first_postal_code, 3),
          right(first_postal_code, 3)))")))
          .join(postalToStateDF.withColumnRenamed("postal_code",
          "first_postal_code"), Seq("first_postal_code"), "left")
          .withColumn("state", coalesce(col("state"), lit("Unknown")))
          .select(
          col("user_id"),
          col("device_platform"),
          col("country"),
          col("state"),
          col("birth_year"),
          col("birth_month"),
          col("birth_day_of_month"),
          col("birth_day_of_week"),
          col("birth_day_of_year"),
          col("days_between_birth_2015"))

        df.join(stationaryUIDimensions, Seq("user_id"), "left")
          }



def withCumUDSDimensions(
                        udsCols: Vector[String] = udsCols,
                        metricDateMin: String = null,
                        metricDateMax: String =
                        LocalDate.now.minusDays(1).toString): DataFrame = {

  val cumUDSDimensions =
  spark
  .read
  .format("delta")
  .load("s3://flipp-silver-layer/pipeline/delta/user_daily_summary")
  .transform(
  df => {
    if (metricDateMin == null)
      df.filter(col("date") <= lit(metricDateMax))
    else
      df.filter(col("date").between(lit(metricDateMin),
      lit(metricDateMax)))
  }
      )
      .withColumn("user_id", getOldUserId(col("sid"), col("account_guid"),
      col("postal_code"), col("device_platform")))
      .withColumn("country", getCountry(col("postal_code")))
      .withColumn("revenue", when(col("country") === lit("US"),
      col("premium_unique_engaged_visits") *
      lit(0.29)).otherwise(col("premium_unique_engaged_visits") * lit(0.31)))
      .filter(
      col("device_platform").isin("iOS", "Android") &&
      !col("user_id").isin("00000000-0000-0000-0000-000000000000",
      "daa_opt_out_sid", "unknown", "", "%3cnull%3e", "unknown_sid", "<null>",
      "null"))
      .groupBy(col("user_id"), col("date").as("metric_date"))
      .agg(
      sum(col("revenue")).as("revenue"),
      udsCols.map(columnString => sum(col(columnString)).as(columnString)):
      _*)



    // group by columns list
    val dfOriginalColumns = df.columns.map(columnString =>
    col(columnString).as(columnString))
    val udsColsWithRev = "revenue" +: udsCols
    // custom metrics
    val distinctDaysActiveCol =
    countDistinct(col("metric_date")).as("uds_distinct_days_active")
    val daysSinceLastSeenCol = max(datediff(col("window_end_date"),
    col("metric_date"))).as("uds_days_since_last_seen")
    // aggregate metrics
    val sumRevMetricOnDayX = aggByRangeByColumn(Vector.range(0, 15),
    udsColsWithRev.filter(Vector("revenue").contains(_)), aggColOnDayX)
    val (sumRevMetricOnDayXCols, sumRevMetricOnDayXNames) =
    (sumRevMetricOnDayX.map(_._1), sumRevMetricOnDayX.map(_._2))
    val cumSum =
    aggCumByColumn(udsColsWithRev)
    val (cumSumCols, cumSumNames) = (cumSum.map(_._1), cumSum.map(_._2))
    val sumRevBetweenBdayAndDayX =
    aggByRangeByColumn(
    Vector(3, 7, 14, 21, 28, 60, 90, 120, 150, 180, 210, 240, 270, 300,
    330, 360),
    udsColsWithRev.filter(Vector("revenue").contains(_)),
    aggColBetweenBdayAndDayX)
    val (sumRevBetweenBdayAndDayXCols, sumRevBetweenBdayAndDayXNames) =
    (sumRevBetweenBdayAndDayX.map(_._1), sumRevBetweenBdayAndDayX.map(_._2))
    val sumRevBetweenLastXDays = aggByRangeByColumn(Vector(0, 1, 2, 7, 14, 30,
    60), udsColsWithRev.filter(Vector("revenue").contains(_)),
    aggColBetweenLastXDays)
    val (sumRevBetweenLastXDaysCols, sumRevBetweenLastXDaysNames) =
    (sumRevBetweenLastXDays.map(_._1), sumRevBetweenLastXDays.map(_._2))
    // aggregate list
    val aggList =
      distinctDaysActiveCol +:
      daysSinceLastSeenCol +:
      sumRevMetricOnDayXCols ++:
      cumSumCols ++:
      sumRevBetweenBdayAndDayXCols ++:
      sumRevBetweenLastXDaysCols
    // output
    df
    .join(cumUDSDimensions, Seq("user_id"), "left")
    // .filter(col("metric_date").between(col("birth_date"),
    col("window_end_date"))) //this line excludes a small amount of users; need to
    investigate why (it shouldn't)
    .groupBy(dfOriginalColumns: _*)
    .agg(
    aggList.head,
    aggList.tail: _*
    )
    .na.fill(0)
    }






def withAcquisitionDimensions(jdbcUrl: String, tempDir: String): DataFrame =
{


  val rawAcqSourcesDF =
    spark
    .read
    .format("com.databricks.spark.redshift")
    .option("url", jdbcUrl)
    .option("tempdir", tempDir)
    .option("forward_spark_s3_credentials", true)
    .option("query", "select * from public.user_acquisition_sources")
    .load()
    .withColumn("user_id", getOldUserId(col("sid"), col("account_guid"),
    col("postal_code"), col("device_platform")))
    .filter(
    col("device_platform").isin("iOS", "Android") &&
    !col("user_id").isin("00000000-0000-0000-0000-000000000000",
    "daa_opt_out_sid", "unknown", "", "%3cnull%3e", "unknown_sid", "<null>",
    "null"))
    .withColumn(
    "agg_source",
      when(col("media_source").isin("Blacklisted"), lit("Blacklisted"))
      .when(col("media_source").isin("Untrackable"), lit("Untrackable"))
      .when(col("media_source").isin("organic", "Organic Blog", "Organic
      Facebook", "Organic Instagram", "Organic Social", "Organic Soial", "Organic
      Twitter") , lit("Organic"))
    .when(col("media_source").isNull ||
    lower(col("media_source")).isin("unknown", "null"), lit("Unknown"))
    .otherwise(lit("Paid")))
    .withColumn(
    "media_source_clean",
    when(col("media_source").isin("googleadwordsoptimizer_int", "Facebook
      Ads", "Twitter", "googleadwords_int", "inmobi_int", "pinterest_int", "Apple
      Search Ads"), col("media_source"))
    .otherwise(lit("Other")))
    .withColumn(
    "channel_clean",
    when(col("channel").isin("AudienceNetwork", "UAC_Youtube",
      "UAC_Display", "UAC_Search", "Twitter", "Youtube", "Facebook", "Search",
      "Instagram"), col("channel"))
    .otherwise(lit("Other")))
    .select(
      col("user_id"),
      col("media_source"),
      col("channel"),
      col("campaign"),
      col("adset_name"),
      col("ad"),
      col("agg_source"),
      col("media_source_clean"),
      col("channel_clean"))
      df.join(rawAcqSourcesDF, Seq("user_id"), "left")
    }
    }





  def createDataSet(
                    ltvHorizonLength: Int,
                    bdayMin: String,
                    bdayMax: String,
                    jdbcUrl:String,
                    tempDir: String,
                    ltvMethod: String = "one",
                    audienceSample: Double = 1.0): DataFrame = {

    require(Vector("one", "two").contains(ltvMethod), s"ltvMethod param input
    must be either `one` or `two`; `${ltvMethod}` is an invalid input")

    val metricDateMax =
      if (ltvMethod == "one")
        LocalDate.parse(bdayMax).plusDays(ltvHorizonLength).toString
      else
        LocalDate.parse(bdayMax).plusDays(14).toString


    pullUsersWithBday(bdayMin, bdayMax, ltvHorizonLength)
          .sample(audienceSample)
          .withStationaryUIDimensions(bdayMin, bdayMax, jdbcUrl, tempDir)
          .withAcquisitionDimensions(jdbcUrl, tempDir)
          .transform( df => {
          if (ltvMethod == "one")
            df.withLTVMethodOne(bdayMin, metricDateMax)
          else
            df.withLTVMethodTwo(bdayMin, metricDateMax)
          })
          .blowUp(bdayMin)
          .withCumUDSDimensions(udsCols, bdayMin, metricDateMax)
    }






case class ScoringLTV(
                      maxReportDate: LocalDate,
                      birthDateOffset: Int,
                      reportDatesTotal: Int,
                      ltvHorizonLength: Int,
                      maxDaysSinceBirth: Int,
                      modelType: String,
                      modelPath: String,
                      outputBasePath: String,
                      outputCols: Array[String],
                      withRevenue: Boolean) extends RedshiftAccess {



require(Vector("dtr", "rfr", "gbtr").contains(modelType), "modelType must be
either dtr, rfr, or gbtr")
require(birthDateOffset > 0, "birthDateOffset must be greater-than 0")

// meta-data
val reportDates = Vector.tabulate(reportDatesTotal)(i =>
maxReportDate.minusDays(i))
val outputPath = s"${outputBasePath}/${modelType}"



/** methods **/
// create audienceDF; it is 'the train set without the target variable'
def createAudienceToScore(
                          ltvHorizonLength: Int,
                          bdayMin: String,
                          bdayMax: String,
                          withRevenue: Boolean,
                          jdbcUrl:String,
                          tempDir: String): DataFrame = {



  val metricDateMax =
  LocalDate.parse(bdayMax).plusDays(ltvHorizonLength).toString

  pullUsersWithBday(bdayMin, bdayMax, ltvHorizonLength)
    .withColumn("window_end_date", least(lit(bdayMax).cast(DateType),
    date_add(col("birth_date"), ltvHorizonLength)))
    .withColumn("days_since_birth", datediff(col("window_end_date"),
    col("birth_date")))
    .withCumUDSDimensions(udsCols, bdayMin, bdayMax)
    .withStationaryUIDimensions(bdayMin, bdayMax, jdbcUrl, tempDir)
    .withAcquisitionDimensions(jdbcUrl, tempDir)
    .transform( df => {
      if (withRevenue)
        df.withLTVMethodOne(bdayMin, metricDateMax)
      else
        df
      })
    }





def scoreAudience(audienceDF: DataFrame, fullPathRegressorModels:
Seq[String], outputCols: Array[String]): DataFrame = {

  val outputSchema = new StructType(audienceDF.schema.filter(structField =>
  outputCols.contains(structField.name)).toArray).add("prediction", DoubleType,
  false)

  def scoreAudienceOfOneBirthDate(modelPath: String): DataFrame = {
    val modelDaysSinceBirth = modelPath.split("/").last.toInt
    val audienceFilteredDF = audienceDF.filter(col("days_since_birth") ===
    lit(modelDaysSinceBirth))
    // return empty dataframe if audienceFilteredDF is empty
        if (audienceFilteredDF.head(1).isEmpty)
      spark.createDataFrame(sc.emptyRDD[Row], outputSchema)
      else
        PipelineModel
        .load(modelPath)
        .transform(audienceFilteredDF)
        .select(outputCols.map(columnString => col(columnString)): _*)
        }



def recursiveHelper(fullPathRegressorModelsRemaining: Seq[String], dfAccum:
DataFrame, counter: Int): DataFrame = {
  if (fullPathRegressorModelsRemaining.isEmpty)
    dfAccum
  else if (counter >= 5)
    recursiveHelper(fullPathRegressorModelsRemaining.tail,
    dfAccum.union(scoreAudienceOfOneBirthDate(fullPathRegressorModelsRemaining.head
    )).repartition(600), 0)
  else
    recursiveHelper(fullPathRegressorModelsRemaining.tail,
    dfAccum.union(scoreAudienceOfOneBirthDate(fullPathRegressorModelsRemaining.head
    )), counter + 1)
    }

  recursiveHelper(fullPathRegressorModels.tail,
  scoreAudienceOfOneBirthDate(fullPathRegressorModels.head), 0)

  }



// run scoring
def run(
        reportDates: Seq[LocalDate] = this.reportDates,
        birthDateOffset: Int = this.birthDateOffset,
        ltvHorizonLength: Int = this.ltvHorizonLength,
        maxDaysSinceBirth: Int = this.maxDaysSinceBirth,
        modelType: String = this.modelType,
        modelPath: String = this.modelPath,
        outputPath: String = this.outputPath,
        jdbcUrl: String = this.jdbcUrl,
        tempDir: String = this.tempDir,
        outputCols: Array[String] = this.outputCols,
        withRevenue: Boolean = this.withRevenue): Unit = {


  def scoreForOneReportDate(reportDate: LocalDate, birthDateOffset: Int,
  outputCols: Array[String], withRevenue: Boolean): DataFrame = {

    // audienceDF is 'the train set without the target variable'
    val bdayMax = reportDate.minusDays(birthDateOffset)
    val bdayMin = bdayMax.minusDays(maxDaysSinceBirth) // minus 'x' days will
    produce up-to day_since_birth = x
    val audienceDF = createAudienceToScore(ltvHorizonLength,
    bdayMin.toString, bdayMax.toString, withRevenue, jdbcUrl, tempDir)
    val audienceDFValidCountry = audienceDF.filter(col("country").isin("CA",
    "US"))


    // caching,
    //the scoreAudience function calls audienceDf multiple times
    // score audienceDF; involves breaking apart the df by
    //`days_since_birth`, and scoring each subset df with it's respective model
    audienceDFValidCountry.cache; audienceDFValidCountry.count


    val pathMostRecentModelDate = Try(dbutils.fs.ls(modelPath).map(fileInfo
    => fileInfo.path).filter(path => path.split("/").last <=
    reportDate.toString).sorted.last) match {
      case Failure(e: java.util.NoSuchElementException) => throw new
      IllegalArgumentException(s"no models written before
      `report_date=${reportDate}`")
      case Failure(e) => throw new Exception(s"unexpected error when pulling
      `pathMostRecentModelDate`: ${e.getMessage}")
      case Success(arr) => arr
      }

    val pathMostRecentRegressor =
    dbutils.fs.ls(s"${pathMostRecentModelDate}/regressor").map(fileInfo =>
    fileInfo.path).filter(p => p.split("/").last == modelType).head
    val fullPathRegressorModels =
    dbutils.fs.ls(pathMostRecentRegressor).map(fileInfo =>
    fileInfo.path).filter(path => path.split("/").last.toInt <= maxDaysSinceBirth)
    val daysSinceBirthScored = fullPathRegressorModels.map(modelPath =>
    modelPath.split("/").last.toInt).filter(i => i <= maxDaysSinceBirth).sorted
    val modelBuildDate = fullPathRegressorModels.head.split("/")(7)

    val audienceDFUnknownCountry =
      audienceDF
      .filter(!col("country").isin("CA", "US") &&
      col("days_since_birth").isin(daysSinceBirthScored: _*))
      .select(outputCols.filter(columnString => columnString !=
      "prediction").map(columnString => col(columnString)): _*)
      .withColumn("prediction", lit(0.0))
      scoreAudience(audienceDFValidCountry, fullPathRegressorModels,
      outputCols)
      .union(audienceDFUnknownCountry)
      .withColumn("modelBuildDate", lit(modelBuildDate))
      .withColumn("report_date", lit(reportDate.toString))
    }



    // unions the outputs of multiple runs of `scoreForOneReportDate` and write
    def recursiveHelper(reportDatesRemaining: Seq[LocalDate], birthDateOffset:
    Int, dfAccum: DataFrame, counter: Int, outputCols: Array[String], withRevenue:
    Boolean): DataFrame = {

      if (reportDatesRemaining.isEmpty)
        dfAccum
      else if (counter >= 5)
        recursiveHelper(reportDatesRemaining.tail, birthDateOffset,
        dfAccum.union(scoreForOneReportDate(reportDatesRemaining.head, birthDateOffset,
        outputCols, withRevenue)).repartition(600), 0, outputCols, withRevenue)
      else
        recursiveHelper(reportDatesRemaining.tail, birthDateOffset,
        dfAccum.union(scoreForOneReportDate(reportDatesRemaining.head, birthDateOffset,
        outputCols, withRevenue)), counter + 1, outputCols, withRevenue)
      }

      val audienceScoredDF = recursiveHelper(reportDates.tail, birthDateOffset,
      scoreForOneReportDate(reportDates.head, birthDateOffset, outputCols,
      withRevenue), 0, outputCols, withRevenue)

      reportDates.foreach(reportDate =>
      dbutils.fs.rm(s"${outputPath}/report_date=${reportDate}", true))

      audienceScoredDF.write.partitionBy("report_date").mode(SaveMode.Append).parquet
      (outputPath)
      }
    }













val scoringLTV =
new ScoringLTV(
maxReportDate = LocalDate.now(),
reportDatesTotal = 1,
ltvHorizonLength = 365,
maxDaysSinceBirth = 60,
ltvMethod = "one",
cumUDSMethod = "one",
modelType = "dtr",
modelPath = "s3a://don/ltv/first_prod",
outputBasePath = "s3a://don/ltv_test_path/scored_audience")

scoringLTV.run()
