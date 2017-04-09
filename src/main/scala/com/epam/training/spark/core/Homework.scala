package com.epam.training.spark.core

import java.time.LocalDate
import java.time.temporal.{ChronoUnit, TemporalUnit}

import com.epam.training.spark.core.domain.Climate
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Homework {
  val DELIMITER = ";"
  val RAW_BUDAPEST_DATA = "data/budapest_daily_1901-2010.csv"
  val OUTPUT_DUR = "output"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("EPAM BigData training Spark Core homework")
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.shuffle.partitions", "10")
    val sc = new SparkContext(sparkConf)

    processData(sc)

    sc.stop()

  }

  def processData(sc: SparkContext): Unit = {

    /**
      * Task 1
      * Read raw data from provided file, remove header, split rows by delimiter
      */
    val rawData: RDD[List[String]] = getRawDataWithoutHeader(sc, Homework.RAW_BUDAPEST_DATA)

    /**
      * Task 2
      * Find errors or missing values in the data
      */
    val errors: List[Int] = findErrors(rawData)
    println(errors)

    /**
      * Task 3
      * Map raw data to Climate type
      */
    val climateRdd: RDD[Climate] = mapToClimate(rawData)

    /**
      * Task 4
      * List average temperature for a given day in every year
      */
    val averageTemeperatureRdd: RDD[Double] = averageTemperature(climateRdd, 1, 2)

    /**
      * Task 5
      * Predict temperature based on mean temperature for every year including 1 day before and after
      * For the given month 1 and day 2 (2nd January) include days 1st January and 3rd January in the calculation
      */
    val predictedTemperature: Double = predictTemperature(climateRdd, 1, 2)
    println(s"Predicted temperature: $predictedTemperature")

  }

  def getRawDataWithoutHeader(sc: SparkContext, rawDataPath: String): RDD[List[String]] = {

    sc.textFile(rawDataPath)
      .filter(!_.startsWith("#datum"))
      .map(_.split(";", -1).toList)
  }

  def aggregateLists(aggregate: List[Int], newLine: List[Int]) : List[Int] = {
    val result = ListBuffer.empty[Int]

    for ( i <- 0 until aggregate.size) {
      result += aggregate(i) + newLine(i)
    }

    result.toList

  }

  def findErrors(rawData: RDD[List[String]]): List[Int] = {

    rawData.map(rawData =>
      rawData.map(field => if (field.isEmpty) 1 else 0)
    ).reduce(aggregateLists)

  }

  def mapToClimate(rawData: RDD[List[String]]): RDD[Climate] = {
    rawData.map(Climate.parse)
  }

  def averageTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): RDD[Double] = {

    climateData.filter(climateData => climateData.observationDate.getMonthValue == month &&
      climateData.observationDate.getDayOfMonth == dayOfMonth).
      map(climateData => climateData.meanTemperature.value)

  }

  def predictTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): Double = {
    climateData.
      filter(climateData => withinDateRange(climateData.observationDate, month, dayOfMonth, 1)).
      map(climateData => climateData.meanTemperature.value).mean

  }

  def withinDateRange(date: LocalDate, month: Int, dayOfMonth: Int, tolerance: Int): Boolean = {

    equalsDay(date, month, dayOfMonth) ||
    equalsDay(date.plus(tolerance, ChronoUnit.DAYS), month, dayOfMonth) ||
    equalsDay(date.minus(tolerance, ChronoUnit.DAYS), month, dayOfMonth)

  }

  def equalsDay(date: LocalDate, month: Int, dayOfMonth: Int): Boolean = {
    date.getDayOfMonth == dayOfMonth && date.getMonthValue == month
  }

}


