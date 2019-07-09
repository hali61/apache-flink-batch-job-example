package com.hali

import com.hali.CaseClasses.{Event, Result, UserEvents}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.DataSet
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

object Transformations {


  // Unique ProductView
  def getUniqueProductViewCount(events: DataSet[Event]): DataSet[Result[Int]] = events
    .filter(data => data.event.toLowerCase().equals(Events.VIEW))
    .map(line => (line.productId, line.userId))
    .distinct()
    .map(line => Result(line._1, 1))
    .groupBy("value")
    .sum("count")
    .setParallelism(1)
    .sortPartition("count", Order.DESCENDING)


  // Unique Event
  def getUniqueEventCount(events: DataSet[Event]): DataSet[Result[String]] = events
    .map(line => (line.productId, line.event, line.userId))
    .distinct()
    .map(line => Result(line._2, 1))
    .groupBy("value")
    .sum("count")
    .setParallelism(1)
    .sortPartition("count", Order.DESCENDING)


  // Fulfilled all the events
  def getTopFiveUsersFulfilledAllEvents(events: DataSet[Event]) = events
    .groupBy("userId ")
    .reduceGroup { (lines, out: Collector[(Int, Set[String], Seq[String])]) => // Set has user unique events, Seq has user all events
      var key: Int = 0
      var userAllEvents = Seq[String]()
      var userUniqueEvents = Set[String]()
      for (line <- lines) {
        key = line.userId
        userAllEvents = userAllEvents :+ line.event
        userUniqueEvents = userUniqueEvents + line.event
      }
      if (key != 0)
        out.collect((key, userUniqueEvents, userAllEvents))
    }
    .filter(line => line._2.size == 4) // user fulfilled all events if set has four events
    .map(line => (line._1, line._3.size))
    .groupBy(0)
    .sum(1)
    .setParallelism(1)
    .sortPartition(1, Order.DESCENDING)
    .first(5)
    .map(_._1)


  // Events of user
  def getUserEvent(filteredUser: DataSet[Event]): DataSet[UserEvents] = filteredUser
    .map(line => UserEvents(line.event, line.productId))


  // Product View of user
  def getUserProductView(filteredUser: DataSet[Event]) = filteredUser
    .filter(line => line.event.toLowerCase.equals(Events.VIEW))
    .map(line => line.productId)

}


