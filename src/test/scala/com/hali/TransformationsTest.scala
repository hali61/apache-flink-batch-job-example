package com.hali

import com.hali.CaseClasses.{Event, Result, UserEvents}
import com.hali.Transformations._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.test.util.AbstractTestBase
import org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConverters.asJavaIterableConverter


class TransformationsTest extends AbstractTestBase {

  var env: ExecutionEnvironment = null
  var allEvents: DataSet[Event] = null

  @Before
  def startup(): Unit = {
    env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val data: List[Event] = List[Event](
      Event(1535816823, 164, "remove", 86),
      Event(1536392928, 170, "remove", 13),
      Event(1536272308, 196, "click", 70),
      Event(1536757406, 205, "add", 93),
      Event(1536685863, 219, "add", 25),
      Event(1535903776, 223, "remove", 77),
      Event(1536320315, 270, "view", 69),
      Event(1536855831, 273, "click", 13),
      Event(1535875547, 308, "view", 61),
      Event(1536199670, 322, "click", 16),
      Event(1536460770, 356, "click", 36),
      Event(1535954633, 365, "click", 24),
      Event(1536630261, 388, "remove", 67),
      Event(1536634815, 407, "add", 13),
      Event(1536110009, 412, "view", 97),
      Event(1536022417, 446, "click", 17),
      Event(1536925656, 450, "click", 2),
      Event(1536775625, 458, "remove", 100),
      Event(1536600273, 459, "remove", 76),
      Event(1535851501, 481, "click", 58),
      Event(1536413854, 496, "view", 13),
      Event(1536376131, 496, "view", 13),
      Event(1536733799, 501, "view", 71),
      Event(1536487700, 523, "click", 36),
      Event(1535899552, 546, "view", 14),
      Event(1536789352, 561, "add", 3),
      Event(1536673460, 561, "view", 65),
      Event(1536345324, 618, "add", 33),
      Event(1536344576, 618, "view", 63),
      Event(1536591871, 644, "view", 16),
      Event(1536062486, 687, "click", 62),
      Event(1536930731, 694, "view", 88),
      Event(1536754659, 700, "click", 12),
      Event(1536650806, 715, "remove", 16),
      Event(1536715686, 773, "view", 17),
      Event(1536149189, 790, "add", 42),
      Event(1536879064, 795, "click", 42),
      Event(1536058417, 936, "click", 87),
      Event(1536482084, 937, "view", 82),
      Event(1536062591, 953, "add", 81)
    )
    allEvents = env.fromCollection(data)
  }

  @Test
  def uniqueProductView() = {
    val uniqueProductView = getUniqueProductViewCount(allEvents)
    val list: List[Result[Int]] = uniqueProductView.collect().toList
    assertEquals(list.size, 12)
    assertThat(list.asJava, containsInAnyOrder(Result(270, 1),
      Result(308, 1),
      Result(412, 1),
      Result(496, 1),
      Result(501, 1),
      Result(546, 1),
      Result(561, 1),
      Result(618, 1),
      Result(644, 1),
      Result(694, 1),
      Result(773, 1),
      Result(937, 1)))

  }

  @Test
  def uniqueEventView() = {
    val uniqueEventView = getUniqueEventCount(allEvents)
    val list: List[Result[String]] = uniqueEventView.collect().toList
    assertEquals(list.size, 4)
    assertThat(list.asJava, containsInAnyOrder(Result("remove", 7),
      Result("click", 13),
      Result("view", 12),
      Result("add", 7)))

  }

  @Test
  def topFiveUserFulfilled() = {
    val topUsers = getTopFiveUsersFulfilledAllEvents(allEvents)
    val list: List[Int] = topUsers.collect().toList
    assertEquals(list.size, 1)
    assertThat(list.asJava, containsInAnyOrder(13))

  }

  @Test
  def userProductViewCount() = {
    val userProducts = getUserProductView(allEvents.filter(line => line.userId == 13))
    val list: List[Int] = userProducts.collect().toList
    assertEquals(list.size, 2)
    assertThat(list.asJava, containsInAnyOrder(496,496))

  }

  @Test
  def userEventCount() = {
    val userEvents = getUserEvent(allEvents.filter(line => line.userId == 13))
    val list: List[UserEvents] = userEvents.collect().toList
    assertEquals(list.size, 5)
    assertThat(list.asJava, containsInAnyOrder(UserEvents("remove",170),UserEvents("view",496),UserEvents("view",496),UserEvents("add",407),UserEvents("click",273)))

  }

}
