package com.hali

object CaseClasses {

  case class Event(timestamp: Long, productId: Int, event: String, userId: Int)

  case class Result[T](value: T, count: Int)

  case class UserEvents(event: String, productId: Int)

}
