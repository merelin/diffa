/**
 * Copyright (C) 2010-2011 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lshift.diffa.kernel.participants

import org.joda.time.LocalDate
import org.joda.time.format.{DateTimeFormatterBuilder, ISODateTimeFormat, DateTimeFormatter, DateTimeFormat}
import net.lshift.diffa.kernel.config.{DateTypeDescriptor,DateTimeTypeDescriptor}

/**
 * Provides basic functionality required to narrow down date based categories.
 */
abstract case class DateCategoryFunction extends CategoryFunction {

  val parsers = Array(
          ISODateTimeFormat.dateTime().getParser,
          ISODateTimeFormat.date().getParser
  )
  protected val formatter = new DateTimeFormatterBuilder().append( null, parsers ).toFormatter


  def pattern:DateTimeFormatter
  def descend:Option[CategoryFunction]
  def pointToBounds(d:LocalDate) : (LocalDate,LocalDate)

  def constrain(constraint:QueryConstraint, partition:String) = {
    val point = pattern.parseDateTime(partition).toLocalDate
    val (lower,upper) = pointToBounds(point)

    // TODO converting this eagerly to DateTime is not strictly necessary for every code branch
    val (start,end) = align(lower, upper)

    constraint match {
      case d:DateRangeConstraint
        if d.start != null && d.end != null && (d.start.isAfter(lower) || d.end.isBefore(upper)) => d
      case t:DateTimeRangeConstraint
        if t.start != null && t.end != null && (t.start.isAfter(start) || t.end.isBefore(end)) => t
      case _ =>
         constraint.dataType match {
          case DateTypeDescriptor     =>
            new DateRangeConstraint(constraint.category, lower, upper)
          case DateTimeTypeDescriptor =>
            val (start,end) = align(lower, upper)
            new DateTimeRangeConstraint(constraint.category, start, end)
        }
    }
  }

  def align(s:LocalDate, e:LocalDate) = (s.toDateTimeAtStartOfDay, e.toDateTimeAtStartOfDay.plusDays(1).minusMillis(1))

  def shouldBucket() = true

  override def owningPartition(value:String) =
    try {
      val date = formatter.parseDateTime(value)
      pattern.print(date)
    }
    catch {
      case e: IllegalArgumentException => throw new InvalidAttributeValueException("Value is not a date: "+value)
    }
}

/**
 * This function partitions by whole days.
 */
case object DailyCategoryFunction extends DateCategoryFunction {
  def name = "daily"
  def pattern = DateTimeFormat.forPattern("yyyy-MM-dd")
  def descend = Some(IndividualCategoryFunction)
  def pointToBounds(point:LocalDate) = (point,point)
}

/**
 * This function partitions by whole calendar months.
 */
case object MonthlyCategoryFunction extends DateCategoryFunction {
  def name = "monthly"
  def pattern = DateTimeFormat.forPattern("yyyy-MM")
  def descend = Some(DailyCategoryFunction)
  def pointToBounds(point:LocalDate) = (point.withDayOfMonth(1), point.plusMonths(1).minusDays(1))
}

/**
 * This function partitions by whole years.
 */
case object YearlyCategoryFunction extends DateCategoryFunction {
  def name = "yearly"
  def pattern = DateTimeFormat.forPattern("yyyy")
  def descend = Some(MonthlyCategoryFunction)
  def pointToBounds(point:LocalDate) = (point.withMonthOfYear(1).withDayOfMonth(1), point.withMonthOfYear(12).withDayOfMonth(31))
}