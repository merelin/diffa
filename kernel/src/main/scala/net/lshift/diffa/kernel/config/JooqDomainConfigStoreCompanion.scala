/**
 * Copyright (C) 2010-2012 LShift Ltd.
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
package net.lshift.diffa.kernel.config

import org.jooq.impl.Factory
import net.lshift.diffa.schema.tables.UniqueCategoryNames.UNIQUE_CATEGORY_NAMES
import net.lshift.diffa.schema.tables.PrefixCategories.PREFIX_CATEGORIES
import net.lshift.diffa.schema.tables.SetCategories.SET_CATEGORIES
import net.lshift.diffa.schema.tables.RangeCategories.RANGE_CATEGORIES
import scala.collection.JavaConversions._
import org.jooq.{Record, Result}
import net.lshift.diffa.schema.tables.Escalations.ESCALATIONS
import net.lshift.diffa.schema.tables.PairReports.PAIR_REPORTS
import net.lshift.diffa.schema.tables.RepairActions.REPAIR_ACTIONS
import net.lshift.diffa.schema.tables.Pair.PAIR
import net.lshift.diffa.kernel.util.MissingObjectException
import net.lshift.diffa.schema.tables.UserItemVisibility.USER_ITEM_VISIBILITY
import net.lshift.diffa.schema.tables.PairViews.PAIR_VIEWS
import net.lshift.diffa.schema.tables.StoreCheckpoints.STORE_CHECKPOINTS
import net.lshift.diffa.kernel.frontend.RepairActionDef
import scala.Some
import net.lshift.diffa.kernel.frontend.EscalationDef
import net.lshift.diffa.kernel.frontend.PairReportDef

/**
 * This object is a workaround for the fact that Scala is so slow
 */
object JooqDomainConfigStoreCompanion {

  val ENDPOINT_TARGET_TYPE = "endpoint"
  val ENDPOINT_VIEW_TARGET_TYPE = "endpoint_view"

  def mapResultsToList[T](results:Result[Record], rowMapper:Record => T) = {
    val escalations = new java.util.ArrayList[T]()
    results.iterator().foreach(r => escalations.add(rowMapper(r)))
    escalations
  }

  def recordToEscalation(record:Record) : EscalationDef = {
    EscalationDef(
      pair = record.getValue(ESCALATIONS.PAIR_KEY),
      name = record.getValue(ESCALATIONS.NAME),
      action = record.getValue(ESCALATIONS.ACTION),
      actionType = record.getValue(ESCALATIONS.ACTION_TYPE),
      event = record.getValue(ESCALATIONS.EVENT),
      origin = record.getValue(ESCALATIONS.ORIGIN))
  }

  def recordToPairReport(record:Record) : PairReportDef = {
    PairReportDef(
      pair = record.getValue(PAIR_REPORTS.PAIR_KEY),
      name = record.getValue(PAIR_REPORTS.NAME),
      target = record.getValue(PAIR_REPORTS.TARGET),
      reportType = record.getValue(PAIR_REPORTS.REPORT_TYPE)
    )
  }

  def recordToRepairAction(record:Record) : RepairActionDef = {
    RepairActionDef(
      pair = record.getValue(REPAIR_ACTIONS.PAIR_KEY),
      name = record.getValue(REPAIR_ACTIONS.NAME),
      scope = record.getValue(REPAIR_ACTIONS.SCOPE),
      url = record.getValue(REPAIR_ACTIONS.URL)
    )
  }

  def deletePairWithDependencies(t:Factory, pair:DiffaPairRef) = {
    deleteRepairActionsByPair(t, pair)
    deleteEscalationsByPair(t, pair)
    deleteReportsByPair(t, pair)
    deletePairViewsByPair(t, pair)
    deleteStoreCheckpointsByPair(t, pair)
    deleteUserItemsByPair(t, pair)
    deletePairWithoutDependencies(t, pair)
  }

  private def deletePairWithoutDependencies(t:Factory, pair:DiffaPairRef) = {
    val deleted = t.delete(PAIR).
      where(PAIR.DOMAIN.equal(pair.domain)).
      and(PAIR.PAIR_KEY.equal(pair.key)).
      execute()

    if (deleted == 0) {
      throw new MissingObjectException(pair.identifier)
    }
  }

  def deleteUserItemsByPair(t:Factory, pair:DiffaPairRef) = {
    t.delete(USER_ITEM_VISIBILITY).
      where(USER_ITEM_VISIBILITY.DOMAIN.equal(pair.domain)).
      and(USER_ITEM_VISIBILITY.PAIR.equal(pair.key)).
      execute()
  }

  def deleteRepairActionsByPair(t:Factory, pair:DiffaPairRef) = {
    t.delete(REPAIR_ACTIONS).
      where(REPAIR_ACTIONS.DOMAIN.equal(pair.domain)).
      and(REPAIR_ACTIONS.PAIR_KEY.equal(pair.key)).
      execute()
  }

  def deleteEscalationsByPair(t:Factory, pair:DiffaPairRef) = {
    t.delete(ESCALATIONS).
      where(ESCALATIONS.DOMAIN.equal(pair.domain)).
      and(ESCALATIONS.PAIR_KEY.equal(pair.key)).
      execute()
  }

  def deleteReportsByPair(t:Factory, pair:DiffaPairRef) = {
    t.delete(PAIR_REPORTS).
      where(PAIR_REPORTS.DOMAIN.equal(pair.domain)).
      and(PAIR_REPORTS.PAIR_KEY.equal(pair.key)).
      execute()
  }

  def deletePairViewsByPair(t:Factory, pair:DiffaPairRef) = {
    t.delete(PAIR_VIEWS).
      where(PAIR_VIEWS.DOMAIN.equal(pair.domain)).
      and(PAIR_VIEWS.PAIR.equal(pair.key)).
      execute()
  }

  def deleteStoreCheckpointsByPair(t:Factory, pair:DiffaPairRef) = {
    t.delete(STORE_CHECKPOINTS).
      where(STORE_CHECKPOINTS.DOMAIN.equal(pair.domain)).
      and(STORE_CHECKPOINTS.PAIR.equal(pair.key)).
      execute()
  }

  def insertCategories(t:Factory,
                       domain:String,
                       endpoint:String,
                       categories:java.util.Map[String,CategoryDescriptor],
                       viewName: Option[String] = None) = {

    categories.foreach { case (categoryName, descriptor) => {

      val base = t.insertInto(UNIQUE_CATEGORY_NAMES).
                   set(UNIQUE_CATEGORY_NAMES.DOMAIN, domain).
                   set(UNIQUE_CATEGORY_NAMES.ENDPOINT, endpoint).
                   set(UNIQUE_CATEGORY_NAMES.NAME, categoryName)

      val insert = viewName match {

        case Some(view) =>
          base.set(UNIQUE_CATEGORY_NAMES.TARGET_TYPE, ENDPOINT_VIEW_TARGET_TYPE).
            set(UNIQUE_CATEGORY_NAMES.VIEW_NAME, view)
        case None       =>
          base.set(UNIQUE_CATEGORY_NAMES.TARGET_TYPE, ENDPOINT_TARGET_TYPE)

      }

      try {

        insert.execute()

        descriptor match {
          case r:RangeCategoryDescriptor  => insertRangeCategories(t, domain, endpoint, categoryName, r, viewName)
          case s:SetCategoryDescriptor    => insertSetCategories(t, domain, endpoint, categoryName, s, viewName)
          case p:PrefixCategoryDescriptor => insertPrefixCategories(t, domain, endpoint, categoryName, p, viewName)
        }
      }
      catch
        {
          // TODO Catch the unique constraint exception
          case x => throw x
        }
    }}
  }

  def insertPrefixCategories(t:Factory,
                             domain:String,
                             endpoint:String,
                             categoryName:String,
                             descriptor:PrefixCategoryDescriptor,
                             viewName: Option[String] = None) = {

    val insertBase = t.insertInto(PREFIX_CATEGORIES).
                       set(PREFIX_CATEGORIES.DOMAIN, domain).
                       set(PREFIX_CATEGORIES.ENDPOINT, endpoint).
                       set(PREFIX_CATEGORIES.NAME, categoryName).
                       set(PREFIX_CATEGORIES.STEP, Integer.valueOf(descriptor.step)).
                       set(PREFIX_CATEGORIES.MAX_LENGTH, Integer.valueOf(descriptor.maxLength)).
                       set(PREFIX_CATEGORIES.PREFIX_LENGTH, Integer.valueOf(descriptor.prefixLength))

    val insert = viewName match {

      case Some(view) =>
        insertBase.set(PREFIX_CATEGORIES.TARGET_TYPE, ENDPOINT_VIEW_TARGET_TYPE).
          set(PREFIX_CATEGORIES.VIEW_NAME, view)
      case None       =>
        insertBase.set(PREFIX_CATEGORIES.TARGET_TYPE, ENDPOINT_TARGET_TYPE)

    }

    insert.execute()
  }

  def insertSetCategories(t:Factory,
                          domain:String,
                          endpoint:String,
                          categoryName:String,
                          descriptor:SetCategoryDescriptor,
                          viewName: Option[String] = None) = {

    // TODO Is there a way to re-use the insert statement with a bind parameter?

    descriptor.values.foreach(value => {

      val insertBase = t.insertInto(SET_CATEGORIES).
                         set(SET_CATEGORIES.DOMAIN, domain).
                         set(SET_CATEGORIES.ENDPOINT, endpoint).
                         set(SET_CATEGORIES.NAME, categoryName).
                         set(SET_CATEGORIES.VALUE, value)

      val insert = viewName match {

        case Some(view) =>
          insertBase.set(SET_CATEGORIES.TARGET_TYPE, ENDPOINT_VIEW_TARGET_TYPE).
            set(SET_CATEGORIES.VIEW_NAME, view)
        case None       =>
          insertBase.set(SET_CATEGORIES.TARGET_TYPE, ENDPOINT_TARGET_TYPE)

      }

      insert.execute()

    })

  }

  def insertRangeCategories(t:Factory,
                                    domain:String,
                                    endpoint:String,
                                    categoryName:String,
                                    descriptor:RangeCategoryDescriptor,
                                    viewName: Option[String] = None) = {
    val insertBase = t.insertInto(RANGE_CATEGORIES).
      set(RANGE_CATEGORIES.DOMAIN, domain).
      set(RANGE_CATEGORIES.ENDPOINT, endpoint).
      set(RANGE_CATEGORIES.NAME, categoryName).
      set(RANGE_CATEGORIES.DATA_TYPE, descriptor.dataType).
      set(RANGE_CATEGORIES.LOWER_BOUND, descriptor.lower).
      set(RANGE_CATEGORIES.UPPER_BOUND, descriptor.upper).
      set(RANGE_CATEGORIES.MAX_GRANULARITY, descriptor.maxGranularity)

    val insert = viewName match {

      case Some(view) =>
        insertBase.set(RANGE_CATEGORIES.TARGET_TYPE, ENDPOINT_VIEW_TARGET_TYPE).
          set(RANGE_CATEGORIES.VIEW_NAME, view)
      case None       =>
        insertBase.set(RANGE_CATEGORIES.TARGET_TYPE, ENDPOINT_TARGET_TYPE)

    }

    insert.execute()
  }

  def deleteRangeCategories(t:Factory, domain:String, endpoint:String) = {
    t.delete(RANGE_CATEGORIES).
      where(RANGE_CATEGORIES.DOMAIN.equal(domain)).
      and(RANGE_CATEGORIES.ENDPOINT.equal(endpoint)).
      execute()
  }

  def deleteSetCategories(t:Factory, domain:String, endpoint:String) = {
    t.delete(SET_CATEGORIES).
      where(SET_CATEGORIES.DOMAIN.equal(domain)).
      and(SET_CATEGORIES.ENDPOINT.equal(endpoint)).
      execute()
  }

  def deletePrefixCategories(t:Factory, domain:String, endpoint:String) = {
    t.delete(PREFIX_CATEGORIES).
      where(PREFIX_CATEGORIES.DOMAIN.equal(domain)).
      and(PREFIX_CATEGORIES.ENDPOINT.equal(endpoint)).
      execute()
  }

  def deleteCategories(t:Factory, domain:String, endpoint:String) = {
    deletePrefixCategories(t, domain, endpoint)
    deleteSetCategories(t, domain, endpoint)
    deleteRangeCategories(t, domain, endpoint)

    t.delete(UNIQUE_CATEGORY_NAMES).
      where(UNIQUE_CATEGORY_NAMES.DOMAIN.equal(domain)).
      and(UNIQUE_CATEGORY_NAMES.ENDPOINT.equal(endpoint)).
      execute()
  }

}