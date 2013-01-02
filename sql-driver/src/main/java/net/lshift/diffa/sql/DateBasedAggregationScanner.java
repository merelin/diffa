/*
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

package net.lshift.diffa.sql;

import com.google.common.collect.ImmutableMap;
import net.lshift.diffa.adapter.scanning.ScanAggregation;
import net.lshift.diffa.adapter.scanning.ScanConstraint;
import net.lshift.diffa.adapter.scanning.ScanResultEntry;
import net.lshift.diffa.adapter.scanning.SetConstraint;
import net.lshift.diffa.interview.Answer;
import net.lshift.diffa.interview.SimpleGroupedAnswer;
import net.lshift.diffa.scanning.PruningHandler;
import org.joda.time.DateTime;
import org.jooq.*;
import org.jooq.impl.Factory;
import org.jooq.impl.SQLDataType;

import java.sql.Date;
import java.util.*;

import static org.jooq.impl.Factory.*;

/**
 */
public class DateBasedAggregationScanner {
  private static final Field<Object> day = Factory.field("DAY");
  private static final Field<Object> month = Factory.field("MONTH");
  private static final Field<Object> year = Factory.field("YEAR");

  private static final Field<Object> bucket = Factory.field("BUCKET");
  private static final Field<Object> version = Factory.field("VERSION");
  private static final Field<Object> id = Factory.field("ID");
  private static final Field<Object> digest = Factory.field("DIGEST");

  private final Factory db;
  private final DynamicTable underlyingTable;
  private final Field<String> underlyingId;
  private final Field<String> underlyingVersion;
  private final Field<?> partitionColumn;
  private final int maxSliceSize;
  private Table A;
  private Table B;
  private Field<String> A_ID;
  private Field<String> B_ID;
  private Field<String> A_VERSION;
  private Field<Date> truncDay;
  private Field<Integer> bucketCount;
  private Field<Date> truncMonth;
  private Field<Date> truncYear;

  private ArrayList<Condition> filters = new ArrayList<Condition>();

  public DateBasedAggregationScanner(Factory db, PartitionMetadata config, int maxSliceSize) {
    this.db = db;
    this.underlyingTable = config.getTable();
    this.underlyingId = (Field<String>) config.getId();
    this.underlyingVersion = (Field<String>) config.getVersion();
    this.partitionColumn = config.partitionBy();
    this.maxSliceSize = maxSliceSize;

    // There should be at least one condition - this is just a place-holder which doesn't filter.
    filters.add(trueCondition());
  }

  private void configureFields(Set<ScanConstraint> constraints) {
    if (constraints != null) {
      for (ScanConstraint constraint : constraints) {
        // Hope that this can be implicitly converted to the appropriate data type.
        underlyingTable.addField(constraint.getAttributeName(), SQLDataType.VARCHAR);
      }
    }

    this.A = underlyingTable.as("A");
    this.B = underlyingTable.as("B");
    this.A_ID = A.getField(underlyingId);
    this.B_ID = B.getField(underlyingId);
    this.A_VERSION = A.getField(underlyingVersion);
  }

  private void configurePartitions() {
    Field<?> underlyingPartition = this.partitionColumn;
    Field<?> A_PARTITION = A.getField(underlyingPartition);
    this.truncDay = Factory.field("trunc({0}, {1})", SQLDataType.DATE, A_PARTITION, Factory.inline("DD"));
    this.truncMonth = Factory.field("trunc({0}, {1})", SQLDataType.DATE, day, Factory.inline("MM"));
    this.truncYear = Factory.field("trunc({0}, {1})", SQLDataType.DATE, month, Factory.inline("YY"));

    this.bucketCount = cast(ceil(cast(count(), SQLDataType.REAL).div(maxSliceSize)), SQLDataType.INTEGER);
  }

  public void scan(Set<ScanConstraint> constraints, Set<ScanAggregation> aggregations, PruningHandler handler) {
    configureFields(constraints);
    configurePartitions();
    setFilters(constraints);

    Cursor<Record> cursor = yearly().fetchLazy();

    while (cursor.hasNext()) {
      Record record = cursor.fetchOne();

      String dateComponent = (new DateTime(record.getValueAsDate(year))).getYear() + "";
      String digestValue = record.getValueAsString(digest);

      Answer answer = new SimpleGroupedAnswer(dateComponent, digestValue);
      handler.onPrune(answer);
    }
    cursor.close();
  }

  // Currently only gets the first set constraint (assumed to be the extent for now).
  private void setFilters(Set<ScanConstraint> constraints) {
    if (constraints != null) {
      for (ScanConstraint constraint : constraints) {
        if (constraint instanceof SetConstraint) {
          SetConstraint setConstraint = (SetConstraint) constraint;
          Field<String> A_set = (Field<String>) A.getField(setConstraint.getAttributeName());
          Field<String> B_set = (Field<String>) B.getField(setConstraint.getAttributeName());
          for (String value : setConstraint.getValues()) {
            filters.add(A_set.eq(value));
            filters.add(B_set.eq(value));
          }
        }
      }
    }
  }

  private SelectLimitStep yearly() {
    return db.select(truncYear.as(year.getName()), md5(digest, month)).
        from(monthly()).
        groupBy(truncYear).
        orderBy(year);
  }

  private SelectLimitStep monthly() {
    return db.select(truncMonth.as(month.getName()), md5(digest, day)).
        from(daily()).
        groupBy(truncMonth).
        orderBy(month);
  }

  private SelectLimitStep daily() {
    return db.select(day, md5(digest, bucket)).
        from(dailyAndSliced()).
        groupBy(day).
        orderBy(day);
  }

  private SelectLimitStep dailyAndSliced() {
    return db.select(day, bucket, md5(version, id)).
        from(slicedAssignedEntities()).
        groupBy(day, bucket).
        orderBy(day, bucket); // must order or else the roll-up to daily could be wrong.
  }

  private SelectLimitStep slicedAssignedEntities() {
    return db.select(
          truncDay.as(day.getName()),
          A_ID.as(id.getName()),
          A_VERSION.as(version.getName()),
          bucketCount.as(bucket.getName())).
        from(A).join(B).on(A_ID.ge(B_ID)).
        where(filters).
        groupBy(truncDay, A_ID, A_VERSION).
        orderBy(truncDay, bucket);
  }

  private Field<String> md5(Field<Object> of, Field<Object> orderBy) {
    return function("md5", String.class, groupConcat(of).orderBy(orderBy.asc()).separator("")).as(digest.getName());
  }
}
