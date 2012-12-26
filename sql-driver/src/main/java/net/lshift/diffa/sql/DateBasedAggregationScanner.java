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
import net.lshift.diffa.adapter.scanning.ScanConstraint;
import net.lshift.diffa.adapter.scanning.ScanResultEntry;
import net.lshift.diffa.scanning.ScanResultHandler;
import org.joda.time.DateTime;
import org.jooq.*;
import org.jooq.impl.Factory;
import org.jooq.impl.SQLDataType;

import java.sql.Date;
import java.util.Map;
import java.util.Set;

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
  private final Table<Record> A;
  private final Table<Record> B;
  private final Field<String> A_ID;
  private final Field<String> B_ID;
  private final Field<String> A_VERSION;
  private final Field<Date> truncDay;
  private final Field<Integer> bucketCount;
  private final Field<Date> truncMonth;
  private final Field<Date> truncYear;

  public DateBasedAggregationScanner(Factory db, PartitionMetadata config, int maxSliceSize) {
    this.db = db;
    Table<Record> underlyingTable = config.getTable();
    this.A = underlyingTable.as("A");
    this.B = underlyingTable.as("B");

    Field<String> underlyingId = (Field<String>) config.getId();
    this.A_ID = A.getField(underlyingId);
    this.B_ID = B.getField(underlyingId);

    Field<String> underlyingVersion = (Field<String>) config.getVersion();
    this.A_VERSION = A.getField(underlyingVersion);

    Field<?> underlyingPartition = config.partitionBy();
    Field<?> A_PARTITION = A.getField(underlyingPartition);
    this.truncDay = Factory.field("trunc({0}, {1})", SQLDataType.DATE, A_PARTITION, Factory.inline("DD"));
    this.truncMonth = Factory.field("trunc({0}, {1})", SQLDataType.DATE, day, Factory.inline("MM"));
    this.truncYear = Factory.field("trunc({0}, {1})", SQLDataType.DATE, month, Factory.inline("YY"));

    this.bucketCount = cast(ceil(cast(count(), SQLDataType.REAL).div(maxSliceSize)), SQLDataType.INTEGER);
  }

  public void scan(Set<ScanConstraint> constraints, ScanResultHandler handler) {
    Cursor<Record> cursor = yearly().fetchLazy();

    while (cursor.hasNext()) {
      Record record = cursor.fetchOne();

      String dateComponent = (new DateTime(record.getValueAsDate(year))).getYear() + "";

      Map<String, String> partition = ImmutableMap.of("bizDate", dateComponent);
      String digestValue = record.getValueAsString(digest);
      ScanResultEntry entry = ScanResultEntry.forAggregate(digestValue, partition);
      handler.onEntry(entry);
    }
    cursor.close();
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
        groupBy(truncDay, A_ID, A_VERSION).
        orderBy(truncDay, bucket);
  }

  private Field<String> md5(Field<Object> of, Field<Object> orderBy) {
    return function("md5", String.class, groupConcat(of).orderBy(orderBy.asc()).separator("")).as(digest.getName());
  }
}
