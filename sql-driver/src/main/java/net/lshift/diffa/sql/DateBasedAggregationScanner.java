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

import net.lshift.diffa.adapter.scanning.DateAggregation;
import net.lshift.diffa.adapter.scanning.DateGranularityEnum;
import net.lshift.diffa.adapter.scanning.ScanAggregation;
import net.lshift.diffa.interview.Answer;
import net.lshift.diffa.interview.SimpleGroupedAnswer;
import org.joda.time.DateTime;
import org.jooq.*;
import org.jooq.impl.Factory;
import org.jooq.impl.SQLDataType;

import java.sql.Date;
import java.util.*;

/**
 */
public class DateBasedAggregationScanner extends AggregatingScanner {
  private static final Field<Object> day = Factory.field("DAY");
  private static final Field<Object> month = Factory.field("MONTH");
  private static final Field<Object> year = Factory.field("YEAR");

  private Field<Date> truncDay;
  private Field<Date> truncMonth;
  private Field<Date> truncYear;

  public DateBasedAggregationScanner(Factory db, PartitionMetadata config, int maxSliceSize) {
    super(db, config, maxSliceSize);
  }

  protected Cursor<Record> runScan(Set<ScanAggregation> aggregations) {
    DateGranularityEnum granularity = DateGranularityEnum.Yearly;
    if (aggregations != null && aggregations.size() == 1) {
      for (ScanAggregation aggregation : aggregations) {
        if (aggregation instanceof DateAggregation) {
          DateAggregation dateAggregation = (DateAggregation) aggregation;
          granularity = dateAggregation.getGranularity();
        }
      }
    }

    return queryForGranularity(granularity).fetchLazy();
  }

  private SelectFinalStep queryForGranularity(DateGranularityEnum granularity) {
    switch (granularity) {
      case Yearly: return yearly();
      case Monthly: return monthly();
      default: return daily();
    }
  }

  protected Answer recordToAnswer(Record record) {
    String dateComponent = (new DateTime(record.getValueAsDate(year))).getYear() + "";
    String digestValue = record.getValueAsString(digest);

    return new SimpleGroupedAnswer(dateComponent, digestValue);
  }

  protected void configurePartitions() {
    Field<?> underlyingPartition = this.partitionColumn;
    Field<?> A_PARTITION = A.getField(underlyingPartition);
    this.truncDay = truncDate(A_PARTITION, "DD");
    this.truncMonth = truncDate(day, "MM");
    this.truncYear = truncDate(month, "YY");
  }

  private Field<Date> truncDate(Field<?> column, String granularity) {
    return Factory.field("trunc({0}, {1})", SQLDataType.DATE, column, Factory.inline(granularity));
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
        from(sliceAssignedEntities()).
        groupBy(day, bucket).
        orderBy(day, bucket); // must order or else the roll-up to daily could be wrong.
  }

  private SelectLimitStep sliceAssignedEntities() {
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
}
