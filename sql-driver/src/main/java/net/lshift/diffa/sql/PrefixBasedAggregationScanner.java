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

import net.lshift.diffa.adapter.scanning.ScanAggregation;
import net.lshift.diffa.adapter.scanning.StringPrefixAggregation;
import net.lshift.diffa.interview.Answer;
import net.lshift.diffa.interview.SimpleGroupedAnswer;
import org.jooq.*;
import org.jooq.impl.Factory;
import org.jooq.impl.SQLDataType;

import java.util.Set;

/**
 */
public class PrefixBasedAggregationScanner extends AggregatingScanner {
  private int maxPrefixLength = 3;
  private int step = 1;

  private Field<String> truncDay; // truncDay
  private Field<String> truncMonth; // truncMonth
  private Field<String> truncYear; // truncYear

  private final Field<String> day = Factory.field("P3", SQLDataType.VARCHAR); // day
  private final Field<String> month = Factory.field("P2", SQLDataType.VARCHAR); // month
  private final Field<String> year = Factory.field("P1", SQLDataType.VARCHAR); // year

  public PrefixBasedAggregationScanner(Factory db, PartitionMetadata config, int maxSliceSize) {
    super(db, config, maxSliceSize);
  }

  @Override
  protected Cursor<Record> runScan(Set<ScanAggregation> aggregations) {
    int prefixLength = 1;
    if (aggregations != null && aggregations.size() == 1) {
      for (ScanAggregation aggregation : aggregations) {
        if (aggregation instanceof StringPrefixAggregation) {
          StringPrefixAggregation stringPrefixAggregation = (StringPrefixAggregation) aggregation;
          prefixLength = stringPrefixAggregation.getLength();
          // TODO set 'step' based on stringPrefixAggregation.stepSize (coming soon).
          // TODO ditto for 'maxPrefixLength'.
        }
      }
    }
    SelectLimitStep query = yearly();
    String sql = query.toString();
    return db.fetchLazy(sql);
  }

  @Override
  protected Answer recordToAnswer(Record record) {
    String idComponent = (String) record.getValue(0);
    String digestValue = record.getValueAsString(digest);

    return new SimpleGroupedAnswer(idComponent, digestValue);
  }

  @Override
  protected void configurePartitions() {
    Field<?> underlyingPartition = this.partitionColumn;
    Field<?> A_PARTITION = A.getField(underlyingPartition);

    this.truncDay = columnPrefix((Field<String>) A_PARTITION, 3); // truncDay
    this.truncMonth = columnPrefix(day, 2); // truncMonth
    this.truncYear = columnPrefix(month, 1); // truncYear
  }

  private Field<String> columnPrefix(Field<String> column, int length) {
    return Factory.substring(column, 1, length); // substring offset index is 1-based.
  }

  private <T,U>SelectLimitStep step(Field<T> f1, Field<U> f1a, Field<Object> digest, Field<U> f2a, SelectLimitStep next) {
    return db.select(f1.as(f1a.getName()), md5(digest, f2a)).
        from(next).
        groupBy(f1).
        orderBy(f1a);
  }

  private SelectLimitStep yearly() {
    return step(truncYear, year, digest, month, monthly());
  }

  private SelectLimitStep monthly() {
    return step(truncMonth, month, digest, day, daily());
  }

  private SelectLimitStep daily() {
    return db.select(day, md5(digest, bucket)).
        from(sliced()).
        groupBy(day).
        orderBy(day);
  }

  private SelectLimitStep sliced() { // dailyAndSliced()
    return db.select(day, bucket, md5(version, id)).
        from(sliceAssignedEntities()).
        groupBy(day, bucket).
        orderBy(day, bucket);
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
        orderBy(day, bucket);
  }
}
