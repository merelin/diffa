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

  private Field<String> prefixLength3;
  private Field<String> prefixLength2;
  private Field<String> prefixLength1;

  private final Field<String> aliasP3 = Factory.field("P3", SQLDataType.VARCHAR);
  private final Field<String> aliasP2 = Factory.field("P2", SQLDataType.VARCHAR);
  private final Field<String> aliasP1 = Factory.field("P1", SQLDataType.VARCHAR);

  public PrefixBasedAggregationScanner(Factory db, PartitionMetadata config, int maxSliceSize) {
    super(db, config, maxSliceSize);
  }

  @Override
  protected Cursor<Record> runScan(Set<ScanAggregation> aggregations) {
    int shortestPrefix = 1;
    if (aggregations != null && aggregations.size() == 1) {
      for (ScanAggregation aggregation : aggregations) {
        if (aggregation instanceof StringPrefixAggregation) {
          StringPrefixAggregation stringPrefixAggregation = (StringPrefixAggregation) aggregation;
          shortestPrefix = stringPrefixAggregation.getOffsets().pollFirst();
          // TODO set 'step' based on stringPrefixAggregation.stepSize (coming soon).
          // TODO ditto for 'maxPrefixLength'.
        }
      }
    }
    SelectLimitStep query = getQueryForPrefixLength(shortestPrefix);
    String sql = query.toString();
    return db.fetchLazy(sql);
//    return query.fetchLazy();
  }

  // This is just a temporary solution.
  // TODO replace with dynamic implementation that doesn't have a fixed depth.
  private SelectLimitStep getQueryForPrefixLength(int prefixLength) {
    switch (prefixLength) {
      case 1: return rollupToLength1();
      case 2: return rollupToLength2();
      default: return rollupToLength3();
    }
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

    this.prefixLength3 = columnPrefix((Field<String>) A_PARTITION, 3);
    this.prefixLength2 = columnPrefix(aliasP3, 2);
    this.prefixLength1 = columnPrefix(aliasP2, 1);
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

  private SelectLimitStep rollupToLength1() {
    return step(prefixLength1, aliasP1, digest, aliasP2, rollupToLength2());
  }

  private SelectLimitStep rollupToLength2() {
    return step(prefixLength2, aliasP2, digest, aliasP3, rollupToLength3());
  }

  private SelectLimitStep rollupToLength3() {
    return db.select(aliasP3, md5(digest, bucket)).
        from(sliced()).
        groupBy(aliasP3).
        orderBy(aliasP3);
  }

  private SelectLimitStep sliced() {
    return db.select(aliasP3, bucket, md5(version, id)).
        from(sliceAssignedEntities()).
        groupBy(aliasP3, bucket).
        orderBy(aliasP3, bucket);
  }

  private SelectLimitStep sliceAssignedEntities() {
    return db.select(
          prefixLength3.as(aliasP3.getName()),
          A_ID.as(id.getName()),
          A_VERSION.as(version.getName()),
          bucketCount.as(bucket.getName())).
        from(A).join(B).on(A_ID.ge(B_ID)).
        where(filters).
        groupBy(prefixLength3, A_ID, A_VERSION).
        orderBy(aliasP3, bucket);
  }
}
