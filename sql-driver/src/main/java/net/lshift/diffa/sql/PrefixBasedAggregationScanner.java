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
  private static final Field<String> PREFIX_ALIAS = Factory.field("PREF", SQLDataType.VARCHAR);

  private int maxPrefixLength = 3;
  private int step = 1;

  public PrefixBasedAggregationScanner(Factory db, PartitionMetadata config, int maxSliceSize) {
    super(db, config, maxSliceSize);
  }

  @Override
  protected void configurePartitions() {} // determined dynamically.

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
    SelectLimitStep query = aggregateByPrefixOfLength(prefixLength);
    return query.fetchLazy();
  }

  @Override
  protected Answer recordToAnswer(Record record) {
    String idComponent = record.getValueAsString(PREFIX_ALIAS);
    String digestValue = record.getValueAsString(digest);
//    String digestValue = "abc";

    return new SimpleGroupedAnswer(idComponent, digestValue);
  }

  private SelectLimitStep aggregateByPrefixOfLength(int prefixLength) {
    if (prefixLength >= maxPrefixLength) {
      return sliced();
    } else {
      return db.select(columnPrefix(PREFIX_ALIAS, prefixLength).as(PREFIX_ALIAS.getName()), md5(digest, PREFIX_ALIAS)).
          from(aggregateByPrefixOfLength(prefixLength + step)).
          groupBy(columnPrefix(PREFIX_ALIAS, prefixLength).as(PREFIX_ALIAS.getName())).
          orderBy(1);
    }
  }

  private Field<String> getPrefixAlias(int prefixLength) {
    return Factory.field("PREF" + prefixLength, SQLDataType.VARCHAR);
  }

  private SelectLimitStep sliced() {
    return db.select(PREFIX_ALIAS, bucket, md5(version, id)).
        from(sliceAssignedEntities()).
        groupBy(PREFIX_ALIAS, bucket).
        orderBy(1, 2);
  }

  private SelectLimitStep sliceAssignedEntities() {
    return db.select(
          columnPrefix(A_ID, maxPrefixLength).as(PREFIX_ALIAS.getName()),
          bucketCount.as(bucket.getName()),
          A_ID.as(id.getName()),
          A_VERSION.as(version.getName())).
        from(A).join(B).on(A_ID.ge(B_ID)).
        where(filters).
        groupBy(columnPrefix(A_ID, maxPrefixLength), A_ID, A_VERSION).
        orderBy(1, 2);
  }

  private Field<String> columnPrefix(Field<String> column, int length) {
    return Factory.substring(column, 0, length);
  }
}
