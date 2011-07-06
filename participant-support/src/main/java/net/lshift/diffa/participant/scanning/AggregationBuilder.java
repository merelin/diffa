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
package net.lshift.diffa.participant.scanning;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper for building aggregations from a web request.
 */
public class AggregationBuilder {
  private final HttpServletRequest req;
  private final List<ScanAggregation> result;

  public AggregationBuilder(HttpServletRequest req) {
    this.req = req;
    this.result = new ArrayList<ScanAggregation>();
  }

  /**
   * Transforms the builder into a list of aggregations.
   * @return the aggregations.
   */
  public List<ScanAggregation> toList() {
    return result;
  }

  /**
   * Attempt to add a date aggregation for the given attribute. The aggregation will be added if
   * [attrName]-granularity is present in the request.
   * @param attrName the name of the attribute
   */
  public void maybeAddDateAggregation(String attrName) {
    String attrGranularity = getGranularityAttr(attrName);
    if (attrGranularity != null) {
      result.add(new DateAggregation(attrName, attrGranularity));
    }
  }

  /**
   * Attempt to add a by name aggregation for the given attribute.
   * @param attrName
   */
  public void maybeAddByNameAggregation(String attrName) {
    String attrGranularity = getGranularityAttr(attrName);
    if (attrGranularity != null && attrGranularity.equals("by-name")) {
      result.add(new ByNameAggregation(attrName));
    }
  }

  /**
   * Attempt to add a integer aggregation for the given attribute. The aggregation will be added if
   * [attrName]-granularity is present in the request.
   * @param attrName the name of the attribute
   */
  public void maybeAddIntegerAggregation(String attrName) {
    String attrGranularity = getGranularityAttr(attrName);
    if (attrGranularity != null) {
      result.add(new IntegerAggregation(attrName, attrGranularity));
    }
  }

  /**
   * Attempt to add a string prefix aggregation for the given attribute. The aggregation will be added if
   * [attrName]-length is present in the request.
   * @param attrName the name of the attribute
   */
  public void maybeAddStringPrefixAggregation(String attrName) {
    String length = req.getParameter(attrName + "-length");
    if (length != null) {
      result.add(new StringPrefixAggregation(attrName, length));
    }
  }

  private String getGranularityAttr(String attrName) {
    return req.getParameter(attrName + "-granularity");
  }
}
