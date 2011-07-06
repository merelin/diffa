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
import java.util.List;

/**
 * Handler interface that can be implemented in client libraries to allow for scanning.
 */

public interface ScanningParticipantHandler {
  /**
   * Callback to be implemented by sub-classes to determine the constraints relevant to a request.
   * @param req the request to check for constraints.
   * @return the constraints. Default implementation is to return no constraints.
   */
  List<ScanConstraint> determineConstraints(HttpServletRequest req);

  /**
   * Callback to be implemented by sub-classes to determine the aggregations relevant to a request.
   * @param req the request to check for aggregations.
   * @return the aggregations. Default implementation is to return no aggregations.
   */
  List<ScanAggregation> determineAggregations(HttpServletRequest req);

  /**
   * Callback to be implemented by sub-classes to query for data relevant to this query.
   * @param constraints the constraints to apply.
   * @param aggregations the aggregations to apply.
   * @return the result entries.
   */
  List<ScanResultEntry> doQuery(List<ScanConstraint> constraints, List<ScanAggregation> aggregations);
}
