/**
 * Copyright (C) 2010 LShift Ltd.
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

package net.lshift.diffa.messaging.json

import net.lshift.diffa.kernel.participants._
import JSONEncodingUtils._
import net.lshift.diffa.kernel.frontend.wire.WireResponse._
import net.lshift.diffa.kernel.frontend.wire.{WireConstraint, ConstraintRegistry, WireDigest}

/**
 * Handler for participants being queried via JSON.
 */
abstract class ParticipantHandler(val participant:Participant) extends AbstractJSONHandler {

  protected val commonEndpoints = Map(
    "query_aggregate_digests" -> skeleton((deserializeConstraints _)
                                           andThen (constraintsFromWire _)
                                           andThen (participant.queryAggregateDigests _)
                                           andThen (digestsToWire _)
                                           andThen (serializeDigests _)),

    "query_entity_versions" -> skeleton((deserializeConstraints _)
                                         andThen (constraintsFromWire _)
                                         andThen (participant.queryEntityVersions _)
                                         andThen (digestsToWire _)
                                         andThen (serializeDigests _)),

    "invoke" -> skeleton((deserializeActionRequest _)
                          andThen (req => participant.invoke(req.actionId, req.entityId))
                          andThen (serializeActionResult _)),

    "retrieve_content" -> skeleton((deserializeEntityContentRequest _)
                                    andThen (participant.retrieveContent _)
                                    andThen (serializeEntityContent _))
  )

  private def constraintsFromWire(wire: Seq[WireConstraint]) =
    wire.map(ConstraintRegistry.resolve _)

  private def digestsToWire(digests: Seq[Digest]) =
    digests.map(WireDigest.toWire _)


}

class DownstreamParticipantHandler(val downstream:DownstreamParticipant)
        extends ParticipantHandler(downstream) {

  override protected val endpoints = commonEndpoints ++ Map(
    "generate_version" -> skeleton((deserializeEntityBodyRequest _)
                                    andThen (downstream.generateVersion _)
                                    andThen (toWire _)
                                    andThen (serializeWireResponse _))
  )
}

class UpstreamParticipantHandler(val upstream:UpstreamParticipant)
        extends ParticipantHandler(upstream) {
  override protected val endpoints = commonEndpoints
}