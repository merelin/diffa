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

package net.lshift.diffa.tools.client

import net.lshift.diffa.messaging.json.AbstractRestClient
import net.lshift.diffa.kernel.frontend.wire.InvocationResult
import net.lshift.diffa.kernel.client.{Actionable, ActionableRequest, ActionsClient}
import sun.reflect.generics.reflectiveObjects.NotImplementedException
import net.lshift.diffa.kernel.config.RepairAction

class ActionsRestClient(serverRootUrl:String)
        extends AbstractRestClient(serverRootUrl, "rest/actions/")
        with ActionsClient {

  def listActions(pairKey: String): Seq[Actionable] = {
    val t = classOf[Array[Actionable]]
    rpc(pairKey, t)
  }
  
  def listEntityScopedActions(pairKey: String): Seq[Actionable] = {
    val t = classOf[Array[Actionable]]
    rpc(pairKey, t, "scope" -> RepairAction.ENTITY_SCOPE)
  }

  def listPairScopedActions(pairKey: String): Seq[Actionable] = {
    val t = classOf[Array[Actionable]]
    rpc(pairKey, t, "scope" -> RepairAction.PAIR_SCOPE)
  }

  def invoke(req:ActionableRequest) : InvocationResult = {
    val path = req.pairKey + "/" + req.actionId + "/" + req.entityId
    val p = resource.path(path)
    val response = p.post(classOf[InvocationResult])
    response
  }

}
