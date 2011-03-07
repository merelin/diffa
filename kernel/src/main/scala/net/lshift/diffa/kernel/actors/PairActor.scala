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

package net.lshift.diffa.kernel.actors

import java.util.concurrent.TimeUnit.MILLISECONDS
import org.slf4j.{Logger, LoggerFactory}
import net.lshift.diffa.kernel.events.PairChangeEvent
import net.lshift.diffa.kernel.differencing.{DifferencingListener, VersionPolicy}
import net.lshift.diffa.kernel.differencing.{VersionCorrelationStore, VersionCorrelationWriter}
import se.scalablesolutions.akka.actor.{Actor, Scheduler}
import scala.collection.mutable.ListBuffer
import net.jcip.annotations.ThreadSafe
import net.lshift.diffa.kernel.participants.{DownstreamParticipant, UpstreamParticipant}
import java.util.concurrent.ScheduledFuture

/**
 * This actor serializes access to the underlying version policy from concurrent processes.
 */
case class PairActor(pairKey:String,
                     us:UpstreamParticipant,
                     ds:DownstreamParticipant,
                     policy:VersionPolicy,
                     store:VersionCorrelationStore,
                     changeEventBusyTimeoutMillis: Long,
                     changeEventQuietTimeoutMillis: Long) extends Actor {

  val logger:Logger = LoggerFactory.getLogger(getClass)

  self.id_=(pairKey)

  private var lastEventTime: Long = 0
  private var scheduledFlushes: ScheduledFuture[_] = _

  lazy val writer = store.openWriter()

  override def init {
    // schedule a recurring message to flush the writer
    scheduledFlushes = Scheduler.schedule(self, FlushWriterMessage, 0, changeEventQuietTimeoutMillis, MILLISECONDS)
  }

  override def shutdown {
    scheduledFlushes.cancel(true)
  }

  def receive = {
    case ChangeMessage(event)        => {
      policy.onChange(writer, event)

      // if no events have arrived within the timeout period, flush and clear the buffer
      if (lastEventTime < (System.currentTimeMillis() - changeEventBusyTimeoutMillis)) {
        writer.flush()
      }
      lastEventTime = System.currentTimeMillis()
    }
    case DifferenceMessage(listener) => {
      writer.flush()
      policy.difference(pairKey, writer, us, ds, listener)
    }

    case FlushWriterMessage         => {
      writer.flush()
    }
  }

}

case class ChangeMessage(event:PairChangeEvent)
case class DifferenceMessage(listener:DifferencingListener)
case object FlushWriterMessage

/**
 * This is a thread safe entry point to an underlying version policy.
 */
@ThreadSafe
trait PairPolicyClient {

  /**
   * Propagates the change event to the underlying policy implementation in a serial fashion.
   */
  def propagateChangeEvent(event:PairChangeEvent) : Unit

  /**
   * Runs a syncing difference report on the underlying policy implementation in a thread safe way.
   */
  def syncPair(pairKey:String, listener:DifferencingListener) : Unit
}
