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
import akka.actor.{Actor, Scheduler}
import net.jcip.annotations.ThreadSafe
import java.util.concurrent.ScheduledFuture
import net.lshift.diffa.kernel.differencing._
import collection.mutable.Queue
import net.lshift.diffa.kernel.events.{VersionID, PairChangeEvent}
import net.lshift.diffa.kernel.participants.{Participant, DownstreamParticipant, UpstreamParticipant}
import org.joda.time.DateTime
import net.lshift.diffa.kernel.util.AlertCodes
import java.lang.Exception
import com.eaio.uuid.UUID
import javax.persistence.criteria.CriteriaBuilder.Case

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

  private var currentDiffListener:DifferencingListener = null
  private var currentScanListener:PairSyncListener = null
  private var upstreamSuccess = false
  private var downstreamSuccess = false


  lazy val writer = store.openWriter()

  /**
   * A queue of deferred messages that arrived during a scanning state
   */
  private val deferred = new Queue[Deferrable]

  var lastUUID = new UUID

  abstract case class TraceableCommand(uuid:UUID) {
    var exception:Throwable = null
    if (logger.isTraceEnabled) {
      exception = new Exception().fillInStackTrace()
    }
  }

  abstract case class VersionCorrelationWriterCommand(guid:UUID) extends TraceableCommand(guid)
  case class ClearDownstreamVersion(u:UUID, id: VersionID) extends VersionCorrelationWriterCommand(u)
  case class ClearUpstreamVersion(u:UUID, id: VersionID) extends VersionCorrelationWriterCommand(u)
  case class StoreDownstreamVersion(u:UUID, id: VersionID, attributes: Map[String, TypedAttribute], lastUpdated: DateTime, uvsn: String, dvsn: String) extends VersionCorrelationWriterCommand(u)
  case class StoreUpstreamVersion(u:UUID, id: VersionID, attributes: Map[String, TypedAttribute], lastUpdated: DateTime, vsn: String) extends VersionCorrelationWriterCommand(u)

  private val writerProxy = new LimitedVersionCorrelationWriter() {
    def clearDownstreamVersion(id: VersionID) = get(self !! ClearDownstreamVersion(lastUUID,id) )
    def clearUpstreamVersion(id: VersionID) = get(self !! ClearUpstreamVersion(lastUUID,id) )
    def storeDownstreamVersion(id: VersionID, attributes: Map[String, TypedAttribute], lastUpdated: DateTime, uvsn: String, dvsn: String)
      = get(self !! StoreDownstreamVersion(lastUUID, id, attributes, lastUpdated, uvsn, dvsn) )
    def storeUpstreamVersion(id: VersionID, attributes: Map[String, TypedAttribute], lastUpdated: DateTime, vsn: String)
      = get(self !! StoreUpstreamVersion(lastUUID,id, attributes, lastUpdated, vsn) )
    def get(f:Option[Any]) = f.get.asInstanceOf[Correlation]
  }

  def handleWriterCommand(command:VersionCorrelationWriterCommand) = command match {
    case ClearDownstreamVersion(uuid:UUID, id) => self.reply(writer.clearDownstreamVersion(id))
    case ClearUpstreamVersion(uuid:UUID, id)   => self.reply(writer.clearUpstreamVersion(id))
    case StoreDownstreamVersion(uuid:UUID, id, attributes, lastUpdated, uvsn, dvsn) => self.reply(writer.storeDownstreamVersion(id, attributes, lastUpdated, uvsn, dvsn))
    case StoreUpstreamVersion(uuid:UUID, id, attributes, lastUpdated, vsn) => self.reply(writer.storeUpstreamVersion(id, attributes, lastUpdated, vsn))
  }

  override def preStart = {
    // schedule a recurring message to flush the writer
    scheduledFlushes = Scheduler.schedule(self, FlushWriterMessage, 0, changeEventQuietTimeoutMillis, MILLISECONDS)
  }

  override def postStop = scheduledFlushes.cancel(true)

  /**
   * Main receive loop of this actor
   */
  def receive = {
    case s:ScanMessage => {
      lastUUID = new UUID()
      logger.info("Starting scan %s".format(lastUUID))
      handleScanMessage(s)
      become {
        case c:VersionCorrelationWriterCommand => handleWriterCommand(c)
        case d:Deferrable                      => deferred.enqueue(d)
        case UpstreamScanSuccess(uuid)               => {
          logger.trace("Received upstream success: %s".format(uuid))
          upstreamSuccess = true
          checkForCompletion
        }
        case DownstreamScanSuccess(uuid)             => {
          logger.trace("Received downstream success: %s".format(uuid))
          downstreamSuccess = true
          checkForCompletion
        }
      }
    }
    case d:Deferrable => handleDeferrable(d)
    case c:VersionCorrelationWriterCommand =>  {
      logger.error("%s: Received command (%s) in non-scanning state - potential bug"
                  .format(AlertCodes.OUT_OF_ORDER_MESSAGE, c), c.exception)
    }
    case x            =>
      logger.error("%s: Spurious message: %s".format(AlertCodes.SPURIOUS_ACTOR_MESSAGE, x))
  }

  /**
   * Exit the scanning state and notify interested parties
   */
  def checkForCompletion = {
    if (upstreamSuccess && downstreamSuccess) {
      downstreamSuccess = false
      upstreamSuccess = false
      logger.info("Finished scan %s".format(lastUUID))

      writer.flush()
      policy.difference(pairKey, currentDiffListener)

      processBacklog(PairSyncState.UP_TO_DATE)
      unbecome()
    }
  }

  /**
   * Resets the state of the actor and processes any pending messages that may have arrived during a scan phase.
   */
  def processBacklog(state:PairSyncState) = {
    if (currentScanListener != null) {
      currentScanListener.pairSyncStateChanged(pairKey, state)
    }
    currentDiffListener = null
    currentScanListener = null
    deferred.dequeueAll(d => {self ! d; true})
  }

  def handleDeferrable(d:Deferrable) : Unit = d match {
    case c:ChangeMessage      => handleChangeMessage(c)
    case d:DifferenceMessage  => handleDifferenceMessage(d)
    case s:ScanMessage        => handleScanMessage(s)
    case FlushWriterMessage   => writer.flush()
  }

  def handleChangeMessage(message:ChangeMessage) = {
    logger.info("Handling change event: %s".format(message))
    policy.onChange(writer, message.event)
    // if no events have arrived within the timeout period, flush and clear the buffer
    if (lastEventTime < (System.currentTimeMillis() - changeEventBusyTimeoutMillis)) {
      writer.flush()
    }
    lastEventTime = System.currentTimeMillis()
  }

  def handleDifferenceMessage(message:DifferenceMessage) = {
    try {
      writer.flush()
      policy.difference(pairKey, message.diffListener)
    } catch {
      case ex => {
        logger.error("Failed to difference pair " + pairKey, ex)
      }
    }
  }

  def handleScanMessage(message:ScanMessage) = {
    // squirrel some callbacks away for invocation in subsequent receives in the scanning state
    currentDiffListener = message.diffListener
    currentScanListener = message.pairSyncListener

    message.pairSyncListener.pairSyncStateChanged(pairKey, PairSyncState.SYNCHRONIZING)

    try {
      writer.flush()

      Actor.spawn {
        doScan(us)
        self ! UpstreamScanSuccess(lastUUID)
      }

      Actor.spawn {
        doScan(ds)
        self ! DownstreamScanSuccess(lastUUID)
      }

    } catch {
      case x: Exception => {
        logger.error("Failed to initiate scan for pair: " + pairKey, x)
        processBacklog(PairSyncState.FAILED)
      }
    }
  }

  def doScan(participant:Participant) = {
    try {
      participant match {
        case u:UpstreamParticipant    => policy.scanUpstream(pairKey, writerProxy, us, currentDiffListener)
        case d:DownstreamParticipant  => policy.scanDownstream(pairKey, writerProxy, us, ds, currentDiffListener)
      }
    }
    catch {
      case x:Exception => {
        logger.error("Failed to execute scan for pair %s".format(pairKey), x)
        processBacklog(PairSyncState.FAILED)
      }
    }
  }
}

abstract class ScanResult
case class UpstreamScanSuccess(uuid:UUID) extends ScanResult
case class DownstreamScanSuccess(uuid:UUID) extends ScanResult

abstract class Deferrable
case class ChangeMessage(event: PairChangeEvent) extends Deferrable
case class DifferenceMessage(diffListener: DifferencingListener) extends Deferrable
case class ScanMessage(diffListener: DifferencingListener, pairSyncListener: PairSyncListener) extends Deferrable

/**
 * An internal command that indicates to the actor that the underlying writer should be flushed
 */
private case object FlushWriterMessage extends Deferrable

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
   * Runs a difference report based on stored data for the given pair. Does not synchronise with the participants
   * beforehand - use <code>syncPair</code> to do the sync first.
   */
  def difference(pairKey:String, diffListener:DifferencingListener)

  /**
   * TODO This is just a scan, not a sync
   * Synchronises the participants belonging to the given pair, then generates a different report.
   * Activities are performed on the underlying policy in a thread safe manner, allowing multiple
   * concurrent operations to be submitted safely against the same pair concurrently.
   */
  def syncPair(pairKey:String, diffListener:DifferencingListener, pairSyncListener:PairSyncListener) : Unit
}
