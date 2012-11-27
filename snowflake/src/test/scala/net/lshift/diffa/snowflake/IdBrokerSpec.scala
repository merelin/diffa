package net.lshift.diffa.snowflake

import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalacheck.Prop
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.ScalaCheck
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IdBrokerSpec extends SpecificationWithJUnit with ScalaCheck {

  def retryEvery(retryInterval: Long)(fn: => Unit) {
    try {
      fn
    } catch {
      case ex: SequenceExhaustedException =>
        Thread.`yield`()
        retryEvery(retryInterval)(fn)
    }
  }

  def getWithRetry(broker: IdBroker): Long = {
    try {
      broker.getId()
    } catch {
      case ex: SequenceExhaustedException =>
        Thread.sleep(1L)
        getWithRetry(broker)
    }
  }

  "IdBroker" should {

    "generate an ID" in {
      val machineId = 1
      val broker = new IdBroker(machineId)
      (broker.getId should beGreaterThan(0L))
    }

    "generate 1,000,000 identifiers in less than one second" in {
      val broker = new IdBroker(1)
      val targetExecutionCount = 1000000
      val targetExecutionTime = 1000L // 1000 milliseconds == one second

      val startTime = System.currentTimeMillis()
      (1 to targetExecutionCount) foreach { i =>
        retryEvery(100L) { broker.getId() }
      }
      println("Generated %d in %d milliseconds".format(targetExecutionCount, System.currentTimeMillis() - startTime))
      (System.currentTimeMillis() - startTime) must be lessThan targetExecutionTime
    }

    "generate at time T an ID that has a timestamp component of T" in {
      val ts = Gen.choose(1, IdBroker.timestampUpperBound)

      check(Prop.forAll(ts) { timestamp =>
        val broker = new IdBroker(1)
        broker.timeFn = new TimeFunction { def now = timestamp }
        ((broker.getId() & IdBroker.timestampMask) >> (IdBroker.machineBits + IdBroker.sequenceBits)) must_== timestamp
      })
    }

    "generate an ID that has a machine ID component matching the generating ID Broker" in {
      val machineId = Gen.choose(0, IdBroker.machineIdUpperBound)

      check(Prop.forAll(machineId) { id =>
        val broker = new IdBroker(id)
        ((broker.getId() & IdBroker.machineIdMask) >> IdBroker.sequenceBits) must_== id
      })
    }

    "generate the first ID with a sequence component of 0" in {
      ((new IdBroker(1)).getId & IdBroker.sequenceMask) should_== 0
    }

    "generate a second ID for the same timestamp with a sequence component of 1" in {
      val broker = new IdBroker(1)
      broker.timeFn = new TimeFunction { def now = 1L }

      broker.getId()
      (broker.getId() & IdBroker.sequenceMask) should_== 1
    }

    "generate the (k+1)th ID for the same timestamp with a sequence component of k" in {
      val seqNum = Gen.choose(1, IdBroker.sequenceUpperBound)

      check(Prop.forAll(seqNum) { k =>
        val broker = new IdBroker(1)
        broker.timeFn = new TimeFunction { def now = 1L }

        (1 to k) foreach { i => broker.getId() }
        (broker.getId() & IdBroker.sequenceMask) must_== k
      })
    }

    "serialise requests to generate an ID" in {
      val pauseGen = Gen.choose(50, 100)

      check(Prop.forAllNoShrink(pauseGen) { pause =>
        val broker = new IdBroker(1)
        broker.setPauseMs(pause)
        val otherThread = new Thread(new Runnable() {
          def run() {
            broker.getId()
          }
        })
        val startTime = System.currentTimeMillis()
        otherThread.start()
        broker.getId()
        otherThread.join()
        (System.currentTimeMillis() - startTime) must be greaterThanOrEqualTo (2 * pause)
      })(set(minTestsOk -> 5))
    }

    "generate k-ordered identifiers over a period" in {
      val broker = new IdBroker(1)
      val period = 200L // milliseconds
      var lastId = -1L

      val startTime = System.currentTimeMillis()
      while (elapsedTime < period) {
        val nextId = getWithRetry(broker)
        nextId must be greaterThan lastId
        lastId = nextId
      }

      def elapsedTime = System.currentTimeMillis() - startTime
    }

    "generate only unique identifiers, even when time runs backwards" in {
      val broker = new IdBroker(1)
      broker.timeFn = new TimeFunction { def now = 3L }
      val id = broker.getId()
      broker.timeFn = new TimeFunction { def now = 2L }
      try {
        broker.getId()
        failure("getId() should throw an InvalidSystemClockException when its clock runs backwards")
      } catch {
        case ex: InvalidSystemClockException =>
        case x => throw x
      }
    }
  }
}
