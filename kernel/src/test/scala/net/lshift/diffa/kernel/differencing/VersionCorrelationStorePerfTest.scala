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

package net.lshift.diffa.kernel.differencing

import org.junit.Assert._
import org.junit.Assume._
import org.hamcrest.CoreMatchers._
import org.junit.{Test, Before}
import net.lshift.diffa.kernel.util.Dates._
import org.joda.time.DateTime
import net.lshift.diffa.kernel.events._
import org.slf4j.LoggerFactory
import net.lshift.diffa.kernel.indexing.LuceneVersionCorrelationStore
import ch.qos.logback.classic.Level
import net.lshift.diffa.kernel.participants.DateRangeConstraint

/**
 * Performance test for the version correlation store.
 */

class VersionCorrelationStorePerfTest {
  private def attributes(idx:Int) = Map("bizDate" -> DateAttribute(JUL_8_2010_1.plusSeconds(idx)), "someInt" -> IntegerAttribute(idx))

  @Before
  def checkPerformanceTestingEnabled {
    assumeThat(System.getProperty("diffa.perftest"), is(equalTo("1")))

    // Disable the VersionCorrelationStore logging
    val attrLogger = LoggerFactory.getLogger(classOf[LuceneVersionCorrelationStore]).asInstanceOf[ch.qos.logback.classic.Logger]
    attrLogger.setLevel(Level.ERROR)
  }

  @Before
  def cleanupStore {
    LuceneVersionCorrelationStoreTest.flushStore
  }

  private val stores = LuceneVersionCorrelationStoreTest.stores
  private val pairKey = "ab"

  @Test
  def canQueryWithLargeNumbersOfMatchingCorrelations() {
    val vsnCount = Integer.valueOf(System.getProperty("diffa.perf.versionCount", "1000")).intValue

    withTiming("load upstream versions") {
      val session = stores(pairKey).startSession()
      for (i <- 0 until vsnCount) {
        session.storeUpstreamVersion(VersionID(pairKey, "id" + i), attributes(i), JUL_1_2010_1, "vsn" + i)
      }
      session.flush()
    }

    withTiming("run unmatched version query") {
      val res = stores(pairKey).unmatchedVersions(Seq(DateRangeConstraint("bizDate", JUL_2010, END_JUL_2010)), Seq())
      println("Retrieved " + res.length + " unmatched versions")
      assertEquals(vsnCount, res.length)
    }

    withTiming("load downstream versions") {
      val session = stores(pairKey).startSession()
      for (i <- 0 until vsnCount) {
        session.storeDownstreamVersion(VersionID(pairKey, "id" + i), attributes(i), JUL_1_2010_1, "vsn" + i, "dvsn" + i)
      }
      session.flush()
    }

    withTiming("run unmatched version query (2)") {
      val res = stores(pairKey).unmatchedVersions(Seq(DateRangeConstraint("bizDate", JUL_2010, END_JUL_2010)), Seq())
      println("Retrieved " + res.length + " unmatched versions")
      assertEquals(0, res.length)
    }
  }

  private def withTiming(name:String)(f: => Unit):Unit = {
    val start = new DateTime

    println("Starting to " + name)
    val result = f

    println("Took " + ((new DateTime).getMillis - start.getMillis) + "ms to " + name)
  }
}