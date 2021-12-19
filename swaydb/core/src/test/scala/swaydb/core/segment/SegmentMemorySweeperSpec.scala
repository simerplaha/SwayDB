/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import swaydb.{ActorConfig, Benchmark, TestExecutionContext}
import swaydb.config.MemoryCache
import swaydb.core.{CoreSpecType, CoreTestSweeper}
import swaydb.core.segment.data.KeyValueTestKit._
import swaydb.core.segment.SegmentTestKit._
import swaydb.core.segment.block.SegmentBlockTestKit._
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.cache.sweeper.MemorySweeper
import swaydb.core.segment.ref.search.SegmentSearchTestKit._
import swaydb.core.CoreTestSweeper._
import swaydb.slice.order.TimeOrder
import swaydb.slice.Slice
import swaydb.testkit.RunThis._

import scala.concurrent.duration.DurationInt

/**
 * These class has tests to assert the behavior of [[MemorySweeper]] on [[swaydb.core.segment.Segment]]s.
 */
class SegmentMemorySweeperSpec extends AnyWordSpec {

  val keyValuesCount = 100

  implicit val ec = TestExecutionContext.executionContext
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long

  //  override def deleteFiles = false

  "PersistentSegment" should {
    "drop Group key-value only after it's been decompressed" in {
      runThis(10.times, log = true) {
        CoreTestSweeper {
          implicit sweeper =>
            import sweeper.testCoreFunctionStore

            //add key-values to the right of the group
            val keyValues = randomKeyValues(count = 1000, addUpdates = true, startId = Some(1))

            //set the limiter to drop key-values fast
            implicit val memorySweeper: MemorySweeper.KeyValue =
              MemorySweeper(MemoryCache.KeyValueCacheOnly(1, None, Some(ActorConfig.TimeLoop("", 2.seconds, ec))))
                .get
                .asInstanceOf[MemorySweeper.KeyValue]
                .sweep()

            implicit val coreSpecType: CoreSpecType = CoreSpecType.random()

            //create persistent Segment
            val segment =
              GenSegment(
                keyValues = keyValues,
                segmentConfig = SegmentBlockConfig.random(cacheBlocksOnCreate = false, mmap = mmapSegments)
              )

            //initially Segment's cache is empty
            segment.areAllCachesEmpty shouldBe true

            //read all key-values and this should trigger dropping of key-values
            //read sequentially so that groups are added to the queue in sequential and also dropped.
            Benchmark("Reading all key-values sequentially.") {
              assertGetSequential(keyValues, segment)
            }

            //eventually all other key-values are dropped and the group remains.
            eventual(4.seconds)(segment.cachedKeyValueSize shouldBe 0)

            segment.close()
        }
      }
    }
  }
}
