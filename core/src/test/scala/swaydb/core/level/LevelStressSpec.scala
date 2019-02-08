/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.level

import swaydb.core.TestBase
import swaydb.core.actor.TestActor
import swaydb.core.data.KeyValue
import swaydb.core.data.KeyValue.WriteOnly
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.level.actor.LevelCommand
import swaydb.core.level.actor.LevelCommand._
import swaydb.core.util.Benchmark
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.data.order.KeyOrder
import scala.concurrent.Future
import scala.concurrent.duration._
import swaydb.data.io.IO.Failure
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.IOAssert._
import swaydb.data.io.IO

//@formatter:off
class LevelStressSpec0 extends LevelStressSpec

class LevelStressSpec1 extends LevelStressSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelStressSpec2 extends LevelStressSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelStressSpec3 extends LevelStressSpec {
  override def inMemoryStorage = true
}
//@formatter:on

sealed trait LevelStressSpec extends TestBase with Benchmark {

  implicit val keyOrder = KeyOrder.default

  "A 4 leveled database" should {
    "concurrently reads and write records that are written in batches and concurrently pushed to lower levels." +
      "All KeyValues should eventually end up in last level" in {

      val keyValueCount = 1000
//      val keyValueCount = 100000
      val iterations = 5
      implicit val groupingStrategy: Option[KeyValueGroupingStrategyInternal] = randomGroupingStrategyOption(keyValueCount)

      val level4 = TestLevel(segmentSize = 2.mb)
      val level3 = TestLevel(segmentSize = 2.mb, nextLevel = Some(level4))
      val level2 = TestLevel(segmentSize = 2.mb, nextLevel = Some(level3))
      val level1 = TestLevel(segmentSize = 2.mb, nextLevel = Some(level2))

      val testSegmentsDir = createRandomIntDirectory

      def doPut: Unit = {
        val keyValues: Slice[KeyValue.WriteOnly] = randomKeyValues(keyValueCount)
        val segment = TestSegment(keyValues, path = testSegmentsDir.resolve(nextSegmentId)).unsafeGet
        val replyTo = TestActor[LevelCommand]()
        level1 ! PushSegments(Seq(segment), replyTo)
        replyTo.getMessage(20.seconds) match {
          case PushSegmentsResponse(request, result) =>
            segment.delete.assertGet
            result match {
              case IO.Failure(IO.Error.OverlappingPushSegment) =>
                println("BUSY SEGMENTS")
                sleep(100.milliseconds)
                doPut

              case IO.Failure(error) =>
                error.toException.printStackTrace()
                fail(error.toException)

              case _ =>
                //dispatch a read future for the put.
                Future(assertGet(keyValues, level1))
                println("PUT SUCCESSFUL")
            }
        }
      }

      (1 to iterations) foreach (_ => doPut)
    }
  }
}
