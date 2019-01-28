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

import org.scalamock.scalatest.MockFactory
import scala.util.Random
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.TryAssert._
import swaydb.core.data.{Memory, Time, Value}
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.util.Benchmark
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

//@formatter:off
class LevelReadSomeSpec0 extends LevelReadSomeSpec

class LevelReadSomeSpec1 extends LevelReadSomeSpec {
  override def levelFoldersCount = 10

  override def mmapSegmentsOnWrite = true

  override def mmapSegmentsOnRead = true

  override def level0MMAP = true

  override def appendixStorageMMAP = true
}

class LevelReadSomeSpec2 extends LevelReadSomeSpec {
  override def levelFoldersCount = 10

  override def mmapSegmentsOnWrite = false

  override def mmapSegmentsOnRead = false

  override def level0MMAP = false

  override def appendixStorageMMAP = false
}

class LevelReadSomeSpec3 extends LevelReadSomeSpec {
  override def inMemoryStorage = true
}

//@formatter:on

sealed trait LevelReadSomeSpec extends TestBase with MockFactory with Benchmark {

  implicit def groupingStrategy: Option[KeyValueGroupingStrategyInternal] = randomGroupingStrategyOption(keyValuesCount)

  //  override def deleteFiles = false

  val keyValuesCount = 10000

  val times = 10

  "return Put" when {

    "level has valid puts" in {
      runThisParallel(times) {
        assertOnLevel(
          level0KeyValues =
            (_, _, timeGenerator) =>
              randomPutKeyValues(keyValuesCount)(timeGenerator),

          assertLevel0 =
            (level0KeyValues, _, _, level) =>
              assertReads(level0KeyValues, level)
        )
      }
    }

//    "contains put that were updated" in {
//      runThisParallel(times) {
//        val updatedValue = randomStringOption
//        val deadline = randomDeadlineOption(false)
//
//        assertOnLevel(
//          level0KeyValues =
//            (level1KeyValues, level2KeyValues, timeGenerator) => {
//              val puts = unexpiredPuts(level2KeyValues ++ level1KeyValues)
//              randomUpdate(puts, updatedValue, deadline, false)(timeGenerator)
//            },
//
//          level1KeyValues =
//            (level2KeyValues, timeGenerator) =>
//              randomizedKeyValues(keyValuesCount, startId = Some(maxKey(Slice(level2KeyValues.last.toTransient)).maxKey.readInt() + 10000))(timeGenerator).toMemory,
//
//          level2KeyValues =
//            timeGenerator =>
//              randomizedKeyValues(keyValuesCount, startId = Some(0))(timeGenerator).toMemory,
//
//          assertLevel0 =
//            (level0KeyValues, level1KeyValues, _, level) =>
//              level0KeyValues foreach {
//                update =>
//                  try {
//                    val got = level.get(update.key).withRetry.assertGet
//                    got.getOrFetchValue.assertGetOpt shouldBe updatedValue
//                    //check if deadline was updated
//                    deadline foreach {
//                      deadline =>
//                        got.deadline should contain(deadline)
//                    }
//                  } catch {
//                    case exception: Exception =>
//                      println("debug")
//                      throw exception
//                  }
//              }
//
//        )
//      }
//    }

  }
}
