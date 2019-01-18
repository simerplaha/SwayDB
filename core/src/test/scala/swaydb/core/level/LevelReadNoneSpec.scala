/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
class LevelFindNoneSpec0 extends LevelFindNoneSpec

class LevelFindNoneSpec1 extends LevelFindNoneSpec {
  override def levelFoldersCount = 10

  override def mmapSegmentsOnWrite = true

  override def mmapSegmentsOnRead = true

  override def level0MMAP = true

  override def appendixStorageMMAP = true
}

class LevelFindNoneSpec2 extends LevelFindNoneSpec {
  override def levelFoldersCount = 10

  override def mmapSegmentsOnWrite = false

  override def mmapSegmentsOnRead = false

  override def level0MMAP = false

  override def appendixStorageMMAP = false
}

class LevelFindNoneSpec3 extends LevelFindNoneSpec {
  override def inMemoryStorage = true
}

//@formatter:on

sealed trait LevelFindNoneSpec extends TestBase with MockFactory with Benchmark {

  implicit def groupingStrategy: Option[KeyValueGroupingStrategyInternal] = randomGroupingStrategyOption(keyValuesCount)

  val keyValuesCount = 1000

  "return None" when {

    "level is empty" in {
      assertOnLevel(
        level0KeyValues =
          (_, _, _) =>
            Slice.empty,

        assertAllLevels =
          (_, _, _, level) => {
            runThisParallel(10.times) {
              level.get(randomStringOption).assertGetOpt shouldBe empty
              level.higher(randomStringOption).assertGetOpt shouldBe empty
              level.lower(randomStringOption).assertGetOpt shouldBe empty
              level.head.assertGetOpt shouldBe empty
              level.last.assertGetOpt shouldBe empty
            }
          }
      )
    }

    "level is nonEmpty but contains no put" in {
      runThis(10.times) {
        assertOnLevel(
          level0KeyValues =
            (_, _, timeGenerator) =>
              randomizedKeyValues(keyValuesCount, addPut = false, startId = Some(0))(timeGenerator).toMemory,

          assertAllLevels =
            (level1KeyValues, _, _, level) => {
              assertGetNone(level1KeyValues, level)
              assertHigherNone(level1KeyValues, level)
              assertLowerNone(level1KeyValues, level)
            }
        )
      }
    }

    "level contains expired puts" in {
      runThisParallel(10.times) {
        assertOnLevel(
          level0KeyValues =
            (_, _, timeGenerator) =>
              (1 to keyValuesCount).map({
                key =>
                  randomPutKeyValue(key, deadline = Some(expiredDeadline()))(timeGenerator)
              })(collection.breakOut),

          assertAllLevels =
            (level1KeyValues, _, _, level) => {
              assertGetNone(level1KeyValues, level)
              assertHigherNone(level1KeyValues, level)
              assertLowerNone(level1KeyValues, level)
            }
        )
      }
    }

    "level is non empty but the searched key do not exist" in {
      runThis(10.times) {
        assertOnLevel(
          level0KeyValues =
            (_, _, timeGenerator) =>
              randomizedKeyValues(100, addRandomExpiredPutDeadlines = false, startId = Some(100))(timeGenerator).toMemory,

          assertAllLevels =
            (level1KeyValues, _, _, level) => {
              val existing = unexpiredPuts(level1KeyValues)

              import swaydb.data.order.KeyOrder.default._
              val nonExistingKeys: List[Int] =
                (level1KeyValues.head.key.readInt() - 100 to getMaxKey(level1KeyValues.last.toTransient).maxKey.readInt() + 100)
                  .filterNot(intKey => existing.exists(_.key equiv Slice.writeInt(intKey)))
                  .toList

              assertGetNone(nonExistingKeys, level)
              assertHigher(existing, level)
              assertLower(existing, level)

              nonExistingKeys foreach {
                nonExistentKey =>
                  val expectedHigher = existing.find(_.key.readInt() > nonExistentKey).map(_.key.readInt())
                  level.higher(nonExistentKey).assertGetOpt.map(_.key.readInt()) shouldBe expectedHigher
              }
              nonExistingKeys foreach {
                nonExistentKey =>
                  val expectedLower = existing.reverse.find(_.key.readInt() < nonExistentKey).map(_.key.readInt())
                  level.lower(nonExistentKey).assertGetOpt.map(_.key.readInt()) shouldBe expectedLower
              }
            }
        )
      }
    }

    "for remove ranges with expired Put fromValue" in {
      runThisParallel(10.times) {
        assertOnLevel(
          level0KeyValues =
            (_, _, timeGenerator) => {
              implicit val time = timeGenerator
              Slice(
                Memory.Range(1, 10, Some(Value.put(1, expiredDeadline())), Value.remove(None)),
                Memory.Range(20, 30, Some(Value.put(2, expiredDeadline())), Value.remove(None))
              )
            },

          assertAllLevels =
            (keyValues, _, _, level) => {
              assertGetNone(keyValues, level)
              assertHigherNone(keyValues, level)
              assertLowerNone(keyValues, level)
            }
        )
      }
    }

    "put existed but was removed or expired" in {
      runThisParallel(100.times) {
        assertOnLevel(

          level0KeyValues =
            (level1KeyValues, _, timeGenerator) => {
              implicit val time = timeGenerator
              eitherOne(
                //either do a range remove
                left =
                  randomRemoveRanges(level1KeyValues).toIterable.toSlice,
                //or do fixed removes via function or fixed.
                right =
                  level1KeyValues map {
                    keyValue =>
                      randomRemoveOrUpdateOrFunctionRemove(keyValue.key)
                  }
              )
            },

          level1KeyValues =
            (level2KeyValues, timeGenerator) => {
              implicit val time = timeGenerator
              level2KeyValues should have size 0

              randomPutKeyValues(keyValuesCount, startId = Some(0), addRandomPutDeadlines = false, addRandomExpiredPutDeadlines = false)
            },

          assertLevel0 =
            (level0KeyValues, level1KeyValues, level2KeyValues, level) => {
              assertGetNone(level1KeyValues, level)
              assertHigherNone(level1KeyValues, level, Some(10))
              assertLowerNone(level1KeyValues, level, Some(10))
            },

          assertLevel1 =
            (level1KeyValues, level2KeyValues, level) => {
              assertGet(level1KeyValues, level)
              assertHigher(level1KeyValues, level)
              assertLower(level1KeyValues, level)
            }
        )
      }
    }
  }
}
