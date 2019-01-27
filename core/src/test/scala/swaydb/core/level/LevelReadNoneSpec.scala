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

  //  override def deleteFiles = false

  val keyValuesCount = 1000

  val times = 2

  "return None" when {

    "level is empty" in {
      assertOnLevel(
        level0KeyValues =
          (_, _, _) =>
            Slice.empty,

        assertAllLevels =
          (_, _, _, level) =>
            Seq(
              () => level.get(randomStringOption).assertGetOpt shouldBe empty,
              () => level.higher(randomStringOption).assertGetOpt shouldBe empty,
              () => level.lower(randomStringOption).assertGetOpt shouldBe empty,
              () => level.head.assertGetOpt shouldBe empty,
              () => level.last.assertGetOpt shouldBe empty
            ).runThisRandomlyInParallel
      )
    }

    "level is nonEmpty but contains no put" in {
      runThisParallel(times) {
        assertOnLevel(
          level0KeyValues =
            (_, _, timeGenerator) =>
              randomizedKeyValues(keyValuesCount, addPut = false, startId = Some(0))(timeGenerator).toMemory,

          assertAllLevels =
            (level0KeyValues, _, _, level) =>
              assertEmpty(level0KeyValues, level)
        )
      }
    }

    "level contains expired puts" in {
      runThisParallel(times) {
        assertOnLevel(
          level0KeyValues =
            (_, _, timeGenerator) =>
              (1 to keyValuesCount).map {
                key =>
                  randomPutKeyValue(key, deadline = Some(expiredDeadline()))(timeGenerator)
              }(collection.breakOut),

          assertAllLevels =
            (level0KeyValues, _, _, level) =>
              assertEmpty(level0KeyValues, level)
        )
      }
    }

    "level is non empty but the searched key do not exist" in {
      runThisParallel(times) {
        assertOnLevel(
          level0KeyValues =
            (_, _, timeGenerator) =>
              randomizedKeyValues(keyValuesCount)(timeGenerator).toMemory,

          assertLevel0 =
            (level0KeyValues, _, _, level) => {
              val existing = unexpiredPuts(level0KeyValues)

              import swaydb.data.order.KeyOrder.default._
              val nonExistingKeys: List[Int] =
                (level0KeyValues.head.key.readInt() - 100 to getMaxKey(level0KeyValues.last.toTransient).maxKey.readInt() + 100)
                  .filterNot(intKey => existing.exists(_.key equiv Slice.writeInt(intKey)))
                  .toList

              Seq(
                () => assertGetNone(nonExistingKeys, level),
                () => assertReads(existing, level),
                () =>
                  nonExistingKeys foreach {
                    nonExistentKey =>
                      val expectedHigher = existing.find(_.key.readInt() > nonExistentKey).map(_.key.readInt())
                      level.higher(nonExistentKey).withRetry.assertGetOpt.map(_.key.readInt()) shouldBe expectedHigher
                  },
                () =>
                  nonExistingKeys foreach {
                    nonExistentKey =>
                      val expectedLower = existing.reverse.find(_.key.readInt() < nonExistentKey).map(_.key.readInt())
                      level.lower(nonExistentKey).withRetry.assertGetOpt.map(_.key.readInt()) shouldBe expectedLower
                  }
              ).runThisRandomlyInParallel
            }
        )
      }
    }

    "for remove ranges with expired Put fromValue" in {
      runThisParallel(times) {
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
            (keyValues, _, _, level) =>
              assertEmpty(keyValues, level)
        )
      }
    }

    "put existed but was removed or expired" in {
      runThisParallel(times) {
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
            (level0KeyValues, level1KeyValues, level2KeyValues, level) =>
              assertEmpty(level1KeyValues, level),

          assertLevel1 =
            (level1KeyValues, level2KeyValues, level) =>
              assertGet(level1KeyValues, level)
        )
      }
    }
  }
}
