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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.level

import org.scalamock.scalatest.MockFactory
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.data.{Memory, Value}
import swaydb.core.segment.ReadState
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class LevelReadNoneSpec0 extends LevelReadNoneSpec

class LevelReadNoneSpec1 extends LevelReadNoneSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelReadNoneSpec2 extends LevelReadNoneSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelReadNoneSpec3 extends LevelReadNoneSpec {
  override def inMemoryStorage = true
}

sealed trait LevelReadNoneSpec extends TestBase with MockFactory {

  //  override def deleteFiles = false

  val keyValuesCount = 100

  val times = 1

  "return None" when {

    "level is empty" in {
      assertLevel(
        level0KeyValues =
          (_, _, _) =>
            Slice.empty,

        assertAllLevels =
          (_, _, _, level) =>
            Seq(
              () => level.get(randomStringOption, ReadState.random).runRandomIO.right.value shouldBe empty,
              () => level.higher(randomStringOption, ReadState.random).runRandomIO.right.value shouldBe empty,
              () => level.lower(randomStringOption, ReadState.random).runRandomIO.right.value shouldBe empty,
              () => level.head(ReadState.random).runRandomIO.right.value shouldBe empty,
              () => level.last(ReadState.random).runRandomIO.right.value shouldBe empty
            ).runThisRandomly
      )
    }

    "level is nonEmpty but contains no put" in {
      runThis(times) {
        assertLevel(
          level0KeyValues =
            (_, _, testTimer) =>
              randomizedKeyValues(keyValuesCount, addPut = false, addUpdates = true, startId = Some(0))(testTimer).toMemory,

          assertAllLevels =
            (level0KeyValues, _, _, level) =>
              assertEmpty(level0KeyValues, level)
        )
      }
    }

    "level contains expired puts" in {
      runThis(times) {
        assertLevel(
          level0KeyValues =
            (_, _, testTimer) =>
              (1 to keyValuesCount).map {
                key =>
                  randomPutKeyValue(key, deadline = Some(expiredDeadline()))(testTimer)
              }(collection.breakOut),

          assertAllLevels =
            (level0KeyValues, _, _, level) =>
              assertEmpty(level0KeyValues, level)
        )
      }
    }

    "level is non empty but the searched key do not exist" in {
      runThis(10.times) {
        implicit val keyOrder = KeyOrder.integer

        assertLevel(
          level0KeyValues =
            (_, _, testTimer) =>
              randomizedKeyValues(keyValuesCount, startId = Some(1))(testTimer).toMemory,

          assertLevel0 =
            (level0KeyValues, _, _, level) => {
              val existing = unexpiredPuts(level0KeyValues)

              import keyOrder._
              val nonExistingKeys: List[Int] =
                (level0KeyValues.head.key.readInt() - 100 to getMaxKey(level0KeyValues.last.toTransient).maxKey.readInt() + 100)
                  .filterNot(intKey => existing.exists(_.key equiv Slice.writeInt(intKey)))
                  .toList

              Seq(
                () => assertGetNone(nonExistingKeys, level),
                () => assertGet(existing, level),
                () => assertReads(existing, level),
                () => assertReads(existing, level),
                () =>
                  nonExistingKeys foreach {
                    nonExistentKey =>
                      val expectedHigher = existing.find(put => put.hasTimeLeft() && put.key.readInt() > nonExistentKey).map(_.key.readInt())
                      level.higher(nonExistentKey, ReadState.random).runRandomIO.right.value.map(_.key.readInt()) shouldBe expectedHigher
                  },
                () =>
                  nonExistingKeys foreach {
                    nonExistentKey =>
                      val expectedLower = existing.reverse.find(put => put.hasTimeLeft() && put.key.readInt() < nonExistentKey).map(_.key.readInt())
                      level.lower(nonExistentKey, ReadState.random).runRandomIO.right.value.map(_.key.readInt()) shouldBe expectedLower
                  }
              ).runThisRandomlyInParallel
            }
        )
      }
    }

    "for remove ranges with expired Put fromValue" in {
      runThis(times) {
        assertLevel(
          level0KeyValues =
            (_, _, testTimer) => {
              implicit val time = testTimer
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
      runThis(times) {
        assertLevel(

          level0KeyValues =
            (level1KeyValues, _, testTimer) => {
              implicit val time = testTimer
              eitherOne(
                //either do a range remove
                left =
                  randomRemoveRanges(level1KeyValues).toList.toSlice,
                //or do fixed removes via function or fixed.
                right =
                  level1KeyValues map {
                    keyValue =>
                      randomRemoveOrUpdateOrFunctionRemove(keyValue.key)
                  }
              )
            },

          level1KeyValues =
            (level2KeyValues, testTimer) => {
              implicit val time = testTimer
              level2KeyValues should have size 0

              randomPutKeyValues(keyValuesCount, startId = Some(0), addPutDeadlines = false, addExpiredPutDeadlines = false)
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
