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
import swaydb.core.TestBase
import swaydb.core.data.{Memory, Value}
import swaydb.core.util.Benchmark
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._
import scala.util.Random

//@formatter:off
class Lower_FromSingleLevel_Spec0 extends Lower_FromSingleLevel_Spec

class Lower_FromSingleLevel_Spec1 extends Lower_FromSingleLevel_Spec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class Lower_FromSingleLevel_Spec2 extends Lower_FromSingleLevel_Spec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class Lower_FromSingleLevel_Spec3 extends Lower_FromSingleLevel_Spec {
  override def inMemoryStorage = true
}
//@formatter:on

trait Lower_FromSingleLevel_Spec extends TestBase with MockFactory with Benchmark {

  implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default
  val keyValuesCount = 100

  "Lower" should {
    "empty Level" in {
      assertOnLevel(
        keyValues = Slice.empty,
        assertion =
          level =>
            (1 to 10) foreach {
              i =>
                level.lower(i).assertGetOpt shouldBe empty
            }
      )
    }
  }

  "Lower Remove" when {
    "Remove None|HasTimeLeft|Expired" in {
      //deadline or no deadline. A single Remove key-value with always return empty
      runThis(10.times) {
        assertOnLevel(
          keyValues = Slice(Memory.Remove(randomIntMax(10), randomDeadlineOption)),
          assertion =
            level =>
              (0 to 10) foreach {
                i =>
                  level.lower(i).assertGetOpt shouldBe empty
              }

        )
      }
    }
  }

  "Lower Put" when {
    "Put None HasTimeLeft" in {
      assertOnLevel(
        keyValues = Slice(Memory.Put(5, None, 10.seconds.fromNow)),
        assertionWithKeyValues =
          (keyValues, level) => {
            (6 to 10) foreach { key => level.lower(key).assertGet shouldBe keyValues.head }
            (0 to 5) foreach { key => level.lower(key).assertGetOpt shouldBe empty }
          }
      )
    }

    "Put None Expired" in {
      runThis(10.times) {
        assertOnLevel(
          keyValues = Slice(Memory.Put(randomIntMax(10), None, expiredDeadline())),
          assertion =
            level =>
              (0 to 20) foreach { key => level.lower(key).assertGetOpt shouldBe empty }
        )
      }
    }

    "Put Some None" in {
      assertOnLevel(
        keyValues = Slice(Memory.Put(5, 100)),
        assertionWithKeyValues =
          (keyValues, level) => {
            (6 to 10) foreach { key => level.lower(key).assertGet shouldBe keyValues.head }
            (0 to 5) foreach { key => level.lower(key).assertGetOpt shouldBe empty }
          }
      )
    }

    "Put Some HasTimeLeft" in {
      assertOnLevel(
        keyValues = Slice(Memory.Put(5, 100, 10.seconds.fromNow)),
        assertionWithKeyValues =
          (keyValues, level) => {
            (6 to 10) foreach { key => level.lower(key).assertGet shouldBe keyValues.head }
            (0 to 5) foreach { key => level.lower(key).assertGetOpt shouldBe empty }
          }
      )
    }

    "Put Some Expired" in {
      assertOnLevel(
        keyValues = Slice(Memory.Put(5, 100, expiredDeadline())),
        assertion =
          level =>
            (0 to 20) foreach { key => level.lower(key).assertGetOpt shouldBe empty }
      )
    }
  }

  "Lower Update" when {
    "Update None HasTimeLeft" in {
      assertOnLevel(
        keyValues = Slice(Memory.Update(5, None, 10.seconds.fromNow)),
        assertion =
          level =>
            (0 to 10) foreach { key => level.lower(key).assertGetOpt shouldBe empty }
      )
    }

    "Update None Expired" in {
      assertOnLevel(
        keyValues = Slice(Memory.Update(5, None, expiredDeadline())),
        assertion =
          level =>
            (0 to 10) foreach { key => level.lower(key).assertGetOpt shouldBe empty }
      )
    }

    "Update Some None" in {
      assertOnLevel(
        keyValues = Slice(Memory.Update(5, 100)),
        assertion =
          level =>
            (0 to 10) foreach { key => level.lower(key).assertGetOpt shouldBe empty }
      )
    }

    "Update Some HasTimeLeft" in {
      assertOnLevel(
        keyValues = Slice(Memory.Update(5, 100, 10.seconds.fromNow)),
        assertion =
          level =>
            (0 to 10) foreach { key => level.lower(key).assertGetOpt shouldBe empty }
      )
    }

    "Update Some Expired" in {
      assertOnLevel(
        keyValues = Slice(Memory.Update(5, 100, expiredDeadline())),
        assertion =
          level =>
            (0 to 10) foreach { key => level.lower(key).assertGetOpt shouldBe empty }
      )
    }
  }

  "Lower Range" when {
    "randomly generated ranges" in {
      //run this test multiple times to randomly generate multiple Range key-value combinations
      runThis(100.times) {
        val range = randomRangeKeyValue(5, 10) //randomly generate a Range key-value.

        assertOnLevel(
          keyValues = Slice(range),
          assertion =
            level =>
              range.fromValue match {
                case Some(put: Value.Put) if put.hasTimeLeft() => //if the range has Put and is not expired executing get will return the key-value.
                  Random.shuffle(6 to 10) foreach { key => level.lower(key).assertGet shouldBe put.toMemory(5) }
                  (0 to 5) foreach { key => level.lower(key).assertGetOpt shouldBe empty }

                case _ =>
                  //if the fromValue is not Put, all keys in the range will return empty
                  (1 to 20) foreach { key => level.lower(key).assertGetOpt shouldBe empty }
              }
        )
      }
    }
  }
}
