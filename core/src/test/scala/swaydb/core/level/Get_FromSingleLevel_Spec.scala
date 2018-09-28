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
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.util.Benchmark
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

//@formatter:off
class Get_FromSingleLevel_Spec0 extends Get_FromSingleLevel_Spec

class Get_FromSingleLevel_Spec1 extends Get_FromSingleLevel_Spec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class Get_FromSingleLevel_Spec2 extends Get_FromSingleLevel_Spec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class Get_FromSingleLevel_Spec3 extends Get_FromSingleLevel_Spec {
  override def inMemoryStorage = true
}
//@formatter:on

sealed trait Get_FromSingleLevel_Spec extends TestBase with MockFactory with Benchmark {

  override implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default
  implicit override val groupingStrategy: Option[KeyValueGroupingStrategyInternal] = randomCompressionTypeOption(keyValuesCount)
  val keyValuesCount = 100

  "Get" should {
    "empty Level" in {
      assertOnLevel(
        keyValues = Slice.empty,
        assertion =
          level =>
            (1 to 10) foreach {
              i =>
                level.get(i).assertGetOpt shouldBe empty
            }
      )
    }
  }

  "Getting Remove" when {
    "Remove None" in {
      assertOnLevel(
        keyValues = Slice(Memory.Remove(1)),
        assertion = _.get(1).assertGetOpt shouldBe empty
      )
    }

    "Remove HasTimeLeft" in {
      assertOnLevel(
        keyValues = Slice(Memory.Remove(1, 10.seconds.fromNow)),
        assertion = _.get(1).assertGetOpt shouldBe empty
      )
    }

    "Remove HasNoTimeLeft" in {
      assertOnLevel(
        keyValues = Slice(Memory.Remove(1, expiredDeadline())),
        assertion = _.get(1).assertGetOpt shouldBe empty
      )
    }
  }

  "Getting Put" when {
    "Put None HasTimeLeft" in {
      assertOnLevel(
        keyValues = Slice(Memory.Put(1, None, 10.seconds.fromNow)),
        assertionWithKeyValues =
          (keyValues, level) =>
            level.get(keyValues.head.key).assertGet shouldBe keyValues.head
      )
    }

    "Put None Expired" in {
      assertOnLevel(
        keyValues = Slice(Memory.Put(1, None, expiredDeadline())),
        assertion = _.get(1).assertGetOpt shouldBe empty
      )
    }

    "Put Some None" in {
      assertOnLevel(
        keyValues = Slice(Memory.Put(1, 100)),
        assertionWithKeyValues =
          (keyValues, level) =>
            level.get(keyValues.head.key).assertGet shouldBe keyValues.head
      )
    }

    "Put Some HasTimeLeft" in {
      assertOnLevel(
        keyValues = Slice(Memory.Put(1, 100, 10.seconds.fromNow)),
        assertionWithKeyValues =
          (keyValues, level) =>
            level.get(keyValues.head.key).assertGet shouldBe keyValues.head
      )
    }

    "Put Some Expired" in {
      assertOnLevel(
        keyValues = Slice(Memory.Put(1, 100, expiredDeadline())),
        assertion = _.get(1).assertGetOpt shouldBe empty
      )
    }
  }

  "Getting Update" when {
    "Update None HasTimeLeft" in {
      assertOnLevel(
        keyValues = Slice(Memory.Update(1, None, 10.seconds.fromNow)),
        assertion = _.get(1).assertGetOpt shouldBe empty
      )
    }

    "Update None Expired" in {
      assertOnLevel(
        keyValues = Slice(Memory.Update(1, None, expiredDeadline())),
        assertion = _.get(1).assertGetOpt shouldBe empty
      )
    }

    "Update Some None" in {
      assertOnLevel(
        keyValues = Slice(Memory.Update(1, 100)),
        assertion = _.get(1).assertGetOpt shouldBe empty
      )
    }

    "Update Some HasTimeLeft" in {
      assertOnLevel(
        keyValues = Slice(Memory.Update(1, 100, 10.seconds.fromNow)),
        assertion = _.get(1).assertGetOpt shouldBe empty
      )
    }

    "Update Some Expired" in {
      assertOnLevel(
        keyValues = Slice(Memory.Update(1, 100, expiredDeadline())),
        assertion = _.get(1).assertGetOpt shouldBe empty
      )
    }
  }

  "Getting Range" when {
    "randomly generated ranges" in {
      //run this test multiple times to randomly generate multiple Range key-value combinations
      runThis(100.times) {
        val range = randomRangeKeyValue(1, 10) //randomly generate a Range key-value.

        assertOnLevel(
          keyValues = Slice(range),
          assertion =
            level =>
              range.fromValue match {
                case Some(put: Value.Put) if put.hasTimeLeft() => //if the range has Put and is not expired executing get will return the key-value.
                  level.get(1).assertGet shouldBe put.toMemory(1)
                  //every other key in the range and outside the range is empty
                  (2 to 20) foreach {
                    i =>
                      level.get(i).assertGetOpt shouldBe empty
                  }
                case _ =>
                  //if the fromValue is not Put, all keys in the range will return empty
                  (1 to 20) foreach {
                    i =>
                      level.get(i).assertGetOpt shouldBe empty
                  }
              }
        )
      }
    }
  }
}
