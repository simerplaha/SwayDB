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
import swaydb.core.data.Memory
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.util.Benchmark
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.util.Random

//@formatter:off
class Get_FromMultipleLevels_Spec0 extends Get_FromMultipleLevels_Spec {
  val keyValuesCount = 1000
  val maxIterations = 100
}

class Get_FromMultipleLevels_Spec1 extends Get_FromMultipleLevels_Spec {
  val keyValuesCount = 1000
  val maxIterations = 10

  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class Get_FromMultipleLevels_Spec2 extends Get_FromMultipleLevels_Spec {
  val keyValuesCount = 1000
  val maxIterations = 10
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class Get_FromMultipleLevels_Spec3 extends Get_FromMultipleLevels_Spec {
  val keyValuesCount = 1000
  val maxIterations = 1000
  override def inMemoryStorage = true
}
//@formatter:on

sealed trait Get_FromMultipleLevels_Spec extends TestBase with MockFactory with Benchmark {

  //@formatter:off
  implicit override val groupingStrategy: Option[KeyValueGroupingStrategyInternal] = randomCompressionTypeOption(keyValuesCount)
  override implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default
  def keyValuesCount: Int
  def maxIterations: Int
  //@formatter:on

  "Get" should {
    "empty Level" in {
      assertOnLevel(
        upperLevelKeyValues = Slice.empty,
        lowerLevelKeyValues = Slice.empty,
        assertion = _.get(1).assertGetOpt shouldBe empty
      )
    }
  }

  /**
    * These tests randomly generate key-values for upper and lower levels and test their Get results.
    *
    * A range key-value give a specific key can be converted in to a Fixed key-value which represents the
    * value of that key in the Range so these tests always convert the upper & lower Level key-values to Memory
    * key-value to keep the tests simple. But they randomly select Range or Fixed key-value for upper and lower Levels.
    *
    */

  //Converts the input Range key-value to Fixed else returns the input key-value.
  def toFixed(keyValue: Memory): Memory.Fixed =
    keyValue match {
      case range: Memory.Range =>
        range.fromValue.getOrElse(range.rangeValue).toMemory(range.fromKey)

      case keyValue: Memory.Fixed =>
        keyValue
    }

  "Get" when {
    "Left - upper Level key has exact matching key in lower Level" in {
      //run this test multiple times to execute on multiple possible combinations
      runThis(maxIterations.times) {
        //randomly generate a key-value
        val upperLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(1, 10) else randomFixedKeyValue(1)
        val lowerLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(1, 10) else randomFixedKeyValue(1)

        println
        println("upperLevelKeyValue: " + upperLevelKeyValue)
        println("lowerLevelKeyValue: " + lowerLevelKeyValue)

        assertOnLevel(
          upperLevelKeyValues = Slice(upperLevelKeyValue),
          lowerLevelKeyValues = Slice(lowerLevelKeyValue),
          assertion =
            level =>
              (toFixed(upperLevelKeyValue), toFixed(lowerLevelKeyValue)) match {
                //If remove hasTimeLeft and lower Level's range has Put which is also not expired, then get should return the Put's value.
                case (remove: Memory.Remove, put: Memory.Put) if remove.hasTimeLeft() && put.hasTimeLeft() =>
                  //expected Put will contain Remove's deadline if deadline exists else put's deadline.
                  val expectedPut = remove.deadline.map(put.updateDeadline) getOrElse put
                  level.get(1).assertGet shouldBe expectedPut
                  (2 to 15) foreach (i => level.get(i).assertGetOpt shouldBe empty)

                case (_: Memory.Remove, _) => //else remove has not time left. If from value is not Put all key-values should return empty since rangeValues are not Put.
                  (1 to 15) foreach (i => level.get(i).assertGetOpt shouldBe empty)

                case (put: Memory.Put, _) =>
                  if (put.hasTimeLeft())
                    level.get(1).assertGet shouldBe put
                  else
                    level.get(1).assertGetOpt shouldBe empty

                  (2 to 15) foreach (i => level.get(i).assertGetOpt shouldBe empty)

                case (update: Memory.Update, put: Memory.Put) if update.hasTimeLeft() && put.hasTimeLeft() =>
                  val expected =
                    if (update.deadline.isDefined)
                      update.toPut()
                    else
                      put.deadline.map(update.toPut) getOrElse update.toPut()

                  level.get(1).assertGet shouldBe expected

                  (2 to 15) foreach (i => level.get(i).assertGetOpt shouldBe empty)

                case (_: Memory.Update, _) =>
                  (1 to 15) foreach (i => level.get(i).assertGetOpt shouldBe empty)
              }
        )
      }
    }

    "Mid - upper Level key overlaps mid of lower levels key range" in {
      //run this test multiple times to execute on multiple possible combinations
      runThis(maxIterations.times) {
        val upperLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(5, 10) else randomFixedKeyValue(5)
        val lowerLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(1, 10) else randomFixedKeyValue(1)

        println
        println("upperLevelKeyValue: " + upperLevelKeyValue)
        println("lowerLevelKeyValue: " + lowerLevelKeyValue)

        assertOnLevel(
          upperLevelKeyValues = Slice(upperLevelKeyValue),
          lowerLevelKeyValues = Slice(lowerLevelKeyValue),
          assertion =
            level =>
              (toFixed(upperLevelKeyValue), toFixed(lowerLevelKeyValue)) match {
                case (_: Memory.Remove, _) =>
                  (5 to 15) foreach (i => level.get(i).assertGetOpt shouldBe empty)

                case (put: Memory.Put, _) =>
                  if (put.hasTimeLeft())
                    level.get(5).assertGet shouldBe put
                  else
                    level.get(5).assertGetOpt shouldBe empty

                  (6 to 15) foreach (i => level.get(i).assertGetOpt shouldBe empty)

                case (_: Memory.Update, _) =>
                  (5 to 15) foreach (i => level.get(i).assertGetOpt shouldBe empty)
              }
        )
      }
    }

    "Right - upper level key does not overlap lower level" in {
      //run this test multiple times to execute on multiple possible combinations
      runThis(maxIterations.times) {
        //10 equals range's toKey and toKeys are exclusive. Randomly select between toKey or toKey + 1
        val key = if (Random.nextBoolean()) 10 else 11
        val upperLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(key, 20) else randomFixedKeyValue(key)
        val lowerLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(1, 10) else randomFixedKeyValue(1)

        println
        println("upperLevelKeyValue: " + upperLevelKeyValue)
        println("lowerLevelKeyValue: " + lowerLevelKeyValue)

        assertOnLevel(
          upperLevelKeyValues = Slice(upperLevelKeyValue),
          lowerLevelKeyValues = Slice(lowerLevelKeyValue),
          assertion =
            level =>
              toFixed(upperLevelKeyValue) match {
                case _: Memory.Remove =>
                  level.get(key).assertGetOpt shouldBe empty

                case put: Memory.Put =>
                  if (put.hasTimeLeft())
                    level.get(key).assertGet shouldBe put
                  else
                    level.get(key).assertGetOpt shouldBe empty

                case _: Memory.Update =>
                  level.get(key).assertGetOpt shouldBe empty
              }
        )
      }
    }

  }

}
