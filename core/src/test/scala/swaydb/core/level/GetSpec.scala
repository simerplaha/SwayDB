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
import swaydb.core.util.FileUtil._
import swaydb.data.compaction.Throttle
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

//@formatter:off
class LevelGetSpec0 extends LevelGetSpec

class LevelGetSpec1 extends LevelGetSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelGetSpec2 extends LevelGetSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelGetSpec3 extends LevelGetSpec {
  override def inMemoryStorage = true
}
//@formatter:on

/**
  * These test cases have been superseded by [[Get_FromMultipleLevels_Spec]] and
  * [[Get_FromSingleLevel_Spec]] and exist as archived test cases only.
  */
trait LevelGetSpec extends TestBase with MockFactory with Benchmark {

  implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default
  val keyValuesCount = 100

  "Level.get when only upper Level contains key-values" should {
    "return None on empty Level" in {
      assertOnLevel(
        keyValues = Slice.empty,
        assertion = _.get(1).assertGetOpt shouldBe empty
      )
    }

    "return key when the key exists" in {
      assertOnLevel(
        keyValues = Slice(Memory.Put(1, "one")),
        assertion = _.get(1).assertGet shouldBe Memory.Put(1, "one")
      )
    }

    "return key when the key exists - randomly genrated key-values" in {
      assertOnLevel(
        keyValues = randomIntKeyValuesMemory(keyValuesCount),
        assertionWithKeyValues =
          (keyValues, level) =>
            keyValues foreach {
              keyValue =>
                level.get(keyValue.key).assertGet shouldBe keyValue
            }
      )
    }

    "return None when the Level is non empty but the input key does not exist" in {
      assertOnLevel(
        keyValues = Slice(Memory.Put(1, "one")),
        assertion = _.get(2).assertGetOpt shouldBe empty
      )

      assertOnLevel(
        keyValues = randomIntKeyValuesMemory(keyValuesCount),
        assertionWithKeyValues =
          (keyValues, level) => {
            level.get(keyValues.head.key.readInt() - 1).assertGetOpt shouldBe empty
            level.get(keyValues.last.key.readInt() + 1).assertGetOpt shouldBe empty
          }
      )
    }

    "return None for removed keys" in {
      assertOnLevel(
        keyValues = Slice(Memory.Remove(1)),
        assertion = _.get(1).assertGetOpt shouldBe empty
      )

      assertOnLevel(
        keyValues = randomIntKeyValuesMemory(keyValuesCount, addRandomRemoves = true).filter(_.isRemove),
        assertionWithKeyValues =
          (keyValues, level) =>
            keyValues foreach {
              keyValue =>
                level.get(keyValue.key).assertGetOpt shouldBe empty
            }
      )
    }

    "return None for removed keys with deadline in the future that do not have Put in lower level" in {
      assertOnLevel(
        keyValues = Slice(Memory.Remove(1, 10.seconds.fromNow)),
        assertion = _.get(1).assertGetOpt shouldBe empty
      )
    }

    "return Put key with expiry" in {
      val deadline = 10.seconds.fromNow
      assertOnLevel(
        keyValues = Slice(Memory.Put(1, 1, deadline)),
        assertion = _.get(1).assertGet shouldBe Memory.Put(1, 1, deadline)
      )
    }

    "return None for expired Put key" in {
      val deadline = 2.seconds.fromNow
      assertOnLevel(
        keyValues = Slice(Memory.Put(1, 1, deadline)),
        assertion =
          level => {
            sleep(2.1.seconds)
            level.get(1).assertGetOpt shouldBe empty
          }
      )
    }

    "return None for Remove Range key-value" in {
      assertOnLevel(
        keyValues = Slice(Memory.Range(1, 10, None, Value.Remove(None))),
        assertion =
          level =>
            (0 to 15) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }
      )

      assertOnLevel(
        keyValues =
          Slice(
            Memory.Range(1, 10, None, Value.Remove(None)),
            Memory.Range(10, 20, Some(Value.Remove(None)), Value.Remove(None)),
            Memory.Range(25, 30, None, Value.Remove(None))
          ),
        assertion =
          level =>
            (0 to 40) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }
      )
    }

    "return None for Remove Range with Put from value" in {
      assertOnLevel(
        keyValues = Slice(Memory.Range(1, 10, Some(Value.Put("one")), Value.Remove(None))),
        assertion =
          level => {
            level.get(0).assertGetOpt shouldBe empty
            level.get(1).assertGet shouldBe Memory.Put(1, Some("one": Slice[Byte]))
            (2 to 10) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }
          }
      )

      assertOnLevel(
        keyValues = Slice(
          Memory.Range(1, 10, Some(Value.Put("one")), Value.Remove(None)),
          Memory.Range(20, 30, Some(Value.Put("twenty")), Value.Remove(None))
        ),
        assertion =
          level => {
            level.get(0).assertGetOpt shouldBe empty
            level.get(1).assertGet shouldBe Memory.Put(1, Some("one": Slice[Byte]))
            (2 to 19) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }
            level.get(20).assertGet shouldBe Memory.Put(20, Some("twenty": Slice[Byte]))
            (21 to 32) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }
          }
      )
    }

    "return None for Remove Range with Remove fromValue" in {
      assertOnLevel(
        keyValues = Slice(Memory.Range(1, 10, Some(Value.Remove(None)), Value.Remove(None))),
        assertion =
          level =>
            (0 to 10) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }
      )

      assertOnLevel(
        keyValues = Slice(
          Memory.Range(1, 10, Some(Value.Remove(None)), Value.Remove(None)),
          Memory.Range(20, 30, Some(Value.Remove(None)), Value.Remove(None))
        ),
        assertion =
          level =>
            (0 to 35) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }
      )
    }

    "return Put Range key-value that doesn't have existing Put key-values but Range's fromValue is set to Put" in {
      assertOnLevel(
        keyValues = Slice(Memory.Range(1, 10, Some(Value.Put(1)), Value.Update(10))),
        assertion =
          level => {
            level.get(0).assertGetOpt shouldBe empty
            level.get(1).assertGet shouldBe Memory.Put(1: Slice[Byte], Some(1))

            (2 to 11) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }
          }
      )

      assertOnLevel(
        keyValues =
          Slice(
            Memory.Range(1, 10, Some(Value.Put(1)), Value.Update(10)),
            Memory.Range(20, 30, Some(Value.Put(20)), Value.Update(10))
          ),
        assertion =
          level => {
            level.get(0).assertGetOpt shouldBe empty
            level.get(1).assertGet shouldBe Memory.Put(1: Slice[Byte], Some(1))

            (2 to 19) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }

            level.get(20).assertGet shouldBe Memory.Put(20: Slice[Byte], Some(20))

            (21 to 35) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }
          }
      )
    }

    "return Put Range key-value that doesn't have existing Put key-values but Range's fromValue is set to Remove" in {
      assertOnLevel(
        keyValues = Slice(Memory.Range(1, 10, Some(Value.Remove(None)), Value.Update(10))),
        assertion =
          level =>
            (0 to 11) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }
      )

      assertOnLevel(
        keyValues =
          Slice(
            Memory.Range(1, 10, Some(Value.Remove(None)), Value.Update(10)),
            Memory.Range(20, 30, Some(Value.Remove(None)), Value.Update(10))
          ),
        assertion =
          level =>
            (0 to 40) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }
      )
    }

    "return Put Range key-value that doesn't have existing Put key-values" in {
      assertOnLevel(
        keyValues = Slice(Memory.Range(1, 10, None, Value.Update(10))),
        assertion =
          level =>
            (0 to 11) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }
      )

      assertOnLevel(
        keyValues =
          Slice(
            Memory.Range(1, 10, None, Value.Update(10)),
            Memory.Range(20, 30, None, Value.Update(10))
          ),
        assertion =
          level =>
            (0 to 40) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }
      )
    }

    "for Range, Remove, Put, Range key-value" in {
      assertOnLevel(
        keyValues =
          Slice(
            Memory.Range(1, 10, None, Value.Update(10)),
            Memory.Remove(11),
            Memory.Put(20, 20),
            Memory.Range(21, 30, None, Value.Update(30))
          ),
        assertion =
          level => {
            (0 to 19) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }

            level.get(20).assertGet shouldBe Memory.Put(20, 20)

            (21 to 35) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }
          }
      )
    }
  }

  "Level.get when both upper and lower Level contains key-values" should {

    "return None is both the Levels are empty" in {
      assertOnLevel(
        upperLevelKeyValues = Slice.empty,
        lowerLevelKeyValues = Slice.empty,
        assertion = _.get(1).assertGetOpt shouldBe empty
      )
    }

    "return Put key-value from lower Level" in {
      assertOnLevel(
        upperLevelKeyValues = Slice.empty,
        lowerLevelKeyValues = Slice(Memory.Put(1, 1)),
        assertion = _.get(1).assertGet shouldBe Memory.Put(1, 1)
      )
    }

    "return Put key-value from upper Level if both lower & upper Levels have the same key" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Put(1, "upper level")),
        lowerLevelKeyValues = Slice(Memory.Put(1, "lower level")),
        assertion = _.get(1).assertGet shouldBe Memory.Put(1, "upper level")
      )
    }

    "return Put key-value from their respective Levels" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Put(1, "upper level"), Memory.Put(2, "upper level")),
        lowerLevelKeyValues = Slice(Memory.Put(1, "lower level"), Memory.Put(3, "lower level"), Memory.Put(4, "lower level")),
        assertion =
          level => {
            level.get(1).assertGet shouldBe Memory.Put(1, "upper level")
            level.get(2).assertGet shouldBe Memory.Put(2, "upper level")
            level.get(3).assertGet shouldBe Memory.Put(3, "lower level")
            level.get(4).assertGet shouldBe Memory.Put(4, "lower level")
          }
      )
    }

    "return None for removed key-value from lower Level" in {
      assertOnLevel(
        upperLevelKeyValues = Slice.empty,
        lowerLevelKeyValues = Slice(Memory.Remove(1)),
        assertion = _.get(1).assertGetOpt shouldBe empty
      )
    }

    "return Put key-value with deadline from lower level" in {
      val deadline = 10.seconds.fromNow
      assertOnLevel(
        upperLevelKeyValues = Slice.empty,
        lowerLevelKeyValues = Slice(Memory.Put(1, 10, deadline)),
        assertion = _.get(1).assertGet shouldBe Memory.Put(1, 10, deadline)
      )
    }

    "return None for expired put key-value from lower level" in {
      val deadline = 2.seconds.fromNow
      assertOnLevel(
        upperLevelKeyValues = Slice.empty,
        lowerLevelKeyValues = Slice(Memory.Put(1, 10, deadline)),
        assertion =
          level => {
            sleep(2.1.seconds)
            level.get(1).assertGetOpt shouldBe empty
          }
      )
    }

    "return key when upper Level contains Remove that hasTimeLeft and lower level contains Put" in {
      val deadline = 1.day.fromNow
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Remove(1, deadline)),
        lowerLevelKeyValues = Slice(Memory.Put(1, 10)),
        assertion = _.get(1).assertGet shouldBe Memory.Put(1, 10, deadline)
      )
    }

    "return key when upper Level contains Put that hasTimeLeft and lower level contains expired Remove" in {
      val putDeadline = 1.day.fromNow
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Put(1, 10, putDeadline)),
        lowerLevelKeyValues = Slice(Memory.Remove(1, 1.nanosecond.fromNow)),
        assertion = _.get(1).assertGet shouldBe Memory.Put(1, 10, putDeadline)
      )
    }

    "return key when upper Level contains Remove that isOverDue and lower level contains Put" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Remove(1, 2.seconds.fromNow)),
        lowerLevelKeyValues = Slice(Memory.Put(1, 10)),
        assertion =
          level => {
            sleep(2.1.seconds)
            level.get(1).assertGetOpt shouldBe empty
          }
      )
    }

    "return None for removed Range key-value from lower Level" in {
      assertOnLevel(
        upperLevelKeyValues = Slice.empty,
        lowerLevelKeyValues = Slice(Memory.Range(1, 10, None, Value.Remove(None))),
        assertion =
          level => {
            (0 to 11) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }
          }
      )
    }

    "return None for removed Range upper Level key-value that also has valid Put key-values in lower Level" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Range(1, 10, None, Value.Remove(None))),
        lowerLevelKeyValues = (1 to 9).map(key => Memory.Put(key, key)).toSlice,
        assertion =
          level => {
            (0 to 11) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }
          }
      )
    }

    "return key-values from lower Level that do not belong to Remove range in upper Level" in {
      def doAssertion(level: Level) = {
        (0 to 4) foreach {
          key =>
            level.get(key).assertGet shouldBe Memory.Put(key, key)
        }

        (5 to 9) foreach {
          key =>
            level.get(key).assertGetOpt shouldBe empty
        }

        (10 to 15) foreach {
          key =>
            level.get(key).assertGet shouldBe Memory.Put(key, key)
        }
      }

      //    5  -  9 (removed)
      //0      -      15
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Range(5, 10, None, Value.Remove(None))),
        lowerLevelKeyValues = (0 to 15).map(key => Memory.Put(key, key)).toSlice,
        assertion = doAssertion(_)
      )

      assertOnLevel( //fromValue set to Remove should return the same result.
        upperLevelKeyValues = Slice(Memory.Range(5, 10, Some(Value.Remove(None)), Value.Remove(None))),
        lowerLevelKeyValues = (0 to 15).map(key => Memory.Put(key, key)).toSlice,
        assertion = doAssertion(_)
      )
    }

    "return key-values from lower Level that do not belong to Remove range in upper Level but fromValue is set to Put" in {
      //    5  -  9 (put - removed)
      //0      -      15
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Range(5, 10, Some(Value.Put(5)), Value.Remove(None))),
        lowerLevelKeyValues = (0 to 15).map(key => Memory.Put(key, key)).toSlice,
        assertion =
          level => {
            (0 to 5) foreach {
              key =>
                level.get(key).assertGet shouldBe Memory.Put(key, key)
            }

            (6 to 9) foreach {
              key =>
                level.get(key).assertGetOpt shouldBe empty
            }

            (10 to 15) foreach {
              key =>
                level.get(key).assertGet shouldBe Memory.Put(key, key)
            }
          }
      )
    }

    "return key-values from upper Level that contain Remove range in lower Level" in {
      //0      -      15
      //    5  -  9 (put - removed)
      assertOnLevel(
        upperLevelKeyValues = (0 to 15).map(key => Memory.Put(key, key)).toSlice,
        lowerLevelKeyValues = Slice(Memory.Range(5, 10, Some(Value.Put(5)), Value.Remove(None))),
        assertion =
          level => {
            (0 to 15) foreach {
              key =>
                level.get(key).assertGet shouldBe Memory.Put(key, key)
            }
          }
      )
    }

    "return key-values with update value when upper Level contains Range key-value and lower Level contains actual Put key-values" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Range(5, 10, Some(Value.Put("five")), Value.Update("upper level"))),
        lowerLevelKeyValues = (0 to 15).map(key => Memory.Put(key, key)).toSlice,
        assertion =
          level => {
            (0 to 4) foreach {
              key =>
                level.get(key).assertGet shouldBe Memory.Put(key, key)
            }
            level.get(5).assertGet shouldBe Memory.Put(5, "five")

            (6 to 9) foreach {
              key =>
                level.get(key).assertGet shouldBe Memory.Put(key, "upper level")
            }
            (10 to 15) foreach {
              key =>
                level.get(key).assertGet shouldBe Memory.Put(key, key)
            }
          }
      )
    }

    "return None for key-values that are removed from lower Level but contain Range update in upper Level" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Range(5, 10, None, Value.Update("upper level"))),
        lowerLevelKeyValues = Slice(Memory.Remove(5), Memory.Put(7, 7), Memory.Remove(9)),
        assertion =
          level => {
            level.get(5).assertGetOpt shouldBe empty
            level.get(6).assertGetOpt shouldBe empty
            level.get(7).assertGet shouldBe Memory.Put(7, "upper level")
            level.get(8).assertGetOpt shouldBe empty
            level.get(9).assertGetOpt shouldBe empty
            level.get(10).assertGetOpt shouldBe empty
            level.get(11).assertGetOpt shouldBe empty
          }
      )
    }
  }

  "Level.get should return key-values if the Level contains Ranges and Removes and lower level contains key-values" in {

    val lowerLevel = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
    val level = TestLevel(nextLevel = Some(lowerLevel), throttle = (_) => Throttle(Duration.Zero, 0))

    val keyValues =
      Slice(
        Memory.Remove(1),
        Memory.Range(2, 10, None, Value.Remove(None)),
        Memory.Range(10, 20, Some(Value.Remove(None)), Value.Update(10)),
        Memory.Range(25, 30, None, Value.Remove(None)),
        Memory.Remove(30),
        Memory.Range(31, 35, None, Value.Update(30)),
        Memory.Range(40, 45, Some(Value.Remove(None)), Value.Remove(None))
      )

    level.putKeyValues(keyValues).assertGet

    (1 to 100) foreach {
      key =>
        level.get(key).assertGetOpt shouldBe empty
    }

    //add remove for each 100 key-values in lower level and all key-value should still return empty
    (1 to 100) foreach {
      key =>
        lowerLevel.putKeyValues(Slice(Memory.Remove(key - 1), Memory.Remove(key), Memory.Remove(key + 1))).assertGet
        lowerLevel.isEmpty shouldBe false
        level.get(key).assertGetOpt shouldBe empty
    }

    lowerLevel.segments should have size 1
    lowerLevel.segments.head.path.fileId.assertGet._1 shouldBe 100 //100 updates occurred

    //1 to 10 is each non existing or is removed from the upper level via a Remove Range or Remove.
    //inserting a Put in the lower Level should still return empty.
    (1 to 10) foreach {
      key =>
        lowerLevel.putKeyValues(Slice(Memory.Put(key - 1), Memory.Put(key), Memory.Put(key + 1))).assertGet
        lowerLevel.isEmpty shouldBe false
        level.get(key).assertGetOpt shouldBe empty
    }

    //11 to 19 is a Put range with rangeValue set to Put(10). Inserting Put in lower level for these key-values should return range's range value.
    //inserting a Put in the lower Level should still return empty.
    (11 to 19) foreach {
      key =>
        lowerLevel.putKeyValues(Slice(Memory.Put(key, key))).assertGet
        level.get(key).assertGet shouldBe Memory.Put(key, Some(10))
        level.get(key + 1).assertGetOpt shouldBe empty //next is empty
    }

    //20 has no key in upper Level. Inserting it in lower Level and reading from upper should return lower level's value
    //PUT
    lowerLevel.putKeyValues(Slice(Memory.Put(20, "twenty"))).assertGet
    level.get(20).assertGet shouldBe Memory.Put(20, Some("twenty"))
    //REMOVE
    lowerLevel.putKeyValues(Slice(Memory.Remove(20))).assertGet

    //25 to 30 is remove in upper Level. Put in lower level should still return empty
    (25 to 30) foreach {
      key =>
        lowerLevel.putKeyValues(Slice(Memory.Put(key))).assertGet
        level.get(key).assertGetOpt shouldBe empty
    }

    //31 to 34 is a Put range with rangeValue set to Put(30). Inserting Put in lower level for these key-values should return range's range value.
    //inserting a Put in the lower Level should still return empty.
    (31 to 34) foreach {
      key =>
        lowerLevel.putKeyValues(Slice(Memory.Put(key, key))).assertGet
        level.get(key).assertGet shouldBe Memory.Put(key, Some(30))
    }

    //40 to 44 is remove in upper Level. Put in lower level should still return empty
    (40 to 44) foreach {
      key =>
        lowerLevel.putKeyValues(Slice(Memory.Put(key))).assertGet
        level.get(key).assertGetOpt shouldBe empty
    }

    //44 to 50 has no key-values
    (44 to 50) foreach {
      key =>
        level.get(key).assertGetOpt shouldBe empty
    }
  }
}
