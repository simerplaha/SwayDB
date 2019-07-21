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

import swaydb.core.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.util.Benchmark
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

//@formatter:off
class LevelPerformanceSpec0 extends LevelPerformanceSpec

class LevelPerformanceSpec1 extends LevelPerformanceSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelPerformanceSpec2 extends LevelPerformanceSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelPerformanceSpec3 extends LevelPerformanceSpec {
  override def inMemoryStorage = true
}
//@formatter:on

sealed trait LevelPerformanceSpec extends TestBase with Benchmark {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  val keyValuesCount = 100

  val keyValues = randomPutKeyValues(25000)
  //  val keyValues = randomIntKeyValues(250000)

  //  override def deleteFiles: Boolean = false

  def readAllKeyValues(level: Level) = {
    keyValues foreach {
      keyValue =>
        //          val key = keyValue.key.asInt()
        //          if (key % 1000 == 0)
        //            println(s"Reading $key")
        level.get(keyValue.key)
      //        level.get(keyValue.key).assertGet
      //        val got = level.get(keyValue.key).assertGet
      //        got.key shouldBe keyValue.key
      //        got.getOrFetchValue.assertGetOpt shouldBe keyValue.getOrFetchValue.assertGetOpt
      //          println("value: " + level.get(keyValue.key).assertGet._2.assertGet.asInt())
    }
  }

  def readLower(level: Level) =
    (1 until keyValues.size) foreach {
      index =>
        //        println(s"index: $index")
        level.lower(keyValues(index).key)
      //        val keyValue = level.lower(keyValues(index).key).assertGet
      //        keyValue.key shouldBe keyValues(index - 1).key
      //        keyValue.getOrFetchValue.assertGetOpt shouldBe keyValues(index - 1).getOrFetchValue.assertGetOpt
    }

  def readHigher(level: Level) =
    (0 until keyValues.size - 1) foreach {
      index =>
        level.higher(keyValues(index).key)
      //        val keyValue = level.higher(keyValues(index).key).assertGet
      //        keyValue.key shouldBe keyValues(index + 1).key
      //        keyValue.getOrFetchValue.assertGetOpt shouldBe keyValues(index + 1).getOrFetchValue.assertGetOpt
    }

  var level = TestLevel()
  level.putKeyValuesTest(keyValues).runIO

  def reopenLevel() = {
    println("Re-opening Level")
    level.segmentsInLevel().foreach {
      segment =>
        segment.clearCachedKeyValues()
        segment.close.runIO
    }
    level = level.reopen
  }

  "Level read performance 1" in {
    benchmark(s"read ${keyValues.size} key values when Level persistent = ${levelStorage.persistent}, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      readAllKeyValues(level)
    }
  }

  "Level read benchmark 2" in {
    benchmark(s"read ${keyValues.size} key values when Level persistent = ${levelStorage.persistent}, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      readAllKeyValues(level)
    }
  }

  "Level read benchmark 3" in {
    benchmark(s"read ${keyValues.size} key values when Level persistent = ${levelStorage.persistent}, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      readLower(level)
    }
  }

  "Level read benchmark 4" in {
    if (levelStorage.persistent) reopenLevel()
    benchmark(s"read ${keyValues.size} key values when Level persistent = ${levelStorage.persistent}, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      readLower(level)
    }
  }

  "Level read benchmark 5" in {
    if (levelStorage.persistent) reopenLevel()
    benchmark(s"read ${keyValues.size} key values when Level persistent = ${levelStorage.persistent}, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      readHigher(level)
    }
  }

  "Level read benchmark 6" in {
    benchmark(s"read ${keyValues.size} key values when Level persistent = ${levelStorage.persistent}, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      readHigher(level)
    }
  }
}
