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

package embedded

import java.nio.charset.StandardCharsets

import swaydb.{Batch, SwayDB}
import swaydb.core.TestBase
import swaydb.serializers.Default._
import swaydb.types.SwayDBMap

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration._

class SwayDBPersistentSpec extends SwayDBSpec {
  override def newDB(): SwayDBMap[Int, String] =
    SwayDB.persistent[Int, String](randomDir).assertGet
}

class SwayDBPersistentSpecWith1ByteMapSize extends SwayDBSpec {

  import swaydb._

  override def newDB(): SwayDBMap[Int, String] =
    SwayDB.persistent[Int, String](randomDir, mapSize = 1.byte).assertGet
}

class SwayDBMemorySpec extends SwayDBSpec {
  override def newDB(): SwayDBMap[Int, String] =
    SwayDB.memory[Int, String]().assertGet
}

class SwayDBMemoryWith1ByteMapSizeSpec extends SwayDBSpec {

  import swaydb._

  override def newDB(): SwayDBMap[Int, String] =
    SwayDB.memory[Int, String](mapSize = 1.byte).assertGet
}

sealed trait SwayDBSpec extends TestBase {

  def newDB(): SwayDBMap[Int, String]

  "SwayDB" should {
    "get" in {

      val db = newDB()

      (1 to 100) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }

      (1 to 100) foreach {
        i =>
          db.get(i).assertGet shouldBe i.toString
      }
    }

    "remove range" in {
      val db = newDB()
      (1 to 100) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }

      db.remove(50, 100).assertGet

      (1 to 49) foreach {
        i =>
          db.get(i).assertGet shouldBe i.toString
      }

      (50 to 99) foreach {
        i =>
          db.get(i).assertGetOpt shouldBe empty
      }
      db.get(100).assertGet shouldBe "100"
    }

    "update" in {
      val db = newDB()

      (1 to 100) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }

      db.update(50, 100, "updated").assertGet

      (1 to 49) foreach {
        i =>
          db.get(i).assertGet shouldBe i.toString
      }

      (50 to 99) foreach {
        i =>
          db.get(i).assertGet shouldBe "updated"
      }
      db.get(100).assertGet shouldBe "100"
    }

    "remove all but first and last" in {
      val db = newDB()

      (1 to 1000) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }
      println("Removing .... ")
      db.remove(2, 1000)

      db.toList should contain only((1, "1"), (1000, "1000"))
      db.head shouldBe ((1, "1"))
      db.last shouldBe ((1000, "1000"))
    }

    "update only key-values that are not removed" in {
      val db = newDB()

      (1 to 100) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }

      (2 to 99) foreach {
        i =>
          db.remove(i).assertGet
      }

      db.update(1, 101, "updated").assertGet

      db.toList should contain only((1, "updated"), (100, "updated"))
      db.head shouldBe ((1, "updated"))
      db.last shouldBe ((100, "updated"))

    }

    "update only key-values that are not removed by remove range" in {
      val db = newDB()

      (1 to 100) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }

      db.remove(2, 100).assertGet

      db.update(1, 101, "updated").assertGet

      db.toList should contain only((1, "updated"), (100, "updated"))
      db.head shouldBe ((1, "updated"))
      db.last shouldBe ((100, "updated"))
    }

    "return only key-values that were not removed from remove range" in {
      val db = newDB()

      (1 to 100) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }

      db.update(10, 90, "updated").assertGet

      db.remove(50, 100).assertGet

      val expectedUnchanged =
        (1 to 9) map {
          i =>
            (i, i.toString)
        }

      val expectedUpdated =
        (10 to 49) map {
          i =>
            (i, "updated")
        }

      val expected = expectedUnchanged ++ expectedUpdated :+ (100, "100")

      db.toList shouldBe expected
      db.head shouldBe ((1, "1"))
      db.last shouldBe ((100, "100"))
    }

    "return empty for an empty database with update range" in {
      val db = newDB()
      db.update(1, Int.MaxValue, "updated").assertGet

      db.isEmpty shouldBe true

      db.toList shouldBe empty

      db.headOption shouldBe empty
      db.lastOption shouldBe empty
    }

    "return empty for an empty database with remove range" in {
      val db = newDB()
      db.remove(1, Int.MaxValue).assertGet

      db.isEmpty shouldBe true

      db.toList shouldBe empty

      db.headOption shouldBe empty
      db.lastOption shouldBe empty
    }

    "batch put, remove, update & range remove key-values" in {
      val db = newDB()

      db.batch(Batch.Put(1, "one"), Batch.Remove(1)).assertGet
      db.get(1).assertGetOpt shouldBe empty

      //remove and then put should return Put's value
      db.batch(Batch.Remove(1), Batch.Put(1, "one")).assertGet
      db.get(1).assertGet shouldBe "one"

      //remove range and put should return Put's value
      db.batch(Batch.Remove(1, 100), Batch.Put(1, "one")).assertGet
      db.get(1).assertGet shouldBe "one"

      db.batch(Batch.Put(1, "one"), Batch.Put(2, "two"), Batch.Put(1, "one one"), Batch.Update(1, 100, "updated"), Batch.Remove(1, 100)).assertGet
      db.get(1).assertGetOpt shouldBe empty
      db.isEmpty shouldBe true

      db.batch(Batch.Put(1, "one"), Batch.Put(2, "two"), Batch.Put(1, "one one"), Batch.Remove(1, 100), Batch.Update(1, 100, "updated")).assertGet
      db.get(1).assertGetOpt shouldBe empty
      db.isEmpty shouldBe true

      db.batch(Batch.Put(1, "one"), Batch.Put(2, "two"), Batch.Put(1, "one again"), Batch.Update(1, 100, "updated")).assertGet
      db.get(1).assertGet shouldBe "updated"
      db.toMap.values should contain only "updated"

      db.batch(Batch.Put(1, "one"), Batch.Put(2, "two"), Batch.Put(100, "hundred"), Batch.Remove(1, 100), Batch.Update(1, 1000, "updated")).assertGet
      db.toList should contain only ((100, "updated"))
    }

    "perform from, until, before & after" in {
      val db = newDB()

      (1 to 10000) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }

      db.from(9999).toList should contain only((9999, "9999"), (10000, "10000"))
      db.from(9999).drop(1).take(1).toList should contain only ((10000, "10000"))
      db.before(9999).take(1).toList should contain only ((9998, "9998"))
      db.after(9999).take(1).toList should contain only ((10000, "10000"))
      db.after(9999).drop(1).toList shouldBe empty

      db.after(10).tillKey(_ <= 11).toList should contain only ((11, "11"))
      db.after(10).tillKey(_ <= 11).drop(1).toList shouldBe empty

      db.fromOrBefore(0).toList shouldBe empty
      db.fromOrAfter(0).take(1).toList should contain only ((1, "1"))
    }

    "perform mightContain & contains" in {
      val db = newDB()

      (1 to 10000) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }

      (1 to 10000) foreach {
        i =>
          db.mightContain(i).assertGet shouldBe true
          db.contains(i).assertGet shouldBe true
      }

      db.mightContain(898989898).assertGet shouldBe false
      db.contains(20000).assertGet shouldBe false
    }

    "contains on removed should return false" in {
      val db = newDB()

      (1 to 100000) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }

      (1 to 100000) foreach {
        i =>
          db.remove(i).assertGet
      }

      (1 to 100000) foreach {
        i =>
          db.contains(i).assertGet shouldBe false
      }

      db.contains(100001).assertGet shouldBe false
    }

    "return valueSize" in {
      val db = newDB()

      (1 to 10000) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }

      (1 to 10000) foreach {
        i =>
          db.valueSize(i).assertGet shouldBe i.toString.getBytes(StandardCharsets.UTF_8).length
      }
    }

    "eventually remove all Segments from the database when remove range is submitted" in {
      val db = newDB()

      (1 to 2000000) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }

      db.remove(1, 2000001).assertGet

      def pluralSegment(count: Int) = if (count == 1) "Segment" else "Segments"

      //recursively go through all levels and assert they do no have any Segments.
      @tailrec
      def assertLevelsAreEmpty(db: SwayDBMap[Int, String], levelNumber: Int, expectedLastLevelEmpty: Boolean): Unit = {
        db.levelMeter(levelNumber) match {
          case Some(meter) if db.levelMeter(levelNumber + 1).nonEmpty => //is not the last Level. Check if this level contains no Segments.
            db.isEmpty shouldBe true //isEmpty will always return true since all key-values were removed.
            if (meter.segmentsCount == 0) { //this Level is empty, jump to next Level.
              println(s"Level $levelNumber is empty.")
              assertLevelsAreEmpty(db, levelNumber + 1, expectedLastLevelEmpty)
            } else {
              val interval = (levelNumber * 3).seconds //Level is not empty, try again with delay.
              println(s"Level $levelNumber contains ${meter.segmentsCount} ${pluralSegment(meter.segmentsCount)}. Will check again after $interval.")
              sleep(interval)
              assertLevelsAreEmpty(db, levelNumber, expectedLastLevelEmpty)
            }
          case _ => //is the last Level which will contains Segments.
            if (!expectedLastLevelEmpty) {
              println(s"Level $levelNumber. Submitting updated to trigger removed.")
              (1 to 200000) foreach { //submit multiple update range key-values so that a map gets submitted for compaction and to trigger merge on copied Segments in last Level.
                i =>
                  db.update(1, 1000001, "just triggering update to assert remove").assertGet
                  if (i == 100000) sleep(2.seconds)
              }
              //update submitted, now expect the merge to get triggered on the Segments in the last Level and Compaction to remove all key-values.
            }

            db.isEmpty shouldBe true //isEmpty will always return true since all key-values were removed.

            val segmentsCount = db.levelMeter(levelNumber).map(_.segmentsCount) getOrElse -1
            if (segmentsCount != 0) {
              println(s"Level $levelNumber contains $segmentsCount ${pluralSegment(segmentsCount)}. Will check again after 8.seconds.")
              sleep(8.seconds)
              assertLevelsAreEmpty(db, levelNumber, true)
            } else {
              println(s"Compaction completed. Level $levelNumber is empty.\n")
            }
        }
      }

      Future(assertLevelsAreEmpty(db, 1, false)).await(5.minutes)
    }

  }
}