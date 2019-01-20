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

package swaydb

import scala.concurrent.Future
import scala.concurrent.duration._
import swaydb.serializers.Default._
import swaydb.core.TryAssert._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._

class SwayDBSpec0 extends SwayDBSpec {
  override def newDB(): Map[Int, String] =
    swaydb.persistent.Map[Int, String](randomDir).assertGet

  override val keyValueCount: Int = 100
}

class SwayDBSpec1 extends SwayDBSpec {

  override val keyValueCount: Int = 100

  override def newDB(): Map[Int, String] =
    swaydb.persistent.Map[Int, String](randomDir, mapSize = 1.byte).assertGet
}

class SwayDBSpec2 extends SwayDBSpec {

  override val keyValueCount: Int = 100

  override def newDB(): Map[Int, String] =
    swaydb.memory.Map[Int, String](mapSize = 1.byte).assertGet
}

class SwayDBSpec3 extends SwayDBSpec {

  override val keyValueCount: Int = 100

  override def newDB(): Map[Int, String] =
    swaydb.memory.Map[Int, String]().assertGet
}

sealed trait SwayDBSpec extends TestBaseEmbedded {

  def newDB(): Map[Int, String]

  "SwayDB" should {
    "remove all but first and last" in {
      val db = newDB()

      (1 to 1000) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }
      println("Removing .... ")
      db.remove(2, 999).assertGet
      println("Removed .... ")

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

      db.update(1, 100, value = "updated").assertGet

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

      db.remove(2, 99).assertGet

      db.update(1, 100, value = "updated").assertGet

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

      db.update(10, 90, value = "updated").assertGet

      db.remove(50, 99).assertGet

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
      db.update(1, Int.MaxValue, value = "updated").assertGet

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

      //      remove and then put should return Put's value
      db.batch(Batch.Remove(1), Batch.Put(1, "one")).assertGet
      db.get(1).assertGet shouldBe "one"

      //remove range and put should return Put's value
      db.batch(Batch.Remove(1, 100), Batch.Put(1, "one")).assertGet
      db.get(1).assertGet shouldBe "one"

      db.batch(Batch.Put(1, "one"), Batch.Put(2, "two"), Batch.Put(1, "one one"), Batch.Update(1, 100, "updated"), Batch.Remove(1, 100)).assertGet
      db.get(1).assertGetOpt shouldBe empty
      db.isEmpty shouldBe true
      //
      db.batch(Batch.Put(1, "one"), Batch.Put(2, "two"), Batch.Put(1, "one one"), Batch.Remove(1, 100), Batch.Update(1, 100, "updated")).assertGet
      db.get(1).assertGetOpt shouldBe empty
      db.isEmpty shouldBe true

      db.batch(Batch.Put(1, "one"), Batch.Put(2, "two"), Batch.Put(1, "one again"), Batch.Update(1, 100, "updated")).assertGet
      db.get(1).assertGet shouldBe "updated"
      db.toMap.values should contain only "updated"

      db.batch(Batch.Put(1, "one"), Batch.Put(2, "two"), Batch.Put(100, "hundred"), Batch.Remove(1, 100), Batch.Update(1, 1000, "updated")).assertGet
      db.toList shouldBe empty
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
      //      val db = newDB()
      //
      //      (1 to 10000) foreach {
      //        i =>
      //          db.put(i, i.toString).assertGet
      //      }

      //      (1 to 10000) foreach {
      //        i =>
      //          db.valueSize(i.toString).assertGet shouldBe i.toString.getBytes(StandardCharsets.UTF_8).length
      //      }
    }

    "eventually remove all Segments from the database when remove range is submitted" in {
      val db = newDB()

      (1 to 2000000) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }

      db.remove(1, 2000000).assertGet

      assertLevelsAreEmpty(db, submitUpdates = true)

    }

    "eventually remove all Segments from the database when expire range is submitted" in {
      val db = newDB()

      (1 to 2000000) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }

      db.expire(1, 2000000, 5.minutes).assertGet
      println("Expiry submitted.")

      assertLevelsAreEmpty(db, submitUpdates = true)

    }

    "eventually remove all Segments from the database when put is submitted with expiry" in {
      val db = newDB()

      (1 to 2000000) foreach {
        i =>
          db.put(i, i.toString, 5.minute).assertGet
      }

      assertLevelsAreEmpty(db, submitUpdates = false)

    }
  }

  "debug" in {
    val db = newDB()

    def doRead() =
      Future {
        while (true) {
          (1 to 2000000) foreach {
            i =>
              db.get(i).assertGet shouldBe i.toString
              if (i % 100000 == 0)
                println(s"Read $i")
          }
        }
      }

    def doWrite() = {
      println("writing")
      (1 to 2000000) foreach {
        i =>
          db.put(i, i.toString).assertGet
          if (i % 100000 == 0)
            println(s"Write $i")
      }
      println("writing end")
    }

    doWrite()
    doRead()
    sleep(1.minutes)
    doWrite()
    sleep(1.minutes)
    doWrite()
    sleep(1.minutes)
    doWrite()
    assertLevelsAreEmpty(db, submitUpdates = false)
  }

  "debug before and mapRight" in {
    val db = newDB()

    (1 to 10) foreach {
      i =>
        db.put(i, i.toString).assertGet
    }

    //    db.before(5).toList foreach println
    db.before(5).mapRight { case (k, v) => (k, v) } foreach println

  }
}
