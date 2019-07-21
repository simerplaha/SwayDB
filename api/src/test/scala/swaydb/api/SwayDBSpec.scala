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

package swaydb.api

import swaydb._
import swaydb.core.IOValues._
import swaydb.core.RunThis._
import swaydb.data.IO
import swaydb.serializers.Default._

class SwayDBSpec0 extends SwayDBSpec {
  override def newDB(): Map[Int, String, IO] =
    swaydb.persistent.Map[Int, String](randomDir).runIO

  override val keyValueCount: Int = 100
}

class SwayDBSpec1 extends SwayDBSpec {

  override val keyValueCount: Int = 100

  override def newDB(): Map[Int, String, IO] =
    swaydb.persistent.Map[Int, String](randomDir, mapSize = 1.byte).runIO
}

class SwayDB_Zero_Spec0 extends SwayDBSpec {
  override def newDB(): Map[Int, String, IO] =
    swaydb.persistent.zero.Map[Int, String](randomDir).runIO

  override val keyValueCount: Int = 100
}

class SwayDB_Zero_Spec1 extends SwayDBSpec {

  override val keyValueCount: Int = 100

  override def newDB(): Map[Int, String, IO] =
    swaydb.persistent.zero.Map[Int, String](randomDir, mapSize = 1.byte).runIO
}

class SwayDBSpec2 extends SwayDBSpec {

  override val keyValueCount: Int = 100

  override def newDB(): Map[Int, String, IO] =
    swaydb.memory.Map[Int, String](mapSize = 1.byte).runIO
}

class SwayDBSpec3 extends SwayDBSpec {

  override val keyValueCount: Int = 100

  override def newDB(): Map[Int, String, IO] =
    swaydb.memory.Map[Int, String]().runIO
}

//class SwayDB_Zero_Spec2 extends SwayDBSpec {
//
//  override val keyValueCount: Int = 100
//
//  override def newDB(): Map[Int, String, IO] =
//    swaydb.memory.zero.Map[Int, String](mapSize = 1.byte).assertGet
//}
//
//class SwayDB_Zero_Spec3 extends SwayDBSpec {
//
//  override val keyValueCount: Int = 100
//
//  override def newDB(): Map[Int, String, IO] =
//    swaydb.memory.zero.Map[Int, String]().assertGet
//}

sealed trait SwayDBSpec extends TestBaseEmbedded {

  def newDB(): Map[Int, String, IO]

  "SwayDB" should {
    "remove all but first and last" in {
      val db = newDB()

      (1 to 1000) foreach {
        i =>
          db.put(i, i.toString).runIO
      }
      println("Removing .... ")
      db.remove(2, 999).runIO
      println("Removed .... ")

      db.stream.materialize.get should contain only((1, "1"), (1000, "1000"))
      db.headOption.runIOValue shouldBe ((1, "1"))
      db.lastOption.runIOValue shouldBe ((1000, "1000"))

      db.close().get
    }

    "update only key-values that are not removed" in {
      val db = newDB()

      (1 to 100) foreach {
        i =>
          db.put(i, i.toString).runIO
      }

      (2 to 99) foreach {
        i =>
          db.remove(i).runIO
      }

      db.update(1, 100, value = "updated").runIO

      db.stream.materialize.get should contain only((1, "updated"), (100, "updated"))
      db.headOption.runIOValue shouldBe ((1, "updated"))
      db.lastOption.runIOValue shouldBe ((100, "updated"))

      db.close().get
    }

    "update only key-values that are not removed by remove range" in {
      val db = newDB()

      (1 to 100) foreach {
        i =>
          db.put(i, i.toString).runIO
      }

      db.remove(2, 99).runIO

      db.update(1, 100, value = "updated").runIO

      db.stream.materialize.get should contain only((1, "updated"), (100, "updated"))
      db.headOption.runIOValue shouldBe ((1, "updated"))
      db.lastOption.runIOValue shouldBe ((100, "updated"))

      db.close().get
    }

    "return only key-values that were not removed from remove range" in {
      val db = newDB()

      (1 to 100) foreach {
        i =>
          db.put(i, i.toString).runIO
      }

      db.update(10, 90, value = "updated").runIO

      db.remove(50, 99).runIO

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

      db.stream.materialize.get shouldBe expected
      db.headOption.runIOValue shouldBe ((1, "1"))
      db.lastOption.runIOValue shouldBe ((100, "100"))

      db.close().get
    }

    "return empty for an empty database with update range" in {
      val db = newDB()
      db.update(1, Int.MaxValue, value = "updated").runIO

      db.isEmpty.get shouldBe true

      db.stream.materialize.get shouldBe empty

      db.headOption.get shouldBe empty
      db.lastOption.get shouldBe empty

      db.close().get
    }

    "return empty for an empty database with remove range" in {
      val db = newDB()
      db.remove(1, Int.MaxValue).runIO

      db.isEmpty.get shouldBe true

      db.stream.materialize.get shouldBe empty

      db.headOption.get shouldBe empty
      db.lastOption.get shouldBe empty

      db.close().get
    }

    "batch put, remove, update & range remove key-values" in {
      val db = newDB()

      db.commit(Prepare.Put(1, "one"), Prepare.Remove(1)).runIO
      db.get(1).runIO shouldBe empty

      //      remove and then put should return Put's value
      db.commit(Prepare.Remove(1), Prepare.Put(1, "one")).runIO
      db.get(1).runIOValue shouldBe "one"

      //remove range and put should return Put's value
      db.commit(Prepare.Remove(1, 100), Prepare.Put(1, "one")).runIO
      db.get(1).runIOValue shouldBe "one"

      db.commit(Prepare.Put(1, "one"), Prepare.Put(2, "two"), Prepare.Put(1, "one one"), Prepare.Update(1, 100, "updated"), Prepare.Remove(1, 100)).runIO
      db.get(1).runIO shouldBe empty
      db.isEmpty.get shouldBe true
      //
      db.commit(Prepare.Put(1, "one"), Prepare.Put(2, "two"), Prepare.Put(1, "one one"), Prepare.Remove(1, 100), Prepare.Update(1, 100, "updated")).runIO
      db.get(1).runIO shouldBe empty
      db.isEmpty.get shouldBe true

      db.commit(Prepare.Put(1, "one"), Prepare.Put(2, "two"), Prepare.Put(1, "one again"), Prepare.Update(1, 100, "updated")).runIO
      db.get(1).runIOValue shouldBe "updated"
      db.stream.materialize.map(_.toMap).runIO.values should contain only "updated"

      db.commit(Prepare.Put(1, "one"), Prepare.Put(2, "two"), Prepare.Put(100, "hundred"), Prepare.Remove(1, 100), Prepare.Update(1, 1000, "updated")).runIO
      db.stream.materialize.get shouldBe empty

      db.close().get
    }

    "perform from, until, before & after" in {
      val db = newDB()

      (1 to 10000) foreach {
        i =>
          db.put(i, i.toString).runIO
      }

      db.from(9999).stream.materialize.runIO should contain only((9999, "9999"), (10000, "10000"))
      db.from(9998).drop(2).take(1).materialize.get should contain only ((10000, "10000"))
      db.before(9999).take(1).materialize.get should contain only ((9998, "9998"))
      db.after(9999).take(1).materialize.get should contain only ((10000, "10000"))
      db.after(9999).drop(1).materialize.get shouldBe empty

      db.after(10).takeWhile(_._1 <= 11).materialize.get should contain only ((11, "11"))
      db.after(10).takeWhile(_._1 <= 11).drop(1).materialize.get shouldBe empty

      db.fromOrBefore(0).stream.materialize.get shouldBe empty
      db.fromOrAfter(0).take(1).materialize.get should contain only ((1, "1"))

      db.close().get
    }

    "perform mightContain & contains" in {
      val db = newDB()

      (1 to 10000) foreach {
        i =>
          db.put(i, i.toString).runIO
      }

      (1 to 10000) foreach {
        i =>
          db.mightContain(i).runIO shouldBe true
          db.contains(i).runIO shouldBe true
      }

      //      db.mightContain(Int.MaxValue).assertGet shouldBe false
      //      db.mightContain(Int.MinValue).assertGet shouldBe false
      db.contains(20000).runIO shouldBe false

      db.close().get
    }

    "contains on removed should return false" in {
      val db = newDB()

      (1 to 100000) foreach {
        i =>
          db.put(i, i.toString).runIO
      }

      (1 to 100000) foreach {
        i =>
          db.remove(i).runIO
      }

      (1 to 100000) foreach {
        i =>
          db.contains(i).runIO shouldBe false
      }

      db.contains(100001).runIO shouldBe false

      db.close().get
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

    //    "eventually remove all Segments from the database when remove range is submitted" in {
    //      val db = newDB()
    //
    //      (1 to 2000000) foreach {
    //        i =>
    //          db.put(i, i.toString).assertGet
    //      }
    //
    //      db.remove(1, 2000000).assertGet
    //
    //      assertLevelsAreEmpty(db, submitUpdates = true)
    //
    //      db.closeDatabase().get
    //    }
    //
    //    "eventually remove all Segments from the database when expire range is submitted" in {
    //      val db = newDB()
    //
    //      (1 to 2000000) foreach {
    //        i =>
    //          db.put(i, i.toString).assertGet
    //      }
    //
    //      db.expire(1, 2000000, 5.minutes).assertGet
    //      println("Expiry submitted.")
    //
    //      assertLevelsAreEmpty(db, submitUpdates = true)
    //
    //      db.closeDatabase().get
    //    }
    //
    //    "eventually remove all Segments from the database when put is submitted with expiry" in {
    //      val db = newDB()
    //
    //      (1 to 2000000) foreach {
    //        i =>
    //          db.put(i, i.toString, 5.minute).assertGet
    //      }
    //
    //      assertLevelsAreEmpty(db, submitUpdates = false)
    //
    //      db.closeDatabase().get
    //    }
    //  }

    //  "debug" in {
    //    val db = newDB()
    //
    //    def doRead() =
    //      Future {
    //        while (true) {
    //          (1 to 2000000) foreach {
    //            i =>
    //              db.get(i).assertGet shouldBe i.toString
    //              if (i % 100000 == 0)
    //                println(s"Read $i")
    //          }
    //        }
    //      }
    //
    //    def doWrite() = {
    //      println("writing")
    //      (1 to 2000000) foreach {
    //        i =>
    //          db.put(i, i.toString).assertGet
    //          if (i % 100000 == 0)
    //            println(s"Write $i")
    //      }
    //      println("writing end")
    //    }
    //
    //    doWrite()
    //    doRead()
    //    sleep(1.minutes)
    //    doWrite()
    //    sleep(1.minutes)
    //    doWrite()
    //    sleep(1.minutes)
    //    doWrite()
    //    assertLevelsAreEmpty(db, submitUpdates = false)
    //
    //    db.closeDatabase().get
    //  }
    //
    //  "debug before and mapRight" in {
    //    val db = newDB()
    //
    //    (1 to 10) foreach {
    //      i =>
    //        db.put(i, i.toString).assertGet
    //    }
    //
    //    //    db.before(5).toList foreach println
    //    db.before(5).reverse.map { case (k, v) => (k, v) } foreach println
    //
    //    db.closeDatabase().get
    //  }
  }
}
