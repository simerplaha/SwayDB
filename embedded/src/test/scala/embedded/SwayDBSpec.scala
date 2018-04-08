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

import swaydb.SwayDB
import swaydb.core.TestBase
import swaydb.serializers.Default._
import swaydb.types.SwayDBMap

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

  }
}