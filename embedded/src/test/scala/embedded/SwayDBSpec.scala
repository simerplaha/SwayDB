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
import swaydb._
import swaydb.serializers.Default._
import swaydb.types.SwayDBMap

class SwayDBPersistentSpec extends SwayDBSpec {
  override val db: SwayDBMap[Int, String] =
    SwayDB.persistent[Int, String](testDir).assertGet
}

class SwayDBPersistentSpecWith1ByteMapSize extends SwayDBSpec {
  override val db: SwayDBMap[Int, String] =
    SwayDB.persistent[Int, String](testDir, mapSize = 1.byte).assertGet
}

class SwayDBMemorySpec extends SwayDBSpec {
  override val db: SwayDBMap[Int, String] =
    SwayDB.memory[Int, String]().assertGet
}

class SwayDBMemoryWith1ByteMapSizeSpec extends SwayDBSpec {
  override val db: SwayDBMap[Int, String] =
    SwayDB.memory[Int, String](mapSize = 1.byte).assertGet
}

trait SwayDBSpec extends TestBase {

  val db: SwayDBMap[Int, String]

  "SwayDB" should {
    "get" in {

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
      db.get(100).assertGet shouldBe 100.toString
    }

    "update" in {
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
      db.get(100).assertGet shouldBe 100.toString
    }

    "remove all but first and last" in {
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

  }

}
