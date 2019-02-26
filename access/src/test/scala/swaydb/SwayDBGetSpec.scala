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

import scala.concurrent.duration._
import swaydb.core.IOAssert._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.serializers.Default._

class SwayDBGetSpec0 extends SwayDBGetSpec {
  override def newDB(): Map[Int, String] =
    swaydb.persistent.Map[Int, String](randomDir).assertGet
}

class SwayDBGetSpec1 extends SwayDBGetSpec {

  override def newDB(): Map[Int, String] =
    swaydb.persistent.Map[Int, String](randomDir, mapSize = 1.byte).assertGet
}

class SwayDBGetSpec2 extends SwayDBGetSpec {

  override def newDB(): Map[Int, String] =
    swaydb.memory.Map[Int, String](mapSize = 1.byte).assertGet
}

class SwayDBGetSpec3 extends SwayDBGetSpec {
  override def newDB(): Map[Int, String] =
    swaydb.memory.Map[Int, String]().assertGet
}

class SwayDBGetSpec4 extends SwayDBGetSpec {

  override def newDB(): Map[Int, String] =
    swaydb.memory.zero.Map[Int, String](mapSize = 1.byte).assertGet
}

class SwayDBGetSpec5 extends SwayDBGetSpec {
  override def newDB(): Map[Int, String] =
    swaydb.memory.zero.Map[Int, String]().assertGet
}

sealed trait SwayDBGetSpec extends TestBase {

  def newDB(): Map[Int, String]

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

      db.closeDatabase().get
    }

    "return empty for removed key-value" in {

      val db = newDB()

      (1 to 100) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }

      (10 to 90) foreach {
        i =>
          db.remove(i).assertGet
      }

      (1 to 9) foreach {
        i =>
          db.get(i).assertGet shouldBe i.toString
      }

      (10 to 90) foreach {
        i =>
          db.get(i).assertGetOpt shouldBe empty
      }

      (91 to 100) foreach {
        i =>
          db.get(i).assertGet shouldBe i.toString
      }

      db.closeDatabase().get
    }

    "return empty for expired key-value" in {
      val db = newDB()

      (1 to 100) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }

      val expire = 2.second.fromNow

      (10 to 90) foreach {
        i =>
          db.expire(i, expire).assertGet
      }

      (1 to 100) foreach { i => db.get(i).assertGet shouldBe i.toString }

      sleep(expire.timeLeft)

      (10 to 90) foreach { i => db.get(i).assertGetOpt shouldBe empty }
      (1 to 9) foreach { i => db.get(i).assertGet shouldBe i.toString }
      (91 to 100) foreach { i => db.get(i).assertGet shouldBe i.toString }

      db.keys.toList shouldBe ((1 to 9) ++ (91 to 100))

      db.closeDatabase().get
    }

    "return empty for range expired key-value" in {
      val db = newDB()

      (1 to 100) foreach {
        i =>
          db.put(i, i.toString).assertGet
      }

      val expire = 2.second.fromNow

      db.expire(10, 90, expire).assertGet

      (1 to 100) foreach { i => db.get(i).assertGet shouldBe i.toString }

      sleep(expire.timeLeft)

      (10 to 90) foreach { i => db.get(i).assertGetOpt shouldBe empty }
      (1 to 9) foreach { i => db.get(i).assertGet shouldBe i.toString }
      (91 to 100) foreach { i => db.get(i).assertGet shouldBe i.toString }

      db.closeDatabase().get
    }
  }
}
