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

import scala.concurrent.duration._
import swaydb._
import swaydb.core.CommonAssertions._
import swaydb.core.IOAssert._
import swaydb.core.RunThis._
import swaydb.data.IO
import swaydb.serializers.Default._

class SwayDBExpireSpec0 extends SwayDBExpireSpec {
  val keyValueCount: Int = 1000

  override def newDB(): Map[Int, String, IO] =
    swaydb.persistent.Map[Int, String](dir = randomDir).assertGet
}

class SwayDBExpireSpec1 extends SwayDBExpireSpec {

  val keyValueCount: Int = 1000

  override def newDB(): Map[Int, String, IO] =
    swaydb.persistent.Map[Int, String](randomDir, mapSize = 1.byte, segmentSize = 10.bytes).assertGet
}

class SwayDBExpireSpec2 extends SwayDBExpireSpec {

  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String, IO] =
    swaydb.memory.Map[Int, String](mapSize = 1.byte).assertGet
}

class SwayDBExpireSpec3 extends SwayDBExpireSpec {
  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String, IO] =
    swaydb.memory.Map[Int, String]().assertGet
}

class SwayDBExpireSpec4 extends SwayDBExpireSpec {

  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String, IO] =
    swaydb.memory.zero.Map[Int, String](mapSize = 1.byte).assertGet
}

class SwayDBExpireSpec5 extends SwayDBExpireSpec {
  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String, IO] =
    swaydb.memory.zero.Map[Int, String]().assertGet
}

sealed trait SwayDBExpireSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): Map[Int, String, IO]

  "Expire" when {
    "put" in {
      val db = newDB()

      db.asScala.put(1, "one")

      db.asScala.get(1) should contain("one")
    }

    "putAll" in {
      val db = newDB()

      db.asScala ++= Seq((1, "one"), (2, "two"))

      db.asScala.get(1) should contain("one")
      db.asScala.get(2) should contain("two")
    }

    "remove" in {
      val db = newDB()

      db.asScala ++= Seq((1, "one"), (2, "two"))

      db.asScala.remove(1)

      db.asScala.get(1) shouldBe empty
      db.asScala.get(2) should contain("two")
    }

    "removeAll" in {
      val db = newDB()

      db.asScala ++= Seq((1, "one"), (2, "two"))

      db.asScala.clear()

      db.asScala.get(1) shouldBe empty
      db.asScala.get(2) shouldBe empty
    }
  }
}
