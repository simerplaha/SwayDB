/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import swaydb.IOValues._
import swaydb._
import swaydb.core.RunThis._
import swaydb.serializers.Default._

class ScalaSetSpec0 extends ScalaSetSpec {
  val keyValueCount: Int = 1000

  override def newDB(): Set[Int, Nothing, IO.ApiIO] =
    swaydb.persistent.Set[Int, Nothing, IO.ApiIO](dir = randomDir).right.value
}

class ScalaSetSpec1 extends ScalaSetSpec {

  val keyValueCount: Int = 1000

  override def newDB(): Set[Int, Nothing, IO.ApiIO] =
    swaydb.persistent.Set[Int, Nothing, IO.ApiIO](randomDir, mapSize = 1.byte, minSegmentSize = 10.bytes).right.value
}

class ScalaSetSpec2 extends ScalaSetSpec {

  val keyValueCount: Int = 10000

  override def newDB(): Set[Int, Nothing, IO.ApiIO] =
    swaydb.memory.Set[Int, Nothing, IO.ApiIO](mapSize = 1.byte).right.value
}

class ScalaSetSpec3 extends ScalaSetSpec {
  val keyValueCount: Int = 10000

  override def newDB(): Set[Int, Nothing, IO.ApiIO] =
    swaydb.memory.Set[Int, Nothing, IO.ApiIO]().right.value
}

class ScalaSetSpec4 extends ScalaSetSpec {

  val keyValueCount: Int = 10000

  override def newDB(): Set[Int, Nothing, IO.ApiIO] =
    swaydb.memory.zero.Set[Int, Nothing, IO.ApiIO](mapSize = 1.byte).right.value
}

class ScalaSetSpec5 extends ScalaSetSpec {
  val keyValueCount: Int = 10000

  override def newDB(): Set[Int, Nothing, IO.ApiIO] =
    swaydb.memory.zero.Set[Int, Nothing, IO.ApiIO]().right.value
}

sealed trait ScalaSetSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): Set[Int, Nothing, IO.ApiIO]

  "Expire" when {
    "put" in {
      val db = newDB()
      db.asScala.add(1)
      db.asScala.contains(1) shouldBe true
    }

    "putAll" in {
      val db = newDB()

      db.asScala ++= Seq(1, 2)

      db.asScala.contains(1) shouldBe true
      db.asScala.contains(2) shouldBe true
    }

    "remove" in {
      val db = newDB()

      db.asScala ++= Seq(1, 2)

      db.asScala.remove(1)

      db.asScala.contains(1) shouldBe false
      db.asScala.contains(2) shouldBe true
    }

    "removeAll" in {
      val db = newDB()

      db.asScala ++= Seq(1, 2)

      db.asScala.clear()

      db.asScala.contains(1) shouldBe false
      db.asScala.contains(2) shouldBe false
    }

    "head, last, contains" in {
      val db = newDB()

      db.asScala ++= Seq(1, 2)

      db.asScala.head shouldBe 1
      db.asScala.last shouldBe 2

      db.asScala.contains(1) shouldBe true
      db.asScala.contains(2) shouldBe true
      db.asScala.contains(3) shouldBe false

      db.close().get
    }
  }
}
