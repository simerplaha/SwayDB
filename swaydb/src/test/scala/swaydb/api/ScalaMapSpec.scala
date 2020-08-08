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
import swaydb.serializers.Default._
import swaydb.core.RunThis._

class ScalaMapSpec0 extends ScalaMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): SetMapT[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](dir = randomDir).right.value
}

class ScalaSetMapSpec0 extends ScalaMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): SetMapT[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.SetMap[Int, String, Nothing, IO.ApiIO](dir = randomDir).right.value
}

class ScalaMapSpec1 extends ScalaMapSpec {

  val keyValueCount: Int = 1000

  override def newDB(): SetMapT[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir, mapSize = 1.byte, segmentConfig = swaydb.persistent.DefaultConfigs.segmentConfig().copy(minSegmentSize = 10.bytes)).right.value
}

class ScalaMapSpec2 extends ScalaMapSpec {

  val keyValueCount: Int = 10000

  override def newDB(): SetMapT[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO](mapSize = 1.byte).right.value
}

class ScalaMapSpec3 extends ScalaMapSpec {
  val keyValueCount: Int = 10000

  override def newDB(): SetMapT[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO]().right.value
}

class MultiMapSpec4 extends ScalaMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): SetMapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.persistent.MultiMap[Int, Int, String, Nothing, IO.ApiIO](dir = randomDir).get)
}

class MultiMapSpec5 extends ScalaMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): SetMapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.memory.MultiMap[Int, Int, String, Nothing, IO.ApiIO]().get)
}

//class ScalaMapSpec4 extends ScalaMapSpec {
//
//  val keyValueCount: Int = 10000
//
//  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
//    swaydb.memory.zero.Map[Int, String, Nothing, IO.ApiIO](mapSize = 1.byte).right.value
//}

//class ScalaMapSpec5 extends ScalaMapSpec {
//  val keyValueCount: Int = 10000
//
//  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
//    swaydb.memory.zero.Map[Int, String, Nothing, IO.ApiIO]().right.value
//}

sealed trait ScalaMapSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): SetMapT[Int, String, Nothing, IO.ApiIO]

  "Expire" when {
    "put" in {
      runThis(times = repeatTest, log = true) {
        val db = newDB()

        db.asScala.put(1, "one")

        db.asScala.get(1) should contain("one")

        db.delete().get
      }
    }

    "putAll" in {
      runThis(times = repeatTest, log = true) {
        val db = newDB()

        db.asScala ++= Seq((1, "one"), (2, "two"))

        db.asScala.get(1) should contain("one")
        db.asScala.get(2) should contain("two")

        db.close().get
      }
    }

    "remove" in {
      runThis(times = repeatTest, log = true) {
        val db = newDB()

        db.asScala ++= Seq((1, "one"), (2, "two"))

        db.asScala.remove(1)

        db.asScala.get(1) shouldBe empty
        db.asScala.get(2) should contain("two")

        db.close().get
      }
    }

    "removeAll" in {
      runThis(times = repeatTest, log = true) {
        val db = newDB()

        db.asScala ++= Seq((1, "one"), (2, "two"))

        db.asScala.clear()

        db.asScala.get(1) shouldBe empty
        db.asScala.get(2) shouldBe empty

        db.close().get
      }
    }

    "keySet, head, last, contains" in {
      runThis(times = repeatTest, log = true) {
        val db = newDB()

        db.asScala ++= Seq((1, "one"), (2, "two"))

        if (db.isInstanceOf[swaydb.MultiMap[_, _, _, _, IO.ApiIO]])
          assertThrows[NotImplementedError](db.asScala.keySet)
        else
          db.asScala.keySet should contain only(1, 2)

        db.asScala.head shouldBe ((1, "one"))
        db.asScala.last shouldBe ((2, "two"))

        db.asScala.contains(1) shouldBe true
        db.asScala.contains(2) shouldBe true
        db.asScala.contains(3) shouldBe false

        db.close().get
      }
    }
  }
}
