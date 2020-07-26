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

import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.serializers.Default._

import scala.concurrent.duration._

class SwayDBGetSpec0 extends SwayDBGetSpec {
  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir).right.value
}

class SwayDBGet_SetMap_Spec0 extends SwayDBGetSpec {
  override def newDB(): SetMap[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.SetMap[Int, String, Nothing, IO.ApiIO](randomDir).right.value
}

class SwayDBGetSpec1 extends SwayDBGetSpec {
  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir, mapSize = 1.byte).right.value
}

class SwayDBGetSpec2 extends SwayDBGetSpec {
  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO](mapSize = 1.byte).right.value
}

class SwayDBGetSpec3 extends SwayDBGetSpec {
  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO]().right.value
}

class MultiMapGetSpec4 extends SwayDBGetSpec {
  override def newDB(): SetMapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.persistent.MultiMap[Int, Int, String, Nothing, IO.ApiIO](dir = randomDir).get)
}

class MultiMapGetSpec5 extends SwayDBGetSpec {
  override def newDB(): SetMapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.memory.MultiMap[Int, Int, String, Nothing, IO.ApiIO]().get)
}

sealed trait SwayDBGetSpec extends TestBaseEmbedded {

  def newDB(): SetMapT[Int, String, Nothing, IO.ApiIO]

  override val keyValueCount: Int = 1000

  "SwayDB" should {
    "get" in {
      val db = newDB()

      (1 to 100) foreach {
        i =>
          db.put(i, i.toString).right.value
      }

      (1 to 100) foreach {
        i =>
          db.put(i, i.toString).right.value
      }

      (1 to 100) foreach {
        i =>
          db.get(i).right.value.value shouldBe i.toString
      }

      db.close().get
    }

    "return empty for removed key-value" in {

      val db = newDB()

      (1 to 100) foreach {
        i =>
          db.put(i, i.toString).right.value
      }

      (10 to 90) foreach {
        i =>
          db.remove(i).right.value
      }

      (1 to 9) foreach {
        i =>
          db.get(i).right.value.value shouldBe i.toString
      }

      (10 to 90) foreach {
        i =>
          db.get(i).right.value shouldBe empty
      }

      (91 to 100) foreach {
        i =>
          db.get(i).right.value.value shouldBe i.toString
      }

      db.close().get
    }

    "return empty for expired key-value" in {
      val db = newDB()

      (1 to 100) foreach {
        i =>
          db.put(i, i.toString).right.value
      }

      val expire = 2.second.fromNow

      (10 to 90) foreach {
        i =>
          db.expire(i, expire).right.value
      }

      (1 to 100) foreach { i => db.get(i).right.value.value shouldBe i.toString }

      sleep(expire.timeLeft + 10.millisecond)

      (10 to 90) foreach { i => db.get(i).right.value shouldBe empty }
      (1 to 9) foreach { i => db.get(i).right.value.value shouldBe i.toString }
      (91 to 100) foreach { i => db.get(i).right.value.value shouldBe i.toString }

      db match {
        case _: MultiMap[Int, Int, String, Nothing, IO.ApiIO] =>
          assertThrows[NotImplementedError](db.keySet)

        case _ =>
          db.keySet should contain allElementsOf ((1 to 9) ++ (91 to 100))
      }

      db.close().get
    }

    "return empty for range expired key-value" in {
      val db = newDB()

      (1 to 100) foreach {
        i =>
          db.put(i, i.toString).right.value
      }

      val expire = 2.second.fromNow

      doExpire(10, 90, expire, db)

      (1 to 100) foreach { i => db.get(i).right.value.value shouldBe i.toString }

      sleep(expire.timeLeft + 10.millisecond)

      (10 to 90) foreach { i => db.get(i).right.value shouldBe empty }
      (1 to 9) foreach { i => db.get(i).right.value.value shouldBe i.toString }
      (91 to 100) foreach { i => db.get(i).right.value.value shouldBe i.toString }

      db.close().get
    }
  }
}
