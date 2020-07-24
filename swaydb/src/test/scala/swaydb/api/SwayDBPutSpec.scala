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

package swaydb

import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb.api.{SwayDBGetSpec, TestBaseEmbedded}
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.serializers.Default._

import scala.concurrent.duration._

class SwayDBPutSpec0 extends SwayDBPutSpec {
  val keyValueCount: Int = 1000

  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](dir = randomDir).right.value
}

class SwayDBPut_SetMap_Spec0 extends SwayDBPutSpec {
  val keyValueCount: Int = 1000

  override def newDB(): SetMap[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.SetMap[Int, String, Nothing, IO.ApiIO](dir = randomDir).right.value
}

class SwayDBPutSpec1 extends SwayDBPutSpec {

  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir, mapSize = 1.byte).right.value
}

class SwayDBPutSpec2 extends SwayDBPutSpec {

  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO](mapSize = 1.byte).right.value
}

class SwayDBPutSpec3 extends SwayDBPutSpec {
  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO]().right.value
}

class MultiMapPutSpec4 extends SwayDBPutSpec {
  val keyValueCount: Int = 10000

  override def newDB(): SetMapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.persistent.MultiMap[Int, String, Nothing, IO.ApiIO](dir = randomDir).get)
}

class MultiMapPutSpec5 extends SwayDBPutSpec {
  val keyValueCount: Int = 10000

  override def newDB(): SetMapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.memory.MultiMap[Int, String, Nothing, IO.ApiIO]().get)
}


//class SwayDBPutSpec4 extends SwayDBPutSpec {
//
//  val keyValueCount: Int = 10000
//
//  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
//    swaydb.memory.zero.Map[Int, String, Nothing, IO.ApiIO](mapSize = 1.byte).right.value
//}
//
//class SwayDBPutSpec5 extends SwayDBPutSpec {
//  val keyValueCount: Int = 10000
//
//  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
//    swaydb.memory.zero.Map[Int, String, Nothing, IO.ApiIO]().right.value
//}

sealed trait SwayDBPutSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): SetMapT[Int, String, Nothing, IO.ApiIO]

  def doGet(db: SetMapT[Int, String, Nothing, IO.ApiIO]) = {
    (1 to keyValueCount) foreach {
      i =>
        db.expiration(i).right.value shouldBe empty
        db.get(i).right.value.value shouldBe s"$i new"
    }
  }

  "Put" when {
    "Put" in {
      val db = newDB()

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)

      db.close().get
    }

    "Put & Expire" in {
      val db = newDB()

      val deadline =
        eitherOne(4.seconds.fromNow, expiredDeadline())

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
      doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)
      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)
      sleep(deadline)
      doGet(db)

      db.close().get
    }

    "Put & Remove" in {
      val db = newDB()

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
      doRemove(from = 1, to = keyValueCount, db = db)
      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)

      db.close().get
    }

    "Put & Update" in {
      val db = newDB()

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
      doUpdateOrIgnore(1, keyValueCount, "updated", db)
      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)
    }
  }

  "Put" when {
    "Remove" in {
      val db = newDB()

      doRemove(from = 1, to = keyValueCount, db = db)

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)

      db.close().get
    }

    "Remove & Put" in {
      val db = newDB()

      doRemove(from = 1, to = keyValueCount, db = db)
      (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)

      db.close().get
    }

    "Remove & Update" in {
      val db = newDB()

      doRemove(from = 1, to = keyValueCount, db = db)
      doUpdateOrIgnore(1, keyValueCount, "updated", db)
      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)

      db.close().get
    }

    "Remove & Expire" in {
      val db = newDB()

      val deadline = eitherOne(2.seconds.fromNow, expiredDeadline())

      doRemove(from = 1, to = keyValueCount, db = db)
      doExpire(1, keyValueCount, deadline, db)

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)

      db.close().get
    }

    "Remove & Remove" in {
      val db = newDB()

      doRemove(from = 1, to = keyValueCount, db = db)
      doRemove(from = 1, to = keyValueCount, db = db)

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)

      db.close().get
    }
  }

  "Put" when {
    "Update" in {
      val db = newDB()

      doUpdateOrIgnore(1, keyValueCount, "old updated", db)

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)

      db.close().get
    }

    "Update & Put" in {
      val db = newDB()

      doUpdateOrIgnore(1, keyValueCount, "updated", db)

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)

      db.close().get
    }

    "Update & Update" in {
      val db = newDB()

      doUpdateOrIgnore(1, keyValueCount, "updated 1", db)
      doUpdateOrIgnore(1, keyValueCount, "updated 2", db)

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)

      db.close().get
    }

    "Update & Expire" in {
      val db = newDB()

      val deadline = eitherOne(2.seconds.fromNow, expiredDeadline())

      doUpdateOrIgnore(from = 1, to = keyValueCount, value = "updated 1", db = db)
      doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)

      db.close().get
    }

    "Update & Remove" in {
      val db = newDB()

      doUpdateOrIgnore(1, keyValueCount, "updated 1", db)
      doRemove(from = 1, to = keyValueCount, db = db)

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }
      doGet(db)

      db.close().get
    }
  }

  "Put" when {
    "Expire" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)

      doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)

      db.close().get
    }

    "Expire & Remove" in {
      val db = newDB()
      //if the deadline is either expired or delay it does not matter in this case because the underlying key-values are removed.
      val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)

      doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)
      doRemove(from = 1, to = keyValueCount, db = db)
      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)

      db.close().get
    }

    "Expire & Update" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)

      doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)

      doUpdateOrIgnore(from = 1, to = keyValueCount, value = "updated", db = db)

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)

      db.close().get
    }

    "Expire & Expire" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)
      val deadline2 = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)
      doExpire(from = 1, to = keyValueCount, deadline = deadline2, db = db)

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)

      db.close().get
    }

    "Expire & Put" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").right.value }

      doGet(db)

      db.close().get
    }
  }

  "clear" when {
    "empty" in {
      val db = newDB()

      db.isEmpty.get shouldBe true
      db.clearKeyValues().right.value
      db.isEmpty.get shouldBe true

      db.close().get
    }

    "not empty" in {
      val db = newDB()

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
      db.isEmpty.get shouldBe false

      db.clearKeyValues().right.value
      db.isEmpty.get shouldBe true

      db.close().get
    }
  }
}
