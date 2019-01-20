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

package swaydb

import scala.concurrent.duration._
import swaydb.core.TestBase
import swaydb.serializers.Default._
import swaydb.core.TryAssert._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._

class SwayDBPutSpec0 extends SwayDBPutSpec {
  val keyValueCount: Int = 1000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Map[Int, String] =
    swaydb.persistent.Map[Int, String](dir = randomDir).assertGet
}

class SwayDBPutSpec1 extends SwayDBPutSpec {

  val keyValueCount: Int = 10000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Map[Int, String] =
    swaydb.persistent.Map[Int, String](randomDir, mapSize = 1.byte).assertGet
}

class SwayDBPutSpec2 extends SwayDBPutSpec {

  val keyValueCount: Int = 100000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Map[Int, String] =
    swaydb.memory.Map[Int, String](mapSize = 1.byte).assertGet
}

class SwayDBPutSpec3 extends SwayDBPutSpec {
  val keyValueCount: Int = 100000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Map[Int, String] =
    swaydb.memory.Map[Int, String]().assertGet
}

sealed trait SwayDBPutSpec extends TestBase with TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(minTimeLeftToUpdateExpiration: FiniteDuration = 10.seconds): Map[Int, String]

  def doGet(db: Map[Int, String]) = {
    (1 to keyValueCount) foreach {
      i =>
        db.expiration(i).assertGetOpt shouldBe empty
        db.get(i).assertGet shouldBe s"$i new"
    }
  }

  "Put" when {
    "Put" in {
      val db = newDB()

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).assertGet }
      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)
    }

    "Put & Expire" in {
      val db = newDB()

      val deadline = eitherOne(4.seconds.fromNow, expiredDeadline())

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).assertGet }
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).assertGet),
        right = db.expire(1, keyValueCount, deadline).assertGet
      )
      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)
      sleep(deadline)
      doGet(db)
    }

    "Put & Remove" in {
      val db = newDB()

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).assertGet }
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).assertGet),
        right = db.remove(1, keyValueCount).assertGet
      )
      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)
    }

    "Put & Update" in {
      val db = newDB()

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).assertGet }
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").assertGet),
        right = db.update(1, keyValueCount, value = "updated").assertGet
      )
      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)
    }
  }

  "Put" when {
    "Remove" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).assertGet),
        right = db.remove(1, keyValueCount).assertGet
      )

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)
    }

    "Remove & Put" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).assertGet),
        right = db.remove(1, keyValueCount).assertGet
      )
      (1 to keyValueCount) foreach { i => db.put(i, i.toString).assertGet }
      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)
    }

    "Remove & Update" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).assertGet),
        right = db.remove(1, keyValueCount).assertGet
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").assertGet),
        right = db.update(1, keyValueCount, value = "updated").assertGet
      )

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)
    }

    "Remove & Expire" in {
      val db = newDB()

      val deadline = eitherOne(2.seconds.fromNow, expiredDeadline())

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).assertGet),
        right = db.remove(1, keyValueCount).assertGet
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).assertGet),
        right = db.expire(1, keyValueCount, deadline).assertGet
      )

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)
    }

    "Remove & Remove" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).assertGet),
        right = db.remove(1, keyValueCount).assertGet
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).assertGet),
        right = db.remove(1, keyValueCount).assertGet
      )

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)
    }
  }

  "Put" when {
    "Update" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "old updated").assertGet),
        right = db.update(1, keyValueCount, value = "old updated").assertGet
      )
      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)
    }

    "Update & Put" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").assertGet),
        right = db.update(1, keyValueCount, value = "updated").assertGet
      )

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).assertGet }

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)
    }

    "Update & Update" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 1").assertGet),
        right = db.update(1, keyValueCount, value = "updated 1").assertGet
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 2").assertGet),
        right = db.update(1, keyValueCount, value = "updated 2").assertGet
      )

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)

    }

    "Update & Expire" in {
      val db = newDB()

      val deadline = eitherOne(2.seconds.fromNow, expiredDeadline())

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 1").assertGet),
        right = db.update(1, keyValueCount, value = "updated 1").assertGet
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).assertGet),
        right = db.expire(1, keyValueCount, deadline).assertGet
      )

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)
    }

    "Update & Remove" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 1").assertGet),
        right = db.update(1, keyValueCount, value = "updated 1").assertGet
      )

      (1 to keyValueCount) foreach { i => db.remove(i).assertGet }

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }
      doGet(db)
    }
  }

  "Put" when {
    "Expire" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).assertGet),
        right = db.expire(1, keyValueCount, deadline).assertGet
      )

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)
    }

    "Expire & Remove" in {
      val db = newDB()
      //if the deadline is either expired or delay it does not matter in this case because the underlying key-values are removed.
      val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).assertGet),
        right = db.expire(1, keyValueCount, deadline).assertGet
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).assertGet),
        right = db.remove(1, keyValueCount).assertGet
      )
      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)
    }

    "Expire & Update" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).assertGet),
        right = db.expire(1, keyValueCount, deadline).assertGet
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").assertGet),
        right = db.update(1, keyValueCount, value = "updated").assertGet
      )
      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)
    }

    "Expire & Expire" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)
      val deadline2 = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).assertGet),
        right = db.expire(1, keyValueCount, deadline).assertGet
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline2).assertGet),
        right = db.expire(1, keyValueCount, deadline2).assertGet
      )

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)
    }

    "Expire & Put" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).assertGet),
        right = db.expire(1, keyValueCount, deadline).assertGet
      )

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).assertGet }

      (1 to keyValueCount) foreach { i => db.put(i, s"$i new").assertGet }

      doGet(db)
    }
  }
}
