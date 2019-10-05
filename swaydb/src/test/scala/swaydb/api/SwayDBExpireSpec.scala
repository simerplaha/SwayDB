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

import org.scalatest.OptionValues._
import swaydb.api.TestBaseEmbedded
import swaydb.core.CommonAssertions._
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.serializers.Default._

import scala.concurrent.duration._

class SwayDBExpireSpec0 extends SwayDBExpireSpec {
  val keyValueCount: Int = 1000

  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing](dir = randomDir).right.value
}

class SwayDBExpireSpec1 extends SwayDBExpireSpec {

  val keyValueCount: Int = 1000

  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing](randomDir, mapSize = 1.byte, segmentSize = 10.bytes).right.value
}

class SwayDBExpireSpec2 extends SwayDBExpireSpec {

  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing](mapSize = 1.byte).right.value
}

class SwayDBExpireSpec3 extends SwayDBExpireSpec {
  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing]().right.value
}

class SwayDBExpireSpec4 extends SwayDBExpireSpec {

  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.zero.Map[Int, String, Nothing](mapSize = 1.byte).right.value
}

class SwayDBExpireSpec5 extends SwayDBExpireSpec {
  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.zero.Map[Int, String, Nothing]().right.value
}

sealed trait SwayDBExpireSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): Map[Int, String, Nothing, IO.ApiIO]

  "Expire" when {
    "Put" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )

      sleep(deadline)

      doAssertEmpty(db)

      db.close().get
    }

    "Put & Remove" in {
      val db = newDB()
      //if the deadline is either expired or delay it does not matter in this case because the underlying key-values are removed.
      val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
        right = db.remove(1, keyValueCount).right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )

      doAssertEmpty(db)
      sleep(deadline)
      doAssertEmpty(db)

      db.close().get
    }

    "Put & Update" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
        right = db.update(1, keyValueCount, value = "updated").right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )

      if (deadline.hasTimeLeft())
        (1 to keyValueCount) foreach {
          i =>
            db.expiration(i).right.value.value shouldBe deadline
            db.get(i).right.value.value shouldBe "updated"
        }

      sleep(deadline)

      doAssertEmpty(db)

      db.close().get
    }

    "Put & Expire" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 3.seconds.fromNow)
      val deadline2 = eitherOne(expiredDeadline(), 5.seconds.fromNow)

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline2).right.value),
        right = db.expire(1, keyValueCount, deadline2).right.value
      )

      if (deadline.hasTimeLeft() && deadline2.hasTimeLeft())
        (1 to keyValueCount) foreach {
          i =>
            db.expiration(i).right.value shouldBe defined
            db.get(i).right.value.value shouldBe i.toString
        }

      sleep(deadline)

      doAssertEmpty(db)

      db.close().get
    }

    "Put & Put" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 3.seconds.fromNow)

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
      (1 to keyValueCount) foreach { i => db.put(i, i.toString + " replaced").right.value }

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )

      if (deadline.hasTimeLeft())
        (1 to keyValueCount) foreach {
          i =>
            db.expiration(i).right.value.value shouldBe deadline
            db.get(i).right.value.value shouldBe (i.toString + " replaced")
        }

      sleep(deadline)

      doAssertEmpty(db)

      db.close().get
    }
  }

  "Expire" when {
    "Update" in {
      val db: Map[Int, String, Nothing, IO.ApiIO] = newDB()

      val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
        right = db.update(1, keyValueCount, value = "updated").right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )

      doAssertEmpty(db)
      sleep(deadline)
      doAssertEmpty(db)

      db.close().get
    }

    "Update & Remove" in {
      val db = newDB()
      //if the deadline is either expired or delay it does not matter in this case because the underlying key-values are removed.
      val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
        right = db.update(1, keyValueCount, value = "updated").right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
        right = db.remove(1, keyValueCount).right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )

      doAssertEmpty(db)
      sleep(deadline)
      doAssertEmpty(db)

      db.close().get
    }

    "Update & Update" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 1").right.value),
        right = db.update(1, keyValueCount, value = "updated 1").right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 2").right.value),
        right = db.update(1, keyValueCount, value = "updated 2").right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )

      doAssertEmpty(db)
      sleep(deadline)
      doAssertEmpty(db)

      db.close().get
    }

    "Update & Expire" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 3.seconds.fromNow)
      val deadline2 = eitherOne(expiredDeadline(), 5.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 1").right.value),
        right = db.update(1, keyValueCount, value = "updated 1").right.value
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline2).right.value),
        right = db.expire(1, keyValueCount, deadline2).right.value
      )

      doAssertEmpty(db)
      sleep(deadline2)
      doAssertEmpty(db)

      db.close().get
    }

    "Update & Put" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 3.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 1").right.value),
        right = db.update(1, keyValueCount, value = "updated 1").right.value
      )

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )

      if (deadline.hasTimeLeft())
        (1 to keyValueCount) foreach {
          i =>
            db.expiration(i).right.value.value shouldBe deadline
            db.get(i).right.value.value shouldBe i.toString
        }

      sleep(deadline)

      doAssertEmpty(db)

      db.close().get
    }
  }

  "Expire" when {
    "Remove" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
        right = db.remove(1, keyValueCount).right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )

      doAssertEmpty(db)
      sleep(deadline)
      doAssertEmpty(db)

      db.close().get
    }

    "Remove & Remove" in {
      val db = newDB()
      //if the deadline is either expired or delay it does not matter in this case because the underlying key-values are removed.
      val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
        right = db.remove(1, keyValueCount).right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
        right = db.remove(1, keyValueCount).right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )

      doAssertEmpty(db)
      sleep(deadline)
      doAssertEmpty(db)

      db.close().get
    }

    "Remove & Update" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
        right = db.remove(1, keyValueCount).right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
        right = db.update(1, keyValueCount, value = "updated").right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )

      doAssertEmpty(db)
      sleep(deadline)
      doAssertEmpty(db)

      db.close().get
    }

    "Remove & Expire" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 3.seconds.fromNow)
      val deadline2 = eitherOne(expiredDeadline(), 5.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
        right = db.remove(1, keyValueCount).right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline2).right.value),
        right = db.expire(1, keyValueCount, deadline2).right.value
      )

      doAssertEmpty(db)
      sleep(deadline)
      doAssertEmpty(db)

      db.close().get
    }

    "Remove & Put" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 3.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
        right = db.remove(1, keyValueCount).right.value
      )

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )

      if (deadline.hasTimeLeft())
        (1 to keyValueCount) foreach {
          i =>
            db.expiration(i).right.value.value shouldBe deadline
            db.get(i).right.value.value shouldBe i.toString
        }

      sleep(deadline)
      doAssertEmpty(db)

      db.close().get
    }
  }

  "Expire" when {
    "Expire" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)
      val deadline2 = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline2).right.value),
        right = db.expire(1, keyValueCount, deadline2).right.value
      )

      doAssertEmpty(db)
      sleep(deadline)
      doAssertEmpty(db)

      db.close().get
    }

    "Expire & Remove" in {
      val db = newDB()
      //if the deadline is either expired or delay it does not matter in this case because the underlying key-values are removed.
      val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)
      val deadline2 = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
        right = db.remove(1, keyValueCount).right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline2).right.value),
        right = db.expire(1, keyValueCount, deadline2).right.value
      )

      doAssertEmpty(db)
      sleep(deadline)
      doAssertEmpty(db)

      db.close().get
    }

    "Expire & Update" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)
      val deadline2 = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
        right = db.update(1, keyValueCount, value = "updated").right.value
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline2).right.value),
        right = db.expire(1, keyValueCount, deadline2).right.value
      )

      doAssertEmpty(db)
      sleep(deadline)
      doAssertEmpty(db)

      db.close().get
    }

    "Expire & Expire" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)
      val deadline2 = eitherOne(expiredDeadline(), 4.seconds.fromNow)
      val deadline3 = eitherOne(expiredDeadline(), 5.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline2).right.value),
        right = db.expire(1, keyValueCount, deadline2).right.value
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline3).right.value),
        right = db.expire(1, keyValueCount, deadline3).right.value
      )

      doAssertEmpty(db)
      sleep(deadline3)
      doAssertEmpty(db)

      db.close().get
    }

    "Expire & Put" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)
      val deadline2 = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
        right = db.expire(1, keyValueCount, deadline).right.value
      )

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline2).right.value),
        right = db.expire(1, keyValueCount, deadline2).right.value
      )

      if (deadline2.hasTimeLeft())
        (1 to keyValueCount) foreach {
          i =>
            db.expiration(i).right.value.value shouldBe deadline2
            db.get(i).right.value.value shouldBe i.toString
        }

      sleep(deadline2)
      doAssertEmpty(db)

      db.close().get
    }
  }
}
