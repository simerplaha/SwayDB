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

import swaydb.api.TestBaseEmbedded
import swaydb.core.CommonAssertions._
import swaydb.core.IOValues._
import swaydb.core.RunThis._
import swaydb.data.IO
import swaydb.serializers.Default._

import scala.concurrent.duration._

class SwayDBUpdateSpec0 extends SwayDBUpdateSpec {
  val keyValueCount: Int = 1000

  override def newDB(): Map[Int, String, IO] =
    swaydb.persistent.Map[Int, String](dir = randomDir).runIO
}

class SwayDBUpdateSpec1 extends SwayDBUpdateSpec {

  val keyValueCount: Int = 1000

  override def newDB(): Map[Int, String, IO] =
    swaydb.persistent.Map[Int, String](randomDir, mapSize = 1.byte).runIO
}

class SwayDBUpdateSpec2 extends SwayDBUpdateSpec {

  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String, IO] =
    swaydb.memory.Map[Int, String](mapSize = 1.byte).runIO
}

class SwayDBUpdateSpec3 extends SwayDBUpdateSpec {
  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String, IO] =
    swaydb.memory.Map[Int, String]().runIO
}

class SwayDBUpdateSpec4 extends SwayDBUpdateSpec {

  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String, IO] =
    swaydb.memory.zero.Map[Int, String](mapSize = 1.byte).runIO
}

class SwayDBUpdateSpec5 extends SwayDBUpdateSpec {
  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String, IO] =
    swaydb.memory.zero.Map[Int, String]().runIO
}

sealed trait SwayDBUpdateSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): Map[Int, String, IO]

  "Updating" when {
    "Put" in {
      val db = newDB()

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).runIO }
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").runIO),
        right = db.update(1, keyValueCount, value = "updated").runIO
      )

      (1 to keyValueCount) foreach {
        i =>
          db.expiration(i).runIO shouldBe empty
          db.get(i).runIOValue shouldBe "updated"
      }

      db.close().get
    }

    "Put & Expire" in {
      val db = newDB()

      val deadline = eitherOne(4.seconds.fromNow, expiredDeadline())

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).runIO }
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).runIO),
        right = db.expire(1, keyValueCount, deadline).runIO
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").runIO),
        right = db.update(1, keyValueCount, value = "updated").runIO
      )

      if (deadline.hasTimeLeft())
        (1 to keyValueCount) foreach {
          i =>
            db.expiration(i).runIOValue shouldBe deadline
            db.get(i).runIOValue shouldBe "updated"
        }

      if (deadline.hasTimeLeft()) sleep(deadline.timeLeft)

      doAssertEmpty(db)

      db.close().get
    }

    "Put & Remove" in {
      val db = newDB()

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).runIO }
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).runIO),
        right = db.remove(1, keyValueCount).runIO
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").runIO),
        right = db.update(1, keyValueCount, value = "updated").runIO
      )

      doAssertEmpty(db)

      db.close().get
    }

    "Put & Update" in {
      val db = newDB()

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).runIO }
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").runIO),
        right = db.update(1, keyValueCount, value = "updated").runIO
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated again").runIO),
        right = db.update(1, keyValueCount, value = "updated again").runIO
      )

      (1 to keyValueCount) foreach {
        i =>
          db.expiration(i).runIO shouldBe empty
          db.get(i).runIOValue shouldBe "updated again"
      }

      db.close().get
    }
  }

  "Updating" when {
    "Remove" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).runIO),
        right = db.remove(1, keyValueCount).runIO
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").runIO),
        right = db.update(1, keyValueCount, value = "updated").runIO
      )

      doAssertEmpty(db)

      db.close().get
    }

    "Remove & Put" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).runIO),
        right = db.remove(1, keyValueCount).runIO
      )
      (1 to keyValueCount) foreach { i => db.put(i, i.toString).runIO }

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").runIO),
        right = db.update(1, keyValueCount, value = "updated").runIO
      )

      (1 to keyValueCount) foreach {
        i =>
          db.expiration(i).runIO shouldBe empty
          db.get(i).runIOValue shouldBe "updated"
      }

      db.close().get
    }

    "Remove & Update" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).runIO),
        right = db.remove(1, keyValueCount).runIO
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").runIO),
        right = db.update(1, keyValueCount, value = "updated").runIO
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated again").runIO),
        right = db.update(1, keyValueCount, value = "updated again").runIO
      )

      doAssertEmpty(db)

      db.close().get
    }

    "Remove & Expire" in {
      val db = newDB()

      val deadline = eitherOne(2.seconds.fromNow, expiredDeadline())

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).runIO),
        right = db.remove(1, keyValueCount).runIO
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).runIO),
        right = db.expire(1, keyValueCount, deadline).runIO
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").runIO),
        right = db.update(1, keyValueCount, value = "updated").runIO
      )

      doAssertEmpty(db)

      db.close().get
    }

    "Remove & Remove" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).runIO),
        right = db.remove(1, keyValueCount).runIO
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).runIO),
        right = db.remove(1, keyValueCount).runIO
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").runIO),
        right = db.update(1, keyValueCount, value = "updated").runIO
      )

      doAssertEmpty(db)

      db.close().get
    }
  }

  "Updating" when {
    "Update" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "old updated").runIO),
        right = db.update(1, keyValueCount, value = "old updated").runIO
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").runIO),
        right = db.update(1, keyValueCount, value = "updated").runIO
      )

      doAssertEmpty(db)

      db.close().get
    }

    "Update & Put" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").runIO),
        right = db.update(1, keyValueCount, value = "updated").runIO
      )

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).runIO }

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 2").runIO),
        right = db.update(1, keyValueCount, value = "updated 2").runIO
      )

      (1 to keyValueCount) foreach {
        i =>
          db.expiration(i).runIO shouldBe empty
          db.get(i).runIOValue shouldBe "updated 2"
      }

      db.close().get
    }

    "Update & Update" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 1").runIO),
        right = db.update(1, keyValueCount, value = "updated 1").runIO
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 2").runIO),
        right = db.update(1, keyValueCount, value = "updated 2").runIO
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated again 3").runIO),
        right = db.update(1, keyValueCount, value = "updated again 3").runIO
      )

      doAssertEmpty(db)

      db.close().get
    }

    "Update & Expire" in {
      val db = newDB()

      val deadline = eitherOne(2.seconds.fromNow, expiredDeadline())

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 1").runIO),
        right = db.update(1, keyValueCount, value = "updated 1").runIO
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).runIO),
        right = db.expire(1, keyValueCount, deadline).runIO
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 2").runIO),
        right = db.update(1, keyValueCount, value = "updated 2").runIO
      )

      doAssertEmpty(db)

      db.close().get
    }

    "Update & Remove" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 1").runIO),
        right = db.update(1, keyValueCount, value = "updated 1").runIO
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).runIO),
        right = db.remove(1, keyValueCount).runIO
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 2").runIO),
        right = db.update(1, keyValueCount, value = "updated 2").runIO
      )

      doAssertEmpty(db)

      db.close().get
    }
  }

  "Updating" when {
    "Expire" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).runIO),
        right = db.expire(1, keyValueCount, deadline).runIO
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").runIO),
        right = db.update(1, keyValueCount, value = "updated").runIO
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

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).runIO),
        right = db.expire(1, keyValueCount, deadline).runIO
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).runIO),
        right = db.remove(1, keyValueCount).runIO
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").runIO),
        right = db.update(1, keyValueCount, value = "updated").runIO
      )

      doAssertEmpty(db)
      sleep(deadline)
      doAssertEmpty(db)

      db.close().get
    }

    "Expire & Update" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).runIO),
        right = db.expire(1, keyValueCount, deadline).runIO
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").runIO),
        right = db.update(1, keyValueCount, value = "updated").runIO
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").runIO),
        right = db.update(1, keyValueCount, value = "updated").runIO
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

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).runIO),
        right = db.expire(1, keyValueCount, deadline).runIO
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline2).runIO),
        right = db.expire(1, keyValueCount, deadline2).runIO
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").runIO),
        right = db.update(1, keyValueCount, value = "updated").runIO
      )

      doAssertEmpty(db)
      sleep(deadline2)
      doAssertEmpty(db)

      db.close().get
    }

    "Expire & Put" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).runIO),
        right = db.expire(1, keyValueCount, deadline).runIO
      )

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).runIO }

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").runIO),
        right = db.update(1, keyValueCount, value = "updated").runIO
      )

      def doAssert() =
        (1 to keyValueCount) foreach {
          i =>
            db.expiration(i).runIO shouldBe empty
            db.get(i).runIOValue shouldBe "updated"
        }

      doAssert()
      sleep(deadline)
      doAssert()

      db.close().get
    }
  }
}
