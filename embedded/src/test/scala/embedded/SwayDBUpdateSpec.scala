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
import swaydb.serializers.Default._
import swaydb.types.SwayDBMap

import scala.concurrent.duration._

class SwayDBUpdateSpec0 extends SwayDBUpdateSpec {
  val keyValueCount: Int = 1000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): SwayDBMap[Int, String] =
    SwayDB.persistent[Int, String](dir = randomDir, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
}

class SwayDBUpdateSpec1 extends SwayDBUpdateSpec {

  val keyValueCount: Int = 1000

  import swaydb._

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): SwayDBMap[Int, String] =
    SwayDB.persistent[Int, String](randomDir, mapSize = 1.byte, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
}

class SwayDBUpdateSpec2 extends SwayDBUpdateSpec {

  val keyValueCount: Int = 10000

  import swaydb._

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): SwayDBMap[Int, String] =
    SwayDB.memory[Int, String](mapSize = 1.byte, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
}

class SwayDBUpdateSpec3 extends SwayDBUpdateSpec {
  val keyValueCount: Int = 10000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): SwayDBMap[Int, String] =
    SwayDB.memory[Int, String](minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
}

sealed trait SwayDBUpdateSpec extends TestBase with TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(minTimeLeftToUpdateExpiration: FiniteDuration = 10.seconds): SwayDBMap[Int, String]

  "Updating" when {
    "Put" in {
      val db = newDB()

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).assertGet }
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated").assertGet),
        right = db.update(1, keyValueCount, "updated").assertGet
      )

      (1 to keyValueCount) foreach {
        i =>
          db.deadline(i).assertGetOpt shouldBe empty
          db.get(i).assertGet shouldBe "updated"
      }
    }

    "Put & Expire" in {
      val db = newDB()

      val deadline = eitherOne(4.seconds.fromNow, expiredDeadline())

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).assertGet }
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).assertGet),
        right = db.expire(1, keyValueCount, deadline).assertGet
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated").assertGet),
        right = db.update(1, keyValueCount, "updated").assertGet
      )

      if (deadline.hasTimeLeft())
        (1 to keyValueCount) foreach {
          i =>
            db.deadline(i).assertGet shouldBe deadline
            db.get(i).assertGet shouldBe "updated"
        }

      if (deadline.hasTimeLeft()) sleep(deadline.timeLeft)

      doAssertEmpty(db)
    }

    "Put & Remove" in {
      val db = newDB()

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).assertGet }
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).assertGet),
        right = db.remove(1, keyValueCount).assertGet
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated").assertGet),
        right = db.update(1, keyValueCount, "updated").assertGet
      )

      doAssertEmpty(db)
    }

    "Put & Update" in {
      val db = newDB()

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).assertGet }
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated").assertGet),
        right = db.update(1, keyValueCount, "updated").assertGet
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated again").assertGet),
        right = db.update(1, keyValueCount, "updated again").assertGet
      )

      (1 to keyValueCount) foreach {
        i =>
          db.deadline(i).assertGetOpt shouldBe empty
          db.get(i).assertGet shouldBe "updated again"
      }
    }
  }

  "Updating" when {
    "Remove" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).assertGet),
        right = db.remove(1, keyValueCount).assertGet
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated").assertGet),
        right = db.update(1, keyValueCount, "updated").assertGet
      )

      doAssertEmpty(db)
    }

    "Remove & Put" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).assertGet),
        right = db.remove(1, keyValueCount).assertGet
      )
      (1 to keyValueCount) foreach { i => db.put(i, i.toString).assertGet }

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated").assertGet),
        right = db.update(1, keyValueCount, "updated").assertGet
      )

      (1 to keyValueCount) foreach {
        i =>
          db.deadline(i).assertGetOpt shouldBe empty
          db.get(i).assertGet shouldBe "updated"
      }
    }

    "Remove & Update" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).assertGet),
        right = db.remove(1, keyValueCount).assertGet
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated").assertGet),
        right = db.update(1, keyValueCount, "updated").assertGet
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated again").assertGet),
        right = db.update(1, keyValueCount, "updated again").assertGet
      )

      doAssertEmpty(db)
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

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated").assertGet),
        right = db.update(1, keyValueCount, "updated").assertGet
      )

      doAssertEmpty(db)
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

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated").assertGet),
        right = db.update(1, keyValueCount, "updated").assertGet
      )

      doAssertEmpty(db)
    }
  }

  "Updating" when {
    "Update" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "old updated").assertGet),
        right = db.update(1, keyValueCount, "old updated").assertGet
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated").assertGet),
        right = db.update(1, keyValueCount, "updated").assertGet
      )

      doAssertEmpty(db)
    }

    "Update & Put" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated").assertGet),
        right = db.update(1, keyValueCount, "updated").assertGet
      )

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).assertGet }

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated 2").assertGet),
        right = db.update(1, keyValueCount, "updated 2").assertGet
      )

      (1 to keyValueCount) foreach {
        i =>
          db.deadline(i).assertGetOpt shouldBe empty
          db.get(i).assertGet shouldBe "updated 2"
      }
    }

    "Update & Update" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated 1").assertGet),
        right = db.update(1, keyValueCount, "updated 1").assertGet
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated 2").assertGet),
        right = db.update(1, keyValueCount, "updated 2").assertGet
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated again 3").assertGet),
        right = db.update(1, keyValueCount, "updated again 3").assertGet
      )

      doAssertEmpty(db)
    }

    "Update & Expire" in {
      val db = newDB()

      val deadline = eitherOne(2.seconds.fromNow, expiredDeadline())

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated 1").assertGet),
        right = db.update(1, keyValueCount, "updated 1").assertGet
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).assertGet),
        right = db.expire(1, keyValueCount, deadline).assertGet
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated 2").assertGet),
        right = db.update(1, keyValueCount, "updated 2").assertGet
      )

      doAssertEmpty(db)
    }

    "Update & Remove" in {
      val db = newDB()

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated 1").assertGet),
        right = db.update(1, keyValueCount, "updated 1").assertGet
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).assertGet),
        right = db.remove(1, keyValueCount).assertGet
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated 2").assertGet),
        right = db.update(1, keyValueCount, "updated 2").assertGet
      )

      doAssertEmpty(db)
    }
  }

  "Updating" when {
    "Expire" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).assertGet),
        right = db.expire(1, keyValueCount, deadline).assertGet
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated").assertGet),
        right = db.update(1, keyValueCount, "updated").assertGet
      )

      doAssertEmpty(db)
      sleep(deadline)
      doAssertEmpty(db)
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
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated").assertGet),
        right = db.update(1, keyValueCount, "updated").assertGet
      )

      doAssertEmpty(db)
      sleep(deadline)
      doAssertEmpty(db)
    }

    "Expire & Update" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).assertGet),
        right = db.expire(1, keyValueCount, deadline).assertGet
      )

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated").assertGet),
        right = db.update(1, keyValueCount, "updated").assertGet
      )
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated").assertGet),
        right = db.update(1, keyValueCount, "updated").assertGet
      )

      doAssertEmpty(db)
      sleep(deadline)
      doAssertEmpty(db)
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

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated").assertGet),
        right = db.update(1, keyValueCount, "updated").assertGet
      )

      doAssertEmpty(db)
      sleep(deadline2)
      doAssertEmpty(db)
    }

    "Expire & Put" in {
      val db = newDB()

      val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).assertGet),
        right = db.expire(1, keyValueCount, deadline).assertGet
      )

      (1 to keyValueCount) foreach { i => db.put(i, i.toString).assertGet }

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.update(i, "updated").assertGet),
        right = db.update(1, keyValueCount, "updated").assertGet
      )

      def doAssert() =
      (1 to keyValueCount) foreach {
        i =>
          db.deadline(i).assertGetOpt shouldBe empty
          db.get(i).assertGet shouldBe "updated"
      }

      doAssert()
      sleep(deadline)
      doAssert()
    }
  }
}