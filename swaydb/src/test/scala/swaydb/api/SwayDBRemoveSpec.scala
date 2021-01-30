/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
import swaydb.core.CommonAssertions._
import swaydb.core.TestCaseSweeper
import swaydb.core.TestCaseSweeper._
import swaydb.serializers.Default._
import swaydb.testkit.RunThis._

import scala.concurrent.duration._

class SwayDBRemoveSpec0 extends SwayDBRemoveSpec {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](dir = randomDir).right.value.sweep(_.delete().get)
}

class SwayDBRemoveSpec1 extends SwayDBRemoveSpec {

  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir, mapSize = 1.byte, segmentConfig = swaydb.persistent.DefaultConfigs.segmentConfig().copy(minSegmentSize = 10.bytes)).right.value.sweep(_.delete().get)
}

class SwayDBRemoveSpec2 extends SwayDBRemoveSpec {

  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO](mapSize = 1.byte, minSegmentSize = 10.bytes).right.value.sweep(_.delete().get)
}

class SwayDBRemoveSpec3 extends SwayDBRemoveSpec {
  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO]().right.value.sweep(_.delete().get)
}


class MultiMapRemoveSpec4 extends SwayDBRemoveSpec {
  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): MapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.persistent.MultiMap[Int, Int, String, Nothing, IO.ApiIO](dir = randomDir).get).sweep(_.delete().get)
}

class MultiMapRemoveSpec5 extends SwayDBRemoveSpec {
  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): MapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.memory.MultiMap[Int, Int, String, Nothing, IO.ApiIO]().get).sweep(_.delete().get)
}

sealed trait SwayDBRemoveSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB()(implicit sweeper: TestCaseSweeper): MapT[Int, String, Nothing, IO.ApiIO]

  "Remove" when {
    "Put" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
        }
      }
    }

    "Put & Remove" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
        }
      }
    }

    "Put & Update" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
        }
      }
    }

    "Put & Expire" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()

            val deadline = eitherOne(expiredDeadline(), 3.seconds.fromNow)

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
              right = db.expire(1, keyValueCount, deadline).right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
            sleep(deadline)
            doAssertEmpty(db)
        }
      }
    }

    "Put & Put" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
            (1 to keyValueCount) foreach { i => db.put(i, i.toString + " replaced").right.value }

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
        }
      }
    }
  }

  "Remove" when {
    "Update" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
        }
      }
    }

    "Update & Remove" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
        }
      }
    }

    "Update & Update" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 1").right.value),
              right = db.update(1, keyValueCount, value = "updated 1").right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 2").right.value),
              right = db.update(1, keyValueCount, value = "updated 2").right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
        }
      }
    }

    "Update & Expire" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

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
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
            sleep(deadline2)
            doAssertEmpty(db)
        }
      }
    }

    "Update & Put" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 1").right.value),
              right = db.update(1, keyValueCount, value = "updated 1").right.value
            )

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
        }
      }
    }
  }

  "Remove" when {
    "Remove" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
        }
      }
    }

    "Remove & Remove" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()
            //if the deadline is either expired or delay it does not matter in this case because the underlying key-values are removed.

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
        }
      }
    }

    "Remove & Update" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
        }
      }
    }

    "Remove & Expire" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

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
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
            sleep(deadline)
            doAssertEmpty(db)
        }
      }
    }

    "Remove & Put" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            val deadline = eitherOne(expiredDeadline(), 3.seconds.fromNow)

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
            sleep(deadline)
            doAssertEmpty(db)
        }
      }
    }
  }

  "Remove" when {
    "Expire" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
              right = db.expire(1, keyValueCount, deadline).right.value
            )

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
            sleep(deadline)
            doAssertEmpty(db)
        }
      }
    }

    "Expire & Remove" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()
            //if the deadline is either expired or delay it does not matter in this case because the underlying key-values are removed.
            val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
              right = db.expire(1, keyValueCount, deadline).right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
            sleep(deadline)
            doAssertEmpty(db)
        }
      }
    }

    "Expire & Update" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
              right = db.expire(1, keyValueCount, deadline).right.value
            )

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
            sleep(deadline)
            doAssertEmpty(db)
        }
      }
    }

    "Expire & Expire" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

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

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
            sleep(deadline2)
            doAssertEmpty(db)
        }
      }
    }

    "Expire & Put" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)
            val deadline2 = eitherOne(expiredDeadline(), 4.seconds.fromNow)

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
              right = db.expire(1, keyValueCount, deadline).right.value
            )

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doAssertEmpty(db)
            sleep(deadline2)
            doAssertEmpty(db)
        }
      }
    }
  }
}
