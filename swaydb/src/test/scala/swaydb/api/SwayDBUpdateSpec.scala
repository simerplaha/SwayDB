/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.api

import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb._
import swaydb.core.CommonAssertions._
import swaydb.core.TestCaseSweeper
import swaydb.core.TestCaseSweeper._
import swaydb.serializers.Default._
import swaydb.testkit.RunThis._

import scala.concurrent.duration._
import swaydb.testkit.TestKit._

class SwayDBUpdateSpec0 extends SwayDBUpdateSpec {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](dir = randomDir).right.value.sweep(_.delete().get)
}

class SwayDBUpdateSpec1 extends SwayDBUpdateSpec {

  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir, logSize = 1.byte).right.value.sweep(_.delete().get)
}

class SwayDBUpdateSpec2 extends SwayDBUpdateSpec {

  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO](logSize = 1.byte).right.value.sweep(_.delete().get)
}

class SwayDBUpdateSpec3 extends SwayDBUpdateSpec {
  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO]().right.value.sweep(_.delete().get)
}


class SwayDBUpdateSpec4 extends SwayDBUpdateSpec {
  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): MapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.persistent.MultiMap[Int, Int, String, Nothing, IO.ApiIO](dir = randomDir).get).sweep(_.delete().get)
}

class SwayDBUpdateSpec5 extends SwayDBUpdateSpec {
  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): MapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.memory.MultiMap[Int, Int, String, Nothing, IO.ApiIO]().get).sweep(_.delete().get)
}

sealed trait SwayDBUpdateSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB()(implicit sweeper: TestCaseSweeper): MapT[Int, String, Nothing, IO.ApiIO]

  "Updating" when {
    "Put" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
            )

            (1 to keyValueCount) foreach {
              i =>
                db.expiration(i).right.value shouldBe empty
                db.get(i).right.value.value shouldBe "updated"
            }
        }
      }
    }

    "Put & Expire" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            val deadline = eitherOne(4.seconds.fromNow, expiredDeadline())

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
              right = db.expire(1, keyValueCount, deadline).right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
            )

            if (deadline.hasTimeLeft())
              (1 to keyValueCount) foreach {
                i =>
                  db.expiration(i).right.value.value shouldBe deadline
                  db.get(i).right.value.value shouldBe "updated"
              }

            if (deadline.hasTimeLeft()) sleep(deadline.timeLeft)

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
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
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
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated again").right.value),
              right = db.update(1, keyValueCount, value = "updated again").right.value
            )

            (1 to keyValueCount) foreach {
              i =>
                db.expiration(i).right.value shouldBe empty
                db.get(i).right.value.value shouldBe "updated again"
            }
        }
      }
    }
  }

  "Updating" when {
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
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
            )

            doAssertEmpty(db)
        }
      }
    }

    "Remove & Put" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )
            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
            )

            (1 to keyValueCount) foreach {
              i =>
                db.expiration(i).right.value shouldBe empty
                db.get(i).right.value.value shouldBe "updated"
            }
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
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated again").right.value),
              right = db.update(1, keyValueCount, value = "updated again").right.value
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

            val deadline = eitherOne(2.seconds.fromNow, expiredDeadline())

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
              right = db.expire(1, keyValueCount, deadline).right.value
            )

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
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

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
            )

            doAssertEmpty(db)
        }
      }
    }
  }

  "Updating" when {
    "Update" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "old updated").right.value),
              right = db.update(1, keyValueCount, value = "old updated").right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
            )

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
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
            )

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 2").right.value),
              right = db.update(1, keyValueCount, value = "updated 2").right.value
            )

            (1 to keyValueCount) foreach {
              i =>
                db.expiration(i).right.value shouldBe empty
                db.get(i).right.value.value shouldBe "updated 2"
            }
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
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated again 3").right.value),
              right = db.update(1, keyValueCount, value = "updated again 3").right.value
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

            val deadline = eitherOne(2.seconds.fromNow, expiredDeadline())

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 1").right.value),
              right = db.update(1, keyValueCount, value = "updated 1").right.value
            )

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
              right = db.expire(1, keyValueCount, deadline).right.value
            )

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 2").right.value),
              right = db.update(1, keyValueCount, value = "updated 2").right.value
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
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 1").right.value),
              right = db.update(1, keyValueCount, value = "updated 1").right.value
            )

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 2").right.value),
              right = db.update(1, keyValueCount, value = "updated 2").right.value
            )

            doAssertEmpty(db)
        }
      }
    }
  }

  "Updating" when {
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
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
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
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
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
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
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
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
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

            val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
              right = db.expire(1, keyValueCount, deadline).right.value
            )

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
              right = db.update(1, keyValueCount, value = "updated").right.value
            )

            def doAssert() =
              (1 to keyValueCount) foreach {
                i =>
                  db.expiration(i).right.value shouldBe empty
                  db.get(i).right.value.value shouldBe "updated"
              }

            doAssert()
            sleep(deadline)
            doAssert()
        }
      }
    }
  }
}
