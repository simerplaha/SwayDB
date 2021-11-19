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

class SwayDBExpireSpec0 extends SwayDBExpireSpec {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): SetMapT[Int, String, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](dir = randomDir).right.value.sweep(_.delete().get)
}

class SwayDBExpire_SetMap_Spec0 extends SwayDBExpireSpec {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): SetMapT[Int, String, IO.ApiIO] =
    swaydb.persistent.SetMap[Int, String, IO.ApiIO](dir = randomDir).right.value.sweep(_.delete().get)
}

class SwayDBExpireSpec1 extends SwayDBExpireSpec {

  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): SetMapT[Int, String, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir, logSize = 1.byte, segmentConfig = swaydb.persistent.DefaultConfigs.segmentConfig().copy(minSegmentSize = 10.bytes)).right.value.sweep(_.delete().get)
}

class SwayDBExpireSpec2 extends SwayDBExpireSpec {

  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): SetMapT[Int, String, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO](logSize = 1.byte).right.value.sweep(_.delete().get)
}

class SwayDBExpireSpec3 extends SwayDBExpireSpec {
  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): SetMapT[Int, String, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO]().right.value.sweep(_.delete().get)
}

class MultiMapExpireSpec4 extends SwayDBExpireSpec {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): SetMapT[Int, String, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.persistent.MultiMap[Int, Int, String, Nothing, IO.ApiIO](dir = randomDir).get).sweep(_.delete().get)
}

class MultiMapExpireSpec5 extends SwayDBExpireSpec {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): SetMapT[Int, String, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.memory.MultiMap[Int, Int, String, Nothing, IO.ApiIO]().get).sweep(_.delete().get)
}

sealed trait SwayDBExpireSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB()(implicit sweeper: TestCaseSweeper): SetMapT[Int, String, IO.ApiIO]

  "Expire" when {
    "Put" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

            doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)

            sleep(deadline.timeLeft)

            doAssertEmpty(db)
        }
      }
    }

    "Put & Remove" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()
            //if the deadline is either expired or delay it does not matter in this case because the underlying key-values are removed.
            val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

            db match {
              case db: MapT[Int, String, Nothing, IO.ApiIO] =>
                eitherOne(
                  left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
                  right = db.remove(1, keyValueCount).right.value
                )

              case db @ SetMap(_) =>
                //setMap does not have range expiration
                (1 to keyValueCount) foreach (i => db.remove(i).right.value)
            }

            doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)

            doAssertEmpty(db)
            sleep(deadline)
            doAssertEmpty(db)
        }
      }
    }

    "Put & Update" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()

            db match {
              case db: MapT[Int, String, Nothing, IO.ApiIO] =>
                val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

                (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

                eitherOne(
                  left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
                  right = db.update(from = 1, to = keyValueCount, value = "updated").right.value
                )
                eitherOne(
                  left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).right.value),
                  right = db.expire(from = 1, to = keyValueCount, deadline).right.value
                )

                if (deadline.hasTimeLeft())
                  (1 to keyValueCount) foreach {
                    i =>
                      db.expiration(i).right.value.value shouldBe deadline
                      db.get(i).right.value.value shouldBe "updated"
                  }

                sleep(deadline)

                doAssertEmpty(db)

              case SetMap(set) =>
              //no update in SetMap
            }
        }
      }
    }

    "Put & Expire" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()

            val deadline = eitherOne(expiredDeadline(), 3.seconds.fromNow)
            val deadline2 = eitherOne(expiredDeadline(), 5.seconds.fromNow)

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

            doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)
            doExpire(from = 1, to = keyValueCount, deadline = deadline2, db = db)

            if (deadline.hasTimeLeft() && deadline2.hasTimeLeft())
              (1 to keyValueCount) foreach {
                i =>
                  db.expiration(i).right.value shouldBe defined
                  val value = db.get(i).right.value.value
                  value shouldBe i.toString
              }

            sleep(deadline2 + 1.second)

            doAssertEmpty(db)
        }
      }
    }

    "Put & Put" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()

            val deadline = eitherOne(expiredDeadline(), 3.seconds.fromNow)

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }
            (1 to keyValueCount) foreach { i => db.put(i, i.toString + " replaced").right.value }

            doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)

            if (deadline.hasTimeLeft())
              (1 to keyValueCount) foreach {
                i =>
                  db.expiration(i).right.value.value shouldBe deadline
                  db.get(i).right.value.value shouldBe (i.toString + " replaced")
              }

            sleep(deadline)

            doAssertEmpty(db)
        }
      }
    }
  }

  "Expire" when {
    "Update" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()

            val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

            db match {
              case db: MapT[Int, String, Nothing, IO.ApiIO] =>
                eitherOne(
                  left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
                  right = db.update(1, keyValueCount, value = "updated").right.value
                )

              case SetMap(_) =>
              //no update
            }

            doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)

            doAssertEmpty(db)
            sleep(deadline)
            doAssertEmpty(db)
        }
      }
    }

    "Update & Remove" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()
            //if the deadline is either expired or delay it does not matter in this case because the underlying key-values are removed.
            val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

            db match {
              case db: MapT[Int, String, Nothing, IO.ApiIO] =>
                eitherOne(
                  left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
                  right = db.update(1, keyValueCount, value = "updated").right.value
                )

                eitherOne(
                  left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
                  right = db.remove(from = 1, to = keyValueCount).right.value
                )

              case SetMap(_) =>
                //no update

                (1 to keyValueCount) foreach (i => db.remove(i).right.value)
            }


            doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)

            doAssertEmpty(db)
            sleep(deadline)
            doAssertEmpty(db)
        }
      }
    }

    "Update & Update" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()

            db match {
              case db: MapT[Int, String, Nothing, IO.ApiIO] =>
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

              case SetMap(set) =>
              //nothing
            }
        }
      }
    }

    "Update & Expire" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()

            db match {
              case db: MapT[Int, String, Nothing, IO.ApiIO] =>

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

              case SetMap(set) =>
              //nothing
            }
        }
      }
    }

    "Update & Put" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()

            val deadline = eitherOne(expiredDeadline(), 3.seconds.fromNow)

            db match {
              case db: MapT[Int, String, Nothing, IO.ApiIO] =>
                eitherOne(
                  left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated 1").right.value),
                  right = db.update(1, keyValueCount, value = "updated 1").right.value
                )

              case SetMap(set) =>
              //nothing
            }

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

            doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)

            if (deadline.hasTimeLeft())
              (1 to keyValueCount) foreach {
                i =>
                  db.expiration(i).right.value.value shouldBe deadline
                  db.get(i).right.value.value shouldBe i.toString
              }

            sleep(deadline)

            doAssertEmpty(db)
        }
      }
    }
  }

  "Expire" when {
    "Remove" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()

            val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)

            doAssertEmpty(db)
            sleep(deadline)
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
            val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )
            doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)

            doAssertEmpty(db)
            sleep(deadline)
            doAssertEmpty(db)
        }
      }
    }

    "Remove & Update" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()

            val deadline = eitherOne(expiredDeadline(), 4.seconds.fromNow)

            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )

            db match {
              case db: MapT[Int, String, Nothing, IO.ApiIO] =>
                eitherOne(
                  left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
                  right = db.update(1, keyValueCount, value = "updated").right.value
                )

              case SetMap(set) =>
              //no update
            }

            doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)

            doAssertEmpty(db)
            sleep(deadline)
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
            doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)
            doExpire(from = 1, to = keyValueCount, deadline = deadline2, db = db)

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

            doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)

            if (deadline.hasTimeLeft())
              (1 to keyValueCount) foreach {
                i =>
                  db.expiration(i).right.value.value shouldBe deadline
                  db.get(i).right.value.value shouldBe i.toString
              }

            sleep(deadline)
            doAssertEmpty(db)
        }
      }
    }
  }

  "Expire" when {
    "Expire" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()

            val deadline = eitherOne(expiredDeadline(), 2.seconds.fromNow)
            val deadline2 = eitherOne(expiredDeadline(), 4.seconds.fromNow)

            doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)

            doExpire(from = 1, to = keyValueCount, deadline = deadline2, db = db)

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
            val deadline2 = eitherOne(expiredDeadline(), 4.seconds.fromNow)

            doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)
            eitherOne(
              left = (1 to keyValueCount) foreach (i => db.remove(i).right.value),
              right = db.remove(1, keyValueCount).right.value
            )
            doExpire(from = 1, to = keyValueCount, deadline = deadline2, db = db)

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
            val deadline2 = eitherOne(expiredDeadline(), 4.seconds.fromNow)

            doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)

            db match {
              case db: MapT[Int, String, Nothing, IO.ApiIO] =>
                eitherOne(
                  left = (1 to keyValueCount) foreach (i => db.update(i, value = "updated").right.value),
                  right = db.update(1, keyValueCount, value = "updated").right.value
                )

              case SetMap(set) =>

            }

            doExpire(from = 1, to = keyValueCount, deadline = deadline2, db = db)

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
            val deadline3 = eitherOne(expiredDeadline(), 5.seconds.fromNow)

            doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)
            doExpire(from = 1, to = keyValueCount, deadline = deadline2, db = db)
            doExpire(from = 1, to = keyValueCount, deadline = deadline3, db = db)

            doAssertEmpty(db)
            sleep(deadline3)
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

            doExpire(from = 1, to = keyValueCount, deadline = deadline, db = db)

            (1 to keyValueCount) foreach { i => db.put(i, i.toString).right.value }

            doExpire(from = 1, to = keyValueCount, deadline = deadline2, db = db)

            if (deadline2.hasTimeLeft())
              (1 to keyValueCount) foreach {
                i =>
                  db.expiration(i).right.value.value shouldBe deadline2
                  db.get(i).right.value.value shouldBe i.toString
              }

            sleep(deadline2)
            doAssertEmpty(db)
        }
      }
    }
  }
}
