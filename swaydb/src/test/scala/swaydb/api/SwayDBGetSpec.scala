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
import swaydb.core.TestCaseSweeper
import swaydb.core.TestCaseSweeper._
import swaydb.serializers.Default._

import scala.concurrent.duration._

class SwayDBGetSpec0 extends SwayDBGetSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir).right.value.sweep()
}

class SwayDBGet_SetMap_Spec0 extends SwayDBGetSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): SetMap[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.SetMap[Int, String, Nothing, IO.ApiIO](randomDir).right.value.sweep()
}

class SwayDBGetSpec1 extends SwayDBGetSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir, mapSize = 1.byte).right.value.sweep()
}

class SwayDBGetSpec2 extends SwayDBGetSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO](mapSize = 1.byte).right.value.sweep()
}

class SwayDBGetSpec3 extends SwayDBGetSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO]().right.value.sweep()
}

class MultiMapGetSpec4 extends SwayDBGetSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): SetMapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.persistent.MultiMap_EAP[Int, Int, String, Nothing, IO.ApiIO](dir = randomDir).get).sweep()
}

class MultiMapGetSpec5 extends SwayDBGetSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): SetMapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.memory.MultiMap_EAP[Int, Int, String, Nothing, IO.ApiIO]().get).sweep()
}

sealed trait SwayDBGetSpec extends TestBaseEmbedded {

  def newDB()(implicit sweeper: TestCaseSweeper): SetMapT[Int, String, Nothing, IO.ApiIO]

  override val keyValueCount: Int = 1000

  "SwayDB" should {
    "get" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
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

        }
      }
    }

    "return empty for removed key-value" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
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

        }
      }
    }

    "return empty for expired key-value" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
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
              case _: MultiMap_EAP[Int, Int, String, Nothing, IO.ApiIO] =>
                assertThrows[NotImplementedError](db.keySet)

              case _ =>
                db.keySet should contain allElementsOf ((1 to 9) ++ (91 to 100))
            }

        }
      }
    }

    "return empty for range expired key-value" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
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

        }
      }
    }
  }
}
