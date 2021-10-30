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
import swaydb.core.TestCaseSweeper
import swaydb.core.TestCaseSweeper._
import swaydb.serializers.Default._
import swaydb.testkit.RunThis._

import scala.concurrent.duration._

class SwayDBGetSpec0 extends SwayDBGetSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir).right.value.sweep(_.delete().get)
}

class SwayDBGet_SetMap_Spec0 extends SwayDBGetSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): SetMap[Int, String, IO.ApiIO] =
    swaydb.persistent.SetMap[Int, String, IO.ApiIO](randomDir).right.value.sweep(_.delete().get)
}

class SwayDBGet_Eventually_Persistent_SetMap_Spec0 extends SwayDBGetSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): SetMap[Int, String, IO.ApiIO] =
    swaydb.eventually.persistent.SetMap[Int, String, IO.ApiIO](randomDir).right.value.sweep(_.delete().get)
}

class SwayDBGetSpec1 extends SwayDBGetSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir, logSize = 1.byte).right.value.sweep(_.delete().get)
}

class SwayDBGetSpec2 extends SwayDBGetSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO](logSize = 1.byte).right.value.sweep(_.delete().get)
}

class SwayDBGetSpec3 extends SwayDBGetSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO]().right.value.sweep(_.delete().get)
}

class MultiMapGetSpec4 extends SwayDBGetSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): SetMapT[Int, String, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.persistent.MultiMap[Int, Int, String, Nothing, IO.ApiIO](dir = randomDir).get).sweep(_.delete().get)
}

class MultiMapGetSpec5 extends SwayDBGetSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): SetMapT[Int, String, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.memory.MultiMap[Int, Int, String, Nothing, IO.ApiIO]().get).sweep(_.delete().get)
}

sealed trait SwayDBGetSpec extends TestBaseEmbedded {

  def newDB()(implicit sweeper: TestCaseSweeper): SetMapT[Int, String, IO.ApiIO]

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
              case _: MultiMap[Int, Int, String, Nothing, IO.ApiIO] =>
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
