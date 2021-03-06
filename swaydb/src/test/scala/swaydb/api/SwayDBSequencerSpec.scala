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

import org.scalatest.OptionValues._
import swaydb.Bag.Implicits._
import swaydb._
import swaydb.core.TestCaseSweeper._
import swaydb.core.{TestCaseSweeper, TestExecutionContext}
import swaydb.data.sequencer.Sequencer
import swaydb.serializers.Default._
import swaydb.testkit.RunThis.FutureImplicits

import scala.concurrent.Future
import scala.util.Try

class SwayDBSequencerSpec0 extends SwayDBSequencerSpec {
  def newDB[BAG[+_]]()(implicit sweeper: TestCaseSweeper,
                       sequencer: Sequencer[BAG],
                       bag: Bag[BAG]): BAG[Map[Int, String, Nothing, BAG]] =
    swaydb.persistent.Map[Int, String, Nothing, BAG](randomDir).map(_.sweep(_.toBag[Glass].delete()))

  override val keyValueCount: Int = 100
}

class SwayDBSequencerSpec3 extends SwayDBSequencerSpec {

  override val keyValueCount: Int = 100

  def newDB[BAG[+_]]()(implicit sweeper: TestCaseSweeper,
                       sequencer: Sequencer[BAG],
                       bag: Bag[BAG]): BAG[Map[Int, String, Nothing, BAG]] =
    swaydb.memory.Map[Int, String, Nothing, BAG]().map(_.sweep(_.toBag[Glass].delete()))
}

sealed trait SwayDBSequencerSpec extends TestBaseEmbedded {

  def newDB[BAG[+_]]()(implicit sweeper: TestCaseSweeper,
                       sequencer: Sequencer[BAG],
                       bag: Bag[BAG]): BAG[SetMapT[Int, String, BAG]]

  "Synchronised bags" should {
    "used Serial.Synchronised" when {
      "serial is null" in {
        TestCaseSweeper {
          implicit sweeper =>

            def doTest[BAG[+_]](implicit bag: Bag.Sync[BAG]) = {
              implicit val sequencer: Sequencer[BAG] = null

              val map = newDB[BAG]().getUnsafe

              val coresSequencer = getSequencer(map)

              coresSequencer shouldBe a[Sequencer.Synchronised[BAG]]

              map.put(1, "one")
              map.get(1).getUnsafe.value shouldBe "one"

              map.delete().getUnsafe
            }

            //LESS
            doTest[Glass]
            doTest[Try]
            //IO
            doTest[IO.ApiIO]
            doTest[IO.ThrowableIO]
        }
      }
    }
  }

  "Async bags" should {
    "use Serial.Thread" when {
      "serial is null" in {
        TestCaseSweeper {
          implicit sweeper =>

            implicit val bag = Bag.future(TestExecutionContext.executionContext)

            implicit val sequencer: Sequencer[Future] = null

            val map = newDB[Future]().await

            val coresSequencer = getSequencer(map)

            coresSequencer shouldBe a[Sequencer.SingleThread[Future]]

            map.put(1, "one").await
            map.get(1).await.value shouldBe "one"
        }
      }
    }
  }

  "toBag" should {
    "convert" when {
      "Synchronised to" when {
        "Synchronised" in {
          TestCaseSweeper {
            implicit sweeper =>
              implicit val sequencer: Sequencer[Glass] = null
              val map = newDB[Glass]()
              getSequencer(map) shouldBe a[Sequencer.Synchronised[Glass]]

              val ioMap = map.toBag[IO.ApiIO]
              getSequencer(ioMap) shouldBe a[Sequencer.Synchronised[IO.ApiIO]]

              ioMap.put(1, "one").getUnsafe
              ioMap.get(1).getUnsafe.value shouldBe "one"
          }
        }

        "SingleThread" in {
          TestCaseSweeper {
            implicit sweeper =>
              implicit val sequencer: Sequencer[Glass] = null
              val map = newDB[Glass]()
              getSequencer(map) shouldBe a[Sequencer.Synchronised[Glass]]

              implicit val bag = Bag.future(TestExecutionContext.executionContext)
              val futureMap = map.toBag[Future]
              getSequencer(futureMap) shouldBe a[Sequencer.SingleThread[IO.ApiIO]]

              futureMap.put(1, "one").await
              futureMap.get(1).await.value shouldBe "one"
          }
        }
      }

      "SingleThreaded to" when {
        "Synchronised" in {
          TestCaseSweeper {
            implicit sweeper =>

              implicit val bag = Bag.future(TestExecutionContext.executionContext)
              implicit val sequencer: Sequencer[Future] = Sequencer.singleThread

              val map = newDB[Future]().await
              getSequencer(map) shouldBe a[Sequencer.SingleThread[Glass]]

              val lessMap = map.toBag[Glass]
              getSequencer(lessMap) shouldBe a[Sequencer.Synchronised[Glass]]

              lessMap.put(1, "one")
              lessMap.get(1).value shouldBe "one"
          }
        }
      }
    }
  }
}
