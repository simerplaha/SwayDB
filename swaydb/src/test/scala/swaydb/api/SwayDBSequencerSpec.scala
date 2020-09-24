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
import swaydb.Bag.Implicits._
import swaydb._
import swaydb.core.TestCaseSweeper._
import swaydb.core.{TestCaseSweeper, TestExecutionContext}
import swaydb.data.RunThis.FutureImplicits
import swaydb.data.sequencer.Sequencer
import swaydb.serializers.Default._

import scala.concurrent.Future
import scala.util.Try

class SwayDBSequencerSpec0 extends SwayDBSequencerSpec {
  def newDB[BAG[+_]]()(implicit sweeper: TestCaseSweeper,
                       serial: Sequencer[BAG],
                       bag: Bag[BAG]): BAG[Map[Int, String, Nothing, BAG]] =
    swaydb.persistent.Map[Int, String, Nothing, BAG](randomDir).map(_.sweep(_.toBag[Bag.Less].delete()))

  override val keyValueCount: Int = 100
}

class SwayDBSequencerSpec3 extends SwayDBSequencerSpec {

  override val keyValueCount: Int = 100

  def newDB[BAG[+_]]()(implicit sweeper: TestCaseSweeper,
                       serial: Sequencer[BAG],
                       bag: Bag[BAG]): BAG[Map[Int, String, Nothing, BAG]] =
    swaydb.memory.Map[Int, String, Nothing, BAG]().map(_.sweep(_.toBag[Bag.Less].delete()))
}

sealed trait SwayDBSequencerSpec extends TestBaseEmbedded {

  def newDB[BAG[+_]]()(implicit sweeper: TestCaseSweeper,
                       serial: Sequencer[BAG],
                       bag: Bag[BAG]): BAG[SetMapT[Int, String, BAG]]

  "Synchronised bags" should {
    "used Serial.Synchronised" when {
      "serial is null" in {
        TestCaseSweeper {
          implicit sweeper =>

            def doTest[BAG[+_]](implicit bag: Bag.Sync[BAG]) = {
              implicit val serial: Sequencer[BAG] = null

              val map = newDB[BAG]().getUnsafe

              val coresSerial = getSerial(map)

              coresSerial shouldBe a[Sequencer.Synchronised[BAG]]

              map.put(1, "one")
              map.get(1).getUnsafe.value shouldBe "one"

              map.delete().getUnsafe
            }

            //LESS
            doTest[Bag.Less]
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

            implicit val serial: Sequencer[Future] = null

            val map = newDB[Future]().await

            val coresSerial = getSerial(map)

            coresSerial shouldBe a[Sequencer.SingleThread[Future]]

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
              implicit val serial: Sequencer[Bag.Less] = null
              val map = newDB[Bag.Less]()
              getSerial(map) shouldBe a[Sequencer.Synchronised[Bag.Less]]

              val ioMap = map.toBag[IO.ApiIO]
              getSerial(ioMap) shouldBe a[Sequencer.Synchronised[IO.ApiIO]]

              ioMap.put(1, "one").getUnsafe
              ioMap.get(1).getUnsafe.value shouldBe "one"
          }
        }

        "SingleThread" in {
          TestCaseSweeper {
            implicit sweeper =>
              implicit val serial: Sequencer[Bag.Less] = null
              val map = newDB[Bag.Less]()
              getSerial(map) shouldBe a[Sequencer.Synchronised[Bag.Less]]

              implicit val bag = Bag.future(TestExecutionContext.executionContext)
              val futureMap = map.toBag[Future]
              getSerial(futureMap) shouldBe a[Sequencer.SingleThread[IO.ApiIO]]

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
              implicit val serial: Sequencer[Future] = Sequencer.singleThread

              val map = newDB[Future]().await
              getSerial(map) shouldBe a[Sequencer.SingleThread[Bag.Less]]

              val lessMap = map.toBag[Bag.Less]
              getSerial(lessMap) shouldBe a[Sequencer.Synchronised[Bag.Less]]

              lessMap.put(1, "one")
              lessMap.get(1).value shouldBe "one"
          }
        }
      }
    }
  }

  "Actor bag" should {
    "convert to sync" in {
      TestCaseSweeper {
        implicit sweeper =>

          implicit val bag = Bag.future(TestExecutionContext.executionContext)
          implicit val actorSerial: Sequencer[Future] = Sequencer.actor

          val map = newDB[Future]().await
          getSerial(map) shouldBe a[Sequencer.Actor[Bag.Less]]

          val lessMap = map.toBag[Bag.Less]
          getSerial(lessMap) shouldBe a[Sequencer.Synchronised[Bag.Less]]

          lessMap.put(1, "one")
          lessMap.get(1).value shouldBe "one"
      }
    }

    "use the same actor when converted to another Async bag" in {
      TestCaseSweeper {
        implicit sweeper =>

          implicit val bag = Bag.future(TestExecutionContext.executionContext)
          implicit val actorSerial: Sequencer.Actor[Future] = Sequencer.actor

          val map = newDB[Future]().await
          getSerial(map) shouldBe a[Sequencer.Actor[Future]]

          val anotherFutureMap = map.toBag[Future]
          val anotherFutureSerial = getSerial(anotherFutureMap)
          anotherFutureSerial shouldBe a[Sequencer.Actor[Future]]

          actorSerial.actor shouldBe anotherFutureSerial.asInstanceOf[Sequencer.Actor[Future]].actor

          anotherFutureMap.put(1, "one").await
          anotherFutureMap.get(1).await.value shouldBe "one"
      }
    }
  }

}
