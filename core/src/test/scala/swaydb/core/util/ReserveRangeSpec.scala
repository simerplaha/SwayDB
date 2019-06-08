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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.util

import swaydb.core.RunThis._
import swaydb.core.{TestBase, TestTimer}
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class ReserveRangeSpec extends TestBase {

  implicit val ordering = swaydb.data.order.KeyOrder.default
  implicit val timer = TestTimer.Empty

  "ReserveRange" should {
    "reserve a range with toKey inclusive and not allow overwrite until freed" in {
      implicit val state = ReserveRange.create[String]()
      ReserveRange.reserveOrGet(1, 10, true, "first") shouldBe empty

      ReserveRange.get(1, 10) should contain("first")
      ReserveRange.reserveOrGet(1, 10, true, "does not register") should contain("first")
      ReserveRange.reserveOrGet(1, 10, false, "does not register") should contain("first")

      ReserveRange.reserveOrGet(0, 1, true, "does not register") should contain("first")
      (1 to 10) foreach {
        i =>
          //check all overlapping keys are not allowed (0, 1)
          ReserveRange.reserveOrGet(i, i + 1, true, "does not register") should contain("first")
          ReserveRange.reserveOrGet(i, i + 1, false, "does not register") should contain("first")
      }

      ReserveRange.reserveOrGet(0, 1, false, "register") shouldBe empty
    }

    "reserve a range with toKey exclusive and not allow overwrite until freed" in {
      implicit val state = ReserveRange.create[String]()
      ReserveRange.reserveOrGet(1, 10, false, "first") shouldBe empty

      ReserveRange.get(1, 10) should contain("first")
      ReserveRange.reserveOrGet(1, 10, true, "does not register") should contain("first")
      ReserveRange.reserveOrGet(1, 10, false, "does not register") should contain("first")

      (1 to 9) foreach {
        i =>
          //check all overlapping keys are not allowed (0, 1)
          ReserveRange.reserveOrGet(i, i + 1, true, "does not register") should contain("first")
      }
      ReserveRange.reserveOrGet(0, 1, false, "register me") shouldBe empty
      ReserveRange.reserveOrGet(10, 11, Random.nextBoolean(), "register") shouldBe empty
    }

    "complete futures when freed" in {
      implicit val state = ReserveRange.create[String]()
      ReserveRange.reserveOrGet(1, 10, Random.nextBoolean(), "first") shouldBe empty

      val futures =
        (0 to 10) map {
          i =>
            ReserveRange.reserveOrListen(1, 10, Random.nextBoolean(), "does not register").left.get map {
              _ =>
                i
            }
        }

      Future {
        sleep(Random.nextInt(1000).millisecond)
        ReserveRange.free(1)
      }

      Future.sequence(futures).await shouldBe (0 to 10)
      state.ranges shouldBe empty
    }

    "return unreserved if empty" in {
      implicit val state = ReserveRange.create[String]()

      (0 to 100) foreach {
        i =>
          ReserveRange.isUnreserved(i, i + 1, true) shouldBe true
          ReserveRange.isUnreserved(i, i + 1, false) shouldBe true
      }
    }

    "return reserved and unreserved" when {
      "to is inclusive" in {
        implicit val state = ReserveRange.create[String]()
        ReserveRange.reserveOrGet(10, 20, true, "first") shouldBe empty

        (0 to 8) foreach {
          i =>
            ReserveRange.isUnreserved(i, i + 1, true) shouldBe true
            ReserveRange.isUnreserved(i, i + 1, false) shouldBe true
        }

        ReserveRange.isUnreserved(9, 10, true) shouldBe false
        ReserveRange.isUnreserved(9, 10, false) shouldBe true

        (10 to 20) foreach {
          i =>
            ReserveRange.isUnreserved(i, i + 1, true) shouldBe false
        }

        (21 to 30) foreach {
          i =>
            ReserveRange.isUnreserved(i, i + 1, true) shouldBe true
        }
      }

      "to is exclusive" in {
        implicit val state = ReserveRange.create[String]()
        ReserveRange.reserveOrGet(10, 20, false, "first") shouldBe empty

        (0 to 8) foreach {
          i =>
            ReserveRange.isUnreserved(i, i + 1, true) shouldBe true
            ReserveRange.isUnreserved(i, i + 1, false) shouldBe true
        }

        ReserveRange.isUnreserved(9, 10, true) shouldBe false
        ReserveRange.isUnreserved(9, 10, false) shouldBe true

        (10 to 19) foreach {
          i =>
            ReserveRange.isUnreserved(i, i + 1, true) shouldBe false
        }

        (20 to 30) foreach {
          i =>
            ReserveRange.isUnreserved(i, i + 1, true) shouldBe true
        }
      }
    }
  }
}
