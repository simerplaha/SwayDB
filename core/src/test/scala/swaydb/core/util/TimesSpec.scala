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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.util.Times._
import swaydb.data.slice.Slice

import scala.concurrent.duration._

class TimesSpec extends AnyWordSpec with Matchers {

  "toNanos" should {
    "convert deadline to nanos" in {
      val duration = 10.seconds
      val deadline = Deadline(duration)
      deadline.toNanos shouldBe duration.toNanos

      Some(deadline).toNanos shouldBe duration.toNanos
    }

    "convert none deadline to 0" in {
      Option.empty[Deadline].toNanos shouldBe 0L
    }
  }

  "toBytes" should {
    "convert deadline long bytes" in {
      val duration = 10.seconds
      val deadline = Deadline(duration)
      deadline.toUnsignedBytes shouldBe Slice.writeUnsignedLong[Byte](duration.toNanos)
      deadline.toBytes shouldBe Slice.writeLong[Byte](duration.toNanos)
    }
  }

  "toDeadline" should {
    "convert long to deadline" in {
      val duration = 10.seconds
      duration.toNanos.toDeadlineOption should contain(Deadline(duration))
    }

    "convert 0 to None" in {
      0L.toDeadlineOption shouldBe empty
    }
  }

  "earlier" should {
    "return the earliest deadline" in {
      val deadline1 = 10.seconds.fromNow
      val deadline2 = 20.seconds.fromNow

      deadline1.earlier(Some(deadline2)) shouldBe deadline1
      deadline2.earlier(Some(deadline1)) shouldBe deadline1

      deadline1.earlier(None) shouldBe deadline1
      deadline2.earlier(None) shouldBe deadline2
    }
  }
}
