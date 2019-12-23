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

package swaydb.data.slice

import org.scalatest.{Matchers, WordSpec}
import swaydb.data.order.KeyOrder

import scala.util.Random

class SliceOptionalSpec extends WordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default

  def randomByte() = (Random.nextInt(256) - 128).toByte
  val slice: Slice[Int] = Slice(1, 2, 3)

  "getOrElse" in {
    (Slice.Null getOrElseSON slice) shouldBe slice
  }

  "orElse" in {
    (Slice.Null orElseSON slice) shouldBe slice
    Slice.Null.orElseSON(slice: SliceOptional[Int]) shouldBe slice

    (Slice.Null orElseSON Slice.Null) shouldBe Slice.Null
  }

  "map" in {
    (Slice.Null mapSON (_ => slice)) shouldBe None
    (slice mapSON (_ => Slice.Null)) shouldBe Some(Slice.Null)
    (slice mapSON (_ => Slice(10))) shouldBe Some(Slice(10))

  }

  "flatMap" in {
    (Slice.Null flatMapSON (_ => slice)) shouldBe Slice.Null
    (slice flatMapSON (_ => Slice.Null)) shouldBe Slice.Null
    (slice flatMapSON (_ => Slice(10))) shouldBe Slice(10)
  }
}
