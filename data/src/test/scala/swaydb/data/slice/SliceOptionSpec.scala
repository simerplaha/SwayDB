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

package swaydb.data.slice

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.util.Random

class SliceOptionSpec extends AnyWordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default

  def randomByte() = (Random.nextInt(256) - 128).toByte
  val slice: Slice[Int] = Slice(1, 2, 3)

  "getOrElse" in {
    (Slice.Null getOrElseC slice) shouldBe slice
  }

  "orElse" in {
    (Slice.Null orElseC slice) shouldBe slice
    Slice.Null.orElseC(slice: SliceOption[Int]) shouldBe slice

    (Slice.Null orElseC Slice.Null) shouldBe Slice.Null
  }

  "map" in {
    (Slice.Null mapC (_ => slice)) shouldBe None
    (slice mapC (_ => Slice.Null)) shouldBe Some(Slice.Null)
    (slice mapC (_ => Slice(10))) shouldBe Some(Slice(10))

  }

  "flatMap" in {
    (Slice.Null flatMapC (_ => slice)) shouldBe Slice.Null
    (slice flatMapC (_ => Slice.Null)) shouldBe Slice.Null
    (slice flatMapC (_ => Slice(10))) shouldBe Slice(10)
  }
}
