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

package swaydb.core.util.skiplist

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.util.Random

class Length1_SkipListSeriesSpec extends SkipListSeriesSpec {
  override def create[NK, NV, K <: NK, V <: NV](nullKey: NK, nullValue: NV)(implicit keyOrder: KeyOrder[K]): SkipListSeries[NK, NV, K, V] =
    SkipListSeries[NK, NV, K, V](lengthPerSeries = 1, nullKey = nullKey, nullValue = nullValue)
}

class Length10_SkipListSeriesSpec extends SkipListSeriesSpec {
  override def create[NK, NV, K <: NK, V <: NV](nullKey: NK, nullValue: NV)(implicit keyOrder: KeyOrder[K]): SkipListSeries[NK, NV, K, V] =
    SkipListSeries[NK, NV, K, V](lengthPerSeries = 10, nullKey = nullKey, nullValue = nullValue)
}

sealed trait SkipListSeriesSpec extends AnyWordSpec with Matchers {

  sealed trait ValueOption
  object Value {
    final case object Null extends ValueOption
    case class Some(value: Int) extends ValueOption
  }

  implicit val ordering = KeyOrder.integer

  def create[NK, NV, K <: NK, V <: NV](nullKey: NK, nullValue: NV)(implicit keyOrder: KeyOrder[K]): SkipListSeries[NK, NV, K, V]

  def create(): SkipListSeries[SliceOption[Byte], ValueOption, Slice[Byte], Value.Some] =
    create[SliceOption[Byte], ValueOption, Slice[Byte], Value.Some](Slice.Null, Value.Null)

  "maintain index" in {
    val skipList = create()
    val random = Random.shuffle(List.range(0, 1000))
    random foreach {
      int =>
        skipList.put(int, Value.Some(int))
    }

    skipList.series.foreach(0) {
      keyValue =>
        keyValue.index shouldBe keyValue.key.readInt()
    }
  }
}
