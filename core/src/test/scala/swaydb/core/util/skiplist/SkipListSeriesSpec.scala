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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
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
