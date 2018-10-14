/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

package swaydb.data.map

import org.scalatest.{Matchers, WordSpec}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers.Serializer

import scala.collection.SortedSet
import scala.util.Random

class MapKeySpec extends WordSpec with Matchers {

  "mapKeySerializer" should {
    def doAssert[T](key: MapKey[T])(implicit serializer: Serializer[T]) = {
      val mapKeySerializer = MapKey.mapKeySerializer[T](serializer)
      val wrote = mapKeySerializer.write(key)
      val read = mapKeySerializer.read(wrote)
      read shouldBe key
    }

    "write & read MapKeys with Int keys" in {
      doAssert(MapKey.Start(1))
      doAssert(MapKey.Entry(1, 100))
      doAssert(MapKey.End(1))
    }

    "write & read MapKeys with Int String" in {
      doAssert(MapKey.Start("one"))
      doAssert(MapKey.Entry("one", "one key"))
      doAssert(MapKey.End("three"))
    }

    "write & read MapKeys with Int Double" in {
      doAssert(MapKey.Start(Double.MinValue))
      doAssert(MapKey.Entry(0.11, 1001.0))
      doAssert(MapKey.End(Double.MaxValue))
    }
  }

  "ordering" should {
    "ordering MapKeys in the order of Start, Entry & End" in {
      val order = Ordering.by[Slice[Byte], Int](_.readInt())(Ordering.Int)
      val mapKeySerializer = MapKey.mapKeySerializer[Int](IntSerializer)
      implicit val mapKeyOrder = Ordering.by[MapKey[Int], Slice[Byte]](mapKeySerializer.write)(MapKey.ordering(order))

      val keys = Seq(
        MapKey.Start(0),
        MapKey.End(0),
        MapKey.Start(1),
        MapKey.Entry(1, 1),
        MapKey.End(1),
        MapKey.Start(2),
        MapKey.Entry(2, 2),
        MapKey.Entry(2, 3),
        MapKey.End(2),
        MapKey.Start(100),
        MapKey.Entry(100, 2),
        MapKey.Entry(100, 3),
        MapKey.Entry(100, 4),
        MapKey.Entry(100, 5),
        MapKey.End(100)
      )

      //shuffle and create a list
      val map = SortedSet[MapKey[Int]](Random.shuffle(keys): _*)(mapKeyOrder)

      //key-values should
      map.toList shouldBe keys
    }

    "remove duplicate key-values" in {
      val order = Ordering.by[Slice[Byte], Int](_.readInt())(Ordering.Int)
      val mapKeySerializer = MapKey.mapKeySerializer[Int](IntSerializer)
      implicit val mapKeyOrder = Ordering.by[MapKey[Int], Slice[Byte]](mapKeySerializer.write)(MapKey.ordering(order))

      val keys = Seq(
        MapKey.Start(0),
        MapKey.End(0),
        MapKey.Start(0),
        MapKey.End(0),
        MapKey.End(1),
        MapKey.Start(2),
        MapKey.Entry(2, 2),
        MapKey.Entry(2, 2),
        MapKey.End(2),
        MapKey.Start(100),
        MapKey.Entry(100, 4),
        MapKey.Entry(100, 5),
        MapKey.End(100)
      )

      //shuffle and create a list
      val map = SortedSet[MapKey[Int]](Random.shuffle(keys): _*)(mapKeyOrder)

      val expected = Seq(
        MapKey.Start(0),
        MapKey.End(0),
        MapKey.End(1),
        MapKey.Start(2),
        MapKey.Entry(2, 2),
        MapKey.End(2),
        MapKey.Start(100),
        MapKey.Entry(100, 4),
        MapKey.Entry(100, 5),
        MapKey.End(100)
      )

      //key-values should
      map.toList shouldBe expected
    }

  }

}
