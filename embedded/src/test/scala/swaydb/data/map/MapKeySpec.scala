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

import swaydb.TestBaseEmbedded
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers.Serializer

import scala.collection.SortedSet
import scala.util.Random

class MapKeySpec extends TestBaseEmbedded {

  override val keyValueCount: Int = 100

  "mapKeySerializer" should {
    def doAssert[T](key: MapKey[T])(implicit serializer: Serializer[T]) = {
      val mapKeySerializer = MapKey.mapKeySerializer[T](serializer)
      val wrote = mapKeySerializer.write(key)
      val read = mapKeySerializer.read(wrote)
      read shouldBe key
    }

    "write & read MapKeys with Int keys" in {
      doAssert(MapKey.Start(1))
      doAssert(MapKey.EntriesStart(1))
      doAssert(MapKey.Entry(1, 100))
      doAssert(MapKey.EntriesEnd(1))
      doAssert(MapKey.SubMapsStart(1))
      doAssert(MapKey.SubMap(1, 1000))
      doAssert(MapKey.SubMapsEnd(1))
      doAssert(MapKey.End(1))
    }

    "write & read MapKeys with Int String" in {
      doAssert(MapKey.Start("one"))
      doAssert(MapKey.EntriesStart("one"))
      doAssert(MapKey.Entry("one", "one key"))
      doAssert(MapKey.EntriesEnd("one"))
      doAssert(MapKey.SubMapsStart("one"))
      doAssert(MapKey.SubMap("one", "one sub map"))
      doAssert(MapKey.SubMapsEnd("one"))
      doAssert(MapKey.End("three"))
    }

    "write & read MapKeys with large single value" in {
      doAssert(MapKey.Start(randomCharacters(100000)))
      doAssert(MapKey.EntriesStart(randomCharacters(100000)))
      doAssert(MapKey.EntriesEnd(randomCharacters(100000)))
      doAssert(MapKey.Entry(randomCharacters(100000), randomCharacters(100000)))
      doAssert(MapKey.SubMapsStart(randomCharacters(100000)))
      doAssert(MapKey.SubMap(randomCharacters(100000), randomCharacters(100000)))
      doAssert(MapKey.SubMapsEnd(randomCharacters(100000)))
      doAssert(MapKey.End(randomCharacters(100000)))
    }

    "write & read MapKeys with Double" in {
      doAssert(MapKey.Start(Double.MinValue))
      doAssert(MapKey.EntriesStart(Double.MinValue))
      doAssert(MapKey.Entry(Double.MinValue, Double.MaxValue))
      doAssert(MapKey.EntriesEnd(Double.MinValue))
      doAssert(MapKey.SubMapsStart(Double.MinValue))
      doAssert(MapKey.SubMap(Double.MaxValue, Double.MaxValue))
      doAssert(MapKey.SubMapsEnd(Double.MinValue))
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
        MapKey.SubMapsStart(0),
        MapKey.SubMapsEnd(0),
        MapKey.End(0),

        MapKey.Start(1),
        MapKey.EntriesStart(1),
        MapKey.Entry(1, 1),
        MapKey.EntriesEnd(1),
        MapKey.SubMapsStart(1),
        MapKey.SubMap(1, 1000),
        MapKey.SubMapsEnd(1),
        MapKey.End(1),

        MapKey.Start(2),
        MapKey.EntriesStart(2),
        MapKey.Entry(2, 2),
        MapKey.Entry(2, 3),
        MapKey.EntriesEnd(2),
        MapKey.SubMapsStart(2),
        MapKey.SubMap(2, 1000),
        MapKey.SubMap(2, 2000),
        MapKey.SubMapsEnd(2),
        MapKey.End(2),

        MapKey.Start(100),
        MapKey.EntriesStart(100),
        MapKey.Entry(100, 2),
        MapKey.Entry(100, 3),
        MapKey.Entry(100, 4),
        MapKey.Entry(100, 5),
        MapKey.EntriesEnd(100),
        MapKey.SubMapsStart(100),
        MapKey.SubMap(100, 1000),
        MapKey.SubMap(100, 2000),
        MapKey.SubMap(100, 3000),
        MapKey.SubMapsEnd(100),
        MapKey.End(100)
      )

      //shuffle and create a list
      val map = SortedSet[MapKey[Int]](Random.shuffle(keys): _*)(mapKeyOrder)

      //key-values should
      map.toList shouldBe keys
    }

    "ordering MapKeys in the order of Start, Entry & End when keys are large String" in {
      val order = Ordering.by[Slice[Byte], String](_.readString())(Ordering.String)
      val mapKeySerializer = MapKey.mapKeySerializer[String](StringSerializer)
      implicit val mapKeyOrder = Ordering.by[MapKey[String], Slice[Byte]](mapKeySerializer.write)(MapKey.ordering(order))

      val stringLength = 100000

      val randomString1 = "a" + randomCharacters(stringLength)
      val randomString2 = "b" + randomCharacters(stringLength)
      val randomString3 = "c" + randomCharacters(stringLength)
      val randomString4 = "d" + randomCharacters(stringLength)
      val randomString5 = "e" + randomCharacters(stringLength)

      val keys = Seq(
        MapKey.Start(randomString1),
        MapKey.SubMapsStart(randomString1),
        MapKey.SubMapsEnd(randomString1),
        MapKey.End(randomString1),

        MapKey.Start(randomString2),
        MapKey.EntriesStart(randomString2),
        MapKey.Entry(randomString2, randomString3),
        MapKey.Entry(randomString2, randomString4),
        MapKey.Entry(randomString2, randomString5),
        MapKey.EntriesEnd(randomString2),
        MapKey.SubMapsStart(randomString2),
        MapKey.SubMap(randomString2, randomString3),
        MapKey.SubMap(randomString2, randomString4),
        MapKey.SubMap(randomString2, randomString5),
        MapKey.SubMapsEnd(randomString2),
        MapKey.End(randomString2),

        MapKey.Start(randomString3),
        MapKey.Entry(randomString3, randomString3),
        MapKey.Entry(randomString3, randomString4),
        MapKey.Entry(randomString3, randomString5),
        MapKey.End(randomString3)
      )

      //shuffle and create a list
      val map = SortedSet[MapKey[String]](Random.shuffle(keys): _*)(mapKeyOrder)

      //key-values should
      map.toList shouldBe keys
    }

    "remove duplicate key-values" in {
      val order = Ordering.by[Slice[Byte], Int](_.readInt())(Ordering.Int)
      val mapKeySerializer = MapKey.mapKeySerializer[Int](IntSerializer)
      implicit val mapKeyOrder = Ordering.by[MapKey[Int], Slice[Byte]](mapKeySerializer.write)(MapKey.ordering(order))

      val keys = Seq(
        MapKey.Start(0),
        MapKey.EntriesStart(0),
        MapKey.EntriesEnd(0),
        MapKey.End(0),
        MapKey.Start(0),
        MapKey.EntriesStart(0),
        MapKey.EntriesEnd(0),
        MapKey.End(0),

        MapKey.Start(2),
        MapKey.EntriesStart(2),
        MapKey.Entry(2, 2),
        MapKey.Entry(2, 2),
        MapKey.EntriesEnd(2),
        MapKey.SubMapsStart(2),
        MapKey.SubMap(2, 1000),
        MapKey.SubMap(2, 1000),
        MapKey.SubMapsEnd(2),
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
        MapKey.EntriesStart(0),
        MapKey.EntriesEnd(0),
        MapKey.End(0),

        MapKey.Start(2),
        MapKey.EntriesStart(2),
        MapKey.Entry(2, 2),
        MapKey.EntriesEnd(2),
        MapKey.SubMapsStart(2),
        MapKey.SubMap(2, 1000),
        MapKey.SubMapsEnd(2),
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
