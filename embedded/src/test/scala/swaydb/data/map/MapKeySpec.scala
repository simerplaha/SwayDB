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

    "write & read MapKeys with Int key" in {
      doAssert(MapKey.Start(Seq(1)))
      doAssert(MapKey.EntriesStart(Seq(1)))
      doAssert(MapKey.Entry(Seq(1), 100))
      doAssert(MapKey.EntriesEnd(Seq(1)))
      doAssert(MapKey.SubMapsStart(Seq(1)))
      doAssert(MapKey.SubMap(Seq(1), 1000))
      doAssert(MapKey.SubMapsEnd(Seq(1)))
      doAssert(MapKey.End(Seq(1)))
    }

    "write & read MapKeys with multiple Int keys" in {
      doAssert(MapKey.Start(Seq(1, 2, 3)))
      doAssert(MapKey.EntriesStart(Seq(1, 2, 3)))
      doAssert(MapKey.Entry(Seq(1, 2, 3), 100))
      doAssert(MapKey.EntriesEnd(Seq(1, 2, 3)))
      doAssert(MapKey.SubMapsStart(Seq(1, 2, 3)))
      doAssert(MapKey.SubMap(Seq(1, 2, 3), 1000))
      doAssert(MapKey.SubMapsEnd(Seq(1, 2, 3)))
      doAssert(MapKey.End(Seq(1, 2, 3)))
    }

    "write & read MapKeys with Int String" in {
      doAssert(MapKey.Start(Seq("one")))
      doAssert(MapKey.EntriesStart(Seq("one")))
      doAssert(MapKey.Entry(Seq("one"), "one key"))
      doAssert(MapKey.EntriesEnd(Seq("one")))
      doAssert(MapKey.SubMapsStart(Seq("one")))
      doAssert(MapKey.SubMap(Seq("one"), "one sub map"))
      doAssert(MapKey.SubMapsEnd(Seq("one")))
      doAssert(MapKey.End(Seq("one")))
    }

    "write & read MapKeys with large single value" in {
      doAssert(MapKey.Start(Seq(randomCharacters(100000))))
      doAssert(MapKey.EntriesStart(Seq(randomCharacters(100000))))
      doAssert(MapKey.EntriesEnd(Seq(randomCharacters(100000))))
      doAssert(MapKey.Entry(Seq(randomCharacters(100000)), randomCharacters(100000)))
      doAssert(MapKey.SubMapsStart(Seq(randomCharacters(100000))))
      doAssert(MapKey.SubMap(Seq(randomCharacters(100000)), randomCharacters(100000)))
      doAssert(MapKey.SubMapsEnd(Seq(randomCharacters(100000))))
      doAssert(MapKey.End(Seq(randomCharacters(100000))))
    }

    "write & read MapKeys with Double" in {
      doAssert(MapKey.Start(Seq(Double.MinValue)))
      doAssert(MapKey.EntriesStart(Seq(Double.MinValue)))
      doAssert(MapKey.Entry(Seq(Double.MinValue), Double.MaxValue))
      doAssert(MapKey.EntriesEnd(Seq(Double.MinValue)))
      doAssert(MapKey.SubMapsStart(Seq(Double.MinValue)))
      doAssert(MapKey.SubMap(Seq(Double.MinValue), Double.MaxValue))
      doAssert(MapKey.SubMapsEnd(Seq(Double.MinValue)))
      doAssert(MapKey.End(Seq(Double.MinValue)))
    }

  }

  "ordering" should {
    "ordering MapKeys in the order of Start, Entry & End" in {
      val order = Ordering.by[Slice[Byte], Int](_.readInt())(Ordering.Int)
      val mapKeySerializer = MapKey.mapKeySerializer[Int](IntSerializer)
      implicit val mapKeyOrder = Ordering.by[MapKey[Int], Slice[Byte]](mapKeySerializer.write)(MapKey.ordering(order))

      val keys = Seq(
        MapKey.Start(Seq(0)),
        MapKey.SubMapsStart(Seq(0)),
        MapKey.SubMapsEnd(Seq(0)),
        MapKey.End(Seq(0)),

        MapKey.Start(Seq(1)),
        MapKey.EntriesStart(Seq(1)),
        MapKey.Entry(Seq(1), 1),
        MapKey.EntriesEnd(Seq(1)),
        MapKey.SubMapsStart(Seq(1)),
        MapKey.SubMap(Seq(1), 1000),
        MapKey.SubMapsEnd(Seq(1)),
        MapKey.End(Seq(1)),

        MapKey.Start(Seq(100)),
        MapKey.EntriesStart(Seq(100)),
        MapKey.Entry(Seq(100), 2),
        MapKey.Entry(Seq(100), 3),
        MapKey.Entry(Seq(100), 4),
        MapKey.Entry(Seq(100), 5),
        MapKey.EntriesEnd(Seq(100)),
        MapKey.SubMapsStart(Seq(100)),
        MapKey.SubMap(Seq(100), 1000),
        MapKey.SubMap(Seq(100), 2000),
        MapKey.SubMap(Seq(100), 3000),
        MapKey.SubMapsEnd(Seq(100)),
        MapKey.End(Seq(100)),

        MapKey.Start(Seq(2, 3)),
        MapKey.EntriesStart(Seq(2, 3)),
        MapKey.Entry(Seq(2, 3), 2),
        MapKey.Entry(Seq(2, 3), 3),
        MapKey.EntriesEnd(Seq(2, 3)),
        MapKey.SubMapsStart(Seq(2, 3)),
        MapKey.SubMap(Seq(2, 3), 1000),
        MapKey.SubMap(Seq(2, 3), 2000),
        MapKey.SubMapsEnd(Seq(2, 3)),
        MapKey.End(Seq(2, 3))
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
        MapKey.Start(Seq(randomString1)),
        MapKey.SubMapsStart(Seq(randomString1)),
        MapKey.SubMapsEnd(Seq(randomString1)),
        MapKey.End(Seq(randomString1)),

        MapKey.Start(Seq(randomString2)),
        MapKey.EntriesStart(Seq(randomString2)),
        MapKey.Entry(Seq(randomString2), randomString3),
        MapKey.Entry(Seq(randomString2), randomString4),
        MapKey.Entry(Seq(randomString2), randomString5),
        MapKey.EntriesEnd(Seq(randomString2)),
        MapKey.SubMapsStart(Seq(randomString2)),
        MapKey.SubMap(Seq(randomString2), randomString3),
        MapKey.SubMap(Seq(randomString2), randomString4),
        MapKey.SubMap(Seq(randomString2), randomString5),
        MapKey.SubMapsEnd(Seq(randomString2)),
        MapKey.End(Seq(randomString2)),

        MapKey.Start(Seq(randomString3)),
        MapKey.Entry(Seq(randomString3), randomString3),
        MapKey.Entry(Seq(randomString3), randomString4),
        MapKey.Entry(Seq(randomString3), randomString5),
        MapKey.End(Seq(randomString3))
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
        MapKey.Start(Seq(0)),
        MapKey.EntriesStart(Seq(0)),
        MapKey.EntriesEnd(Seq(0)),
        MapKey.End(Seq(0)),
        MapKey.Start(Seq(0)),
        MapKey.EntriesStart(Seq(0)),
        MapKey.EntriesEnd(Seq(0)),
        MapKey.End(Seq(0)),

        MapKey.Start(Seq(2)),
        MapKey.EntriesStart(Seq(2)),
        MapKey.Entry(Seq(2), 2),
        MapKey.Entry(Seq(2), 2),
        MapKey.EntriesEnd(Seq(2)),
        MapKey.SubMapsStart(Seq(2)),
        MapKey.SubMap(Seq(2), 1000),
        MapKey.SubMap(Seq(2), 1000),
        MapKey.SubMapsEnd(Seq(2)),
        MapKey.End(Seq(2)),

        MapKey.Start(Seq(100)),
        MapKey.Entry(Seq(100), 4),
        MapKey.Entry(Seq(100), 5),
        MapKey.End(Seq(100))
      )

      //shuffle and create a list
      val map = SortedSet[MapKey[Int]](Random.shuffle(keys): _*)(mapKeyOrder)

      val expected = Seq(
        MapKey.Start(Seq(0)),
        MapKey.EntriesStart(Seq(0)),
        MapKey.EntriesEnd(Seq(0)),
        MapKey.End(Seq(0)),

        MapKey.Start(Seq(2)),
        MapKey.EntriesStart(Seq(2)),
        MapKey.Entry(Seq(2), 2),
        MapKey.EntriesEnd(Seq(2)),
        MapKey.SubMapsStart(Seq(2)),
        MapKey.SubMap(Seq(2), 1000),
        MapKey.SubMapsEnd(Seq(2)),
        MapKey.End(Seq(2)),

        MapKey.Start(Seq(100)),
        MapKey.Entry(Seq(100), 4),
        MapKey.Entry(Seq(100), 5),
        MapKey.End(Seq(100))
      )

      //key-values should
      map.toList shouldBe expected
    }

  }
}
