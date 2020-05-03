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
 */

package swaydb.multimap

import org.scalatest.{Matchers, WordSpec}
import swaydb.MultiMapKey
import swaydb.core.TestData._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers.Serializer

import scala.collection.SortedSet
import scala.util.Random

class MultiMapKeySpec extends WordSpec with Matchers {

  "mapKeySerializer" should {
    def doAssert[T](key: MultiMapKey[T])(implicit serializer: Serializer[T]) = {
      val mapKeySerializer = MultiMapKey.serializer[T](serializer)
      val wrote = mapKeySerializer.write(key)
      val read = mapKeySerializer.read(wrote)
      read shouldBe key
    }

    "write & read empty keys" in {
      doAssert(MultiMapKey.MapStart(Seq.empty[Int]))
      doAssert(MultiMapKey.MapEntriesStart(Seq.empty[Int]))
      doAssert(MultiMapKey.MapEntry(Seq.empty[Int], 100))
      doAssert(MultiMapKey.MapEntriesEnd(Seq.empty[Int]))
      doAssert(MultiMapKey.SubMapsStart(Seq.empty[Int]))
      doAssert(MultiMapKey.SubMap(Seq.empty[Int], 1000))
      doAssert(MultiMapKey.SubMapsEnd(Seq.empty[Int]))
      doAssert(MultiMapKey.MapEnd(Seq.empty[Int]))
    }

    "write & read MapKeys with Int key" in {
      doAssert(MultiMapKey.MapStart(Seq(1)))
      doAssert(MultiMapKey.MapEntriesStart(Seq(1)))
      doAssert(MultiMapKey.MapEntry(Seq(1), 100))
      doAssert(MultiMapKey.MapEntriesEnd(Seq(1)))
      doAssert(MultiMapKey.SubMapsStart(Seq(1)))
      doAssert(MultiMapKey.SubMap(Seq(1), 1000))
      doAssert(MultiMapKey.SubMapsEnd(Seq(1)))
      doAssert(MultiMapKey.MapEnd(Seq(1)))
    }

    "write & read MapKeys with multiple Int keys" in {
      doAssert(MultiMapKey.MapStart(Seq(1, 2, 3)))
      doAssert(MultiMapKey.MapEntriesStart(Seq(1, 2, 3)))
      doAssert(MultiMapKey.MapEntry(Seq(1, 2, 3), 100))
      doAssert(MultiMapKey.MapEntriesEnd(Seq(1, 2, 3)))
      doAssert(MultiMapKey.SubMapsStart(Seq(1, 2, 3)))
      doAssert(MultiMapKey.SubMap(Seq(1, 2, 3), 1000))
      doAssert(MultiMapKey.SubMapsEnd(Seq(1, 2, 3)))
      doAssert(MultiMapKey.MapEnd(Seq(1, 2, 3)))
    }

    "write & read MapKeys with Int String" in {
      doAssert(MultiMapKey.MapStart(Seq("one")))
      doAssert(MultiMapKey.MapEntriesStart(Seq("one")))
      doAssert(MultiMapKey.MapEntry(Seq("one"), "one key"))
      doAssert(MultiMapKey.MapEntriesEnd(Seq("one")))
      doAssert(MultiMapKey.SubMapsStart(Seq("one")))
      doAssert(MultiMapKey.SubMap(Seq("one"), "one sub map"))
      doAssert(MultiMapKey.SubMapsEnd(Seq("one")))
      doAssert(MultiMapKey.MapEnd(Seq("one")))
    }

    "write & read MapKeys with large single value" in {
      doAssert(MultiMapKey.MapStart(Seq(randomCharacters(100000))))
      doAssert(MultiMapKey.MapEntriesStart(Seq(randomCharacters(100000))))
      doAssert(MultiMapKey.MapEntriesEnd(Seq(randomCharacters(100000))))
      doAssert(MultiMapKey.MapEntry(Seq(randomCharacters(100000)), randomCharacters(100000)))
      doAssert(MultiMapKey.SubMapsStart(Seq(randomCharacters(100000))))
      doAssert(MultiMapKey.SubMap(Seq(randomCharacters(100000)), randomCharacters(100000)))
      doAssert(MultiMapKey.SubMapsEnd(Seq(randomCharacters(100000))))
      doAssert(MultiMapKey.MapEnd(Seq(randomCharacters(100000))))
    }

    "write & read MapKeys with Double" in {
      doAssert(MultiMapKey.MapStart(Seq(Double.MinValue)))
      doAssert(MultiMapKey.MapEntriesStart(Seq(Double.MinValue)))
      doAssert(MultiMapKey.MapEntry(Seq(Double.MinValue), Double.MaxValue))
      doAssert(MultiMapKey.MapEntriesEnd(Seq(Double.MinValue)))
      doAssert(MultiMapKey.SubMapsStart(Seq(Double.MinValue)))
      doAssert(MultiMapKey.SubMap(Seq(Double.MinValue), Double.MaxValue))
      doAssert(MultiMapKey.SubMapsEnd(Seq(Double.MinValue)))
      doAssert(MultiMapKey.MapEnd(Seq(Double.MinValue)))
    }
  }

  "ordering" should {
    "ordering MapKeys in the order of Start, Entry & End" in {
      val order = KeyOrder(Ordering.by[Slice[Byte], Int](_.readInt())(Ordering.Int))
      val mapKeySerializer = MultiMapKey.serializer[Int](IntSerializer)
      implicit val mapKeyOrder = Ordering.by[MultiMapKey[Int], Slice[Byte]](mapKeySerializer.write)(MultiMapKey.ordering(order))

      val keys = Seq(
        MultiMapKey.MapStart(Seq.empty[Int]),
        MultiMapKey.SubMapsStart(Seq.empty[Int]),
        MultiMapKey.SubMapsEnd(Seq.empty[Int]),
        MultiMapKey.MapEnd(Seq.empty[Int]),

        MultiMapKey.MapStart(Seq(0)),
        MultiMapKey.SubMapsStart(Seq(0)),
        MultiMapKey.SubMapsEnd(Seq(0)),
        MultiMapKey.MapEnd(Seq(0)),

        MultiMapKey.MapStart(Seq(1)),
        MultiMapKey.MapEntriesStart(Seq(1)),
        MultiMapKey.MapEntry(Seq(1), 1),
        MultiMapKey.MapEntriesEnd(Seq(1)),
        MultiMapKey.SubMapsStart(Seq(1)),
        MultiMapKey.SubMap(Seq(1), 1000),
        MultiMapKey.SubMapsEnd(Seq(1)),
        MultiMapKey.MapEnd(Seq(1)),

        MultiMapKey.MapStart(Seq(100)),
        MultiMapKey.MapEntriesStart(Seq(100)),
        MultiMapKey.MapEntry(Seq(100), 2),
        MultiMapKey.MapEntry(Seq(100), 3),
        MultiMapKey.MapEntry(Seq(100), 4),
        MultiMapKey.MapEntry(Seq(100), 5),
        MultiMapKey.MapEntriesEnd(Seq(100)),
        MultiMapKey.SubMapsStart(Seq(100)),
        MultiMapKey.SubMap(Seq(100), 1000),
        MultiMapKey.SubMap(Seq(100), 2000),
        MultiMapKey.SubMap(Seq(100), 3000),
        MultiMapKey.SubMapsEnd(Seq(100)),
        MultiMapKey.MapEnd(Seq(100)),

        MultiMapKey.MapStart(Seq(2, 3)),
        MultiMapKey.MapEntriesStart(Seq(2, 3)),
        MultiMapKey.MapEntry(Seq(2, 3), 2),
        MultiMapKey.MapEntry(Seq(2, 3), 3),
        MultiMapKey.MapEntriesEnd(Seq(2, 3)),
        MultiMapKey.SubMapsStart(Seq(2, 3)),
        MultiMapKey.SubMap(Seq(2, 3), 1000),
        MultiMapKey.SubMap(Seq(2, 3), 2000),
        MultiMapKey.SubMapsEnd(Seq(2, 3)),
        MultiMapKey.MapEnd(Seq(2, 3))
      )

      //shuffle and create a list
      val map = SortedSet[MultiMapKey[Int]](Random.shuffle(keys): _*)(mapKeyOrder)

      //key-values should
      map.toList shouldBe keys
    }

    "ordering MapKeys in the order of Start, Entry & End when keys are large String" in {
      val order = KeyOrder(Ordering.by[Slice[Byte], String](_.readString())(Ordering.String))
      val mapKeySerializer = MultiMapKey.serializer[String](StringSerializer)
      implicit val mapKeyOrder = Ordering.by[MultiMapKey[String], Slice[Byte]](mapKeySerializer.write)(MultiMapKey.ordering(order))

      val stringLength = 100000

      val randomString1 = "a" + randomCharacters(stringLength)
      val randomString2 = "b" + randomCharacters(stringLength)
      val randomString3 = "c" + randomCharacters(stringLength)
      val randomString4 = "d" + randomCharacters(stringLength)
      val randomString5 = "e" + randomCharacters(stringLength)

      val keys = Seq(
        MultiMapKey.MapStart(Seq(randomString1)),
        MultiMapKey.SubMapsStart(Seq(randomString1)),
        MultiMapKey.SubMapsEnd(Seq(randomString1)),
        MultiMapKey.MapEnd(Seq(randomString1)),

        MultiMapKey.MapStart(Seq(randomString2)),
        MultiMapKey.MapEntriesStart(Seq(randomString2)),
        MultiMapKey.MapEntry(Seq(randomString2), randomString3),
        MultiMapKey.MapEntry(Seq(randomString2), randomString4),
        MultiMapKey.MapEntry(Seq(randomString2), randomString5),
        MultiMapKey.MapEntriesEnd(Seq(randomString2)),
        MultiMapKey.SubMapsStart(Seq(randomString2)),
        MultiMapKey.SubMap(Seq(randomString2), randomString3),
        MultiMapKey.SubMap(Seq(randomString2), randomString4),
        MultiMapKey.SubMap(Seq(randomString2), randomString5),
        MultiMapKey.SubMapsEnd(Seq(randomString2)),
        MultiMapKey.MapEnd(Seq(randomString2)),

        MultiMapKey.MapStart(Seq(randomString3)),
        MultiMapKey.MapEntry(Seq(randomString3), randomString3),
        MultiMapKey.MapEntry(Seq(randomString3), randomString4),
        MultiMapKey.MapEntry(Seq(randomString3), randomString5),
        MultiMapKey.MapEnd(Seq(randomString3))
      )

      //shuffle and create a list
      val map = SortedSet[MultiMapKey[String]](Random.shuffle(keys): _*)(mapKeyOrder)

      //key-values should
      map.toList shouldBe keys
    }

    "remove duplicate key-values" in {
      val order = KeyOrder(Ordering.by[Slice[Byte], Int](_.readInt())(Ordering.Int))
      val mapKeySerializer = MultiMapKey.serializer[Int](IntSerializer)
      implicit val mapKeyOrder = Ordering.by[MultiMapKey[Int], Slice[Byte]](mapKeySerializer.write)(MultiMapKey.ordering(order))

      val keys = Seq(
        MultiMapKey.MapStart(Seq(0)),
        MultiMapKey.MapEntriesStart(Seq(0)),
        MultiMapKey.MapEntriesEnd(Seq(0)),
        MultiMapKey.MapEnd(Seq(0)),
        MultiMapKey.MapStart(Seq(0)),
        MultiMapKey.MapEntriesStart(Seq(0)),
        MultiMapKey.MapEntriesEnd(Seq(0)),
        MultiMapKey.MapEnd(Seq(0)),

        MultiMapKey.MapStart(Seq(2)),
        MultiMapKey.MapEntriesStart(Seq(2)),
        MultiMapKey.MapEntry(Seq(2), 2),
        MultiMapKey.MapEntry(Seq(2), 2),
        MultiMapKey.MapEntriesEnd(Seq(2)),
        MultiMapKey.SubMapsStart(Seq(2)),
        MultiMapKey.SubMap(Seq(2), 1000),
        MultiMapKey.SubMap(Seq(2), 1000),
        MultiMapKey.SubMapsEnd(Seq(2)),
        MultiMapKey.MapEnd(Seq(2)),

        MultiMapKey.MapStart(Seq(100)),
        MultiMapKey.MapEntry(Seq(100), 4),
        MultiMapKey.MapEntry(Seq(100), 5),
        MultiMapKey.MapEnd(Seq(100))
      )

      //shuffle and create a list
      val map = SortedSet[MultiMapKey[Int]](Random.shuffle(keys): _*)(mapKeyOrder)

      val expected = Seq(
        MultiMapKey.MapStart(Seq(0)),
        MultiMapKey.MapEntriesStart(Seq(0)),
        MultiMapKey.MapEntriesEnd(Seq(0)),
        MultiMapKey.MapEnd(Seq(0)),

        MultiMapKey.MapStart(Seq(2)),
        MultiMapKey.MapEntriesStart(Seq(2)),
        MultiMapKey.MapEntry(Seq(2), 2),
        MultiMapKey.MapEntriesEnd(Seq(2)),
        MultiMapKey.SubMapsStart(Seq(2)),
        MultiMapKey.SubMap(Seq(2), 1000),
        MultiMapKey.SubMapsEnd(Seq(2)),
        MultiMapKey.MapEnd(Seq(2)),

        MultiMapKey.MapStart(Seq(100)),
        MultiMapKey.MapEntry(Seq(100), 4),
        MultiMapKey.MapEntry(Seq(100), 5),
        MultiMapKey.MapEnd(Seq(100))
      )

      //key-values should
      map.toList shouldBe expected
    }
  }
}
