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

package swaydb.extensions

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.TestData._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers.Serializer

import scala.collection.SortedSet
import scala.util.Random

class KeySpec extends WordSpec with Matchers {

  "mapKeySerializer" should {
    def doAssert[T](key: Key[T])(implicit serializer: Serializer[T]) = {
      val mapKeySerializer = Key.serializer[T](serializer)
      val wrote = mapKeySerializer.write(key)
      val read = mapKeySerializer.read(wrote)
      read shouldBe key
    }

    "write & read empty keys" in {
      doAssert(Key.MapStart(Seq.empty[Int]))
      doAssert(Key.MapEntriesStart(Seq.empty[Int]))
      doAssert(Key.MapEntry(Seq.empty[Int], 100))
      doAssert(Key.MapEntriesEnd(Seq.empty[Int]))
      doAssert(Key.SubMapsStart(Seq.empty[Int]))
      doAssert(Key.SubMap(Seq.empty[Int], 1000))
      doAssert(Key.SubMapsEnd(Seq.empty[Int]))
      doAssert(Key.MapEnd(Seq.empty[Int]))
    }

    "write & read MapKeys with Int key" in {
      doAssert(Key.MapStart(Seq(1)))
      doAssert(Key.MapEntriesStart(Seq(1)))
      doAssert(Key.MapEntry(Seq(1), 100))
      doAssert(Key.MapEntriesEnd(Seq(1)))
      doAssert(Key.SubMapsStart(Seq(1)))
      doAssert(Key.SubMap(Seq(1), 1000))
      doAssert(Key.SubMapsEnd(Seq(1)))
      doAssert(Key.MapEnd(Seq(1)))
    }

    "write & read MapKeys with multiple Int keys" in {
      doAssert(Key.MapStart(Seq(1, 2, 3)))
      doAssert(Key.MapEntriesStart(Seq(1, 2, 3)))
      doAssert(Key.MapEntry(Seq(1, 2, 3), 100))
      doAssert(Key.MapEntriesEnd(Seq(1, 2, 3)))
      doAssert(Key.SubMapsStart(Seq(1, 2, 3)))
      doAssert(Key.SubMap(Seq(1, 2, 3), 1000))
      doAssert(Key.SubMapsEnd(Seq(1, 2, 3)))
      doAssert(Key.MapEnd(Seq(1, 2, 3)))
    }

    "write & read MapKeys with Int String" in {
      doAssert(Key.MapStart(Seq("one")))
      doAssert(Key.MapEntriesStart(Seq("one")))
      doAssert(Key.MapEntry(Seq("one"), "one key"))
      doAssert(Key.MapEntriesEnd(Seq("one")))
      doAssert(Key.SubMapsStart(Seq("one")))
      doAssert(Key.SubMap(Seq("one"), "one sub map"))
      doAssert(Key.SubMapsEnd(Seq("one")))
      doAssert(Key.MapEnd(Seq("one")))
    }

    "write & read MapKeys with large single value" in {
      doAssert(Key.MapStart(Seq(randomCharacters(100000))))
      doAssert(Key.MapEntriesStart(Seq(randomCharacters(100000))))
      doAssert(Key.MapEntriesEnd(Seq(randomCharacters(100000))))
      doAssert(Key.MapEntry(Seq(randomCharacters(100000)), randomCharacters(100000)))
      doAssert(Key.SubMapsStart(Seq(randomCharacters(100000))))
      doAssert(Key.SubMap(Seq(randomCharacters(100000)), randomCharacters(100000)))
      doAssert(Key.SubMapsEnd(Seq(randomCharacters(100000))))
      doAssert(Key.MapEnd(Seq(randomCharacters(100000))))
    }

    "write & read MapKeys with Double" in {
      doAssert(Key.MapStart(Seq(Double.MinValue)))
      doAssert(Key.MapEntriesStart(Seq(Double.MinValue)))
      doAssert(Key.MapEntry(Seq(Double.MinValue), Double.MaxValue))
      doAssert(Key.MapEntriesEnd(Seq(Double.MinValue)))
      doAssert(Key.SubMapsStart(Seq(Double.MinValue)))
      doAssert(Key.SubMap(Seq(Double.MinValue), Double.MaxValue))
      doAssert(Key.SubMapsEnd(Seq(Double.MinValue)))
      doAssert(Key.MapEnd(Seq(Double.MinValue)))
    }
  }

  "ordering" should {
    "ordering MapKeys in the order of Start, Entry & End" in {
      val order = KeyOrder(Ordering.by[Slice[Byte], Int](_.readInt())(Ordering.Int))
      val mapKeySerializer = Key.serializer[Int](IntSerializer)
      implicit val mapKeyOrder = Ordering.by[Key[Int], Slice[Byte]](mapKeySerializer.write)(Key.ordering(order))

      val keys = Seq(
        Key.MapStart(Seq.empty[Int]),
        Key.SubMapsStart(Seq.empty[Int]),
        Key.SubMapsEnd(Seq.empty[Int]),
        Key.MapEnd(Seq.empty[Int]),

        Key.MapStart(Seq(0)),
        Key.SubMapsStart(Seq(0)),
        Key.SubMapsEnd(Seq(0)),
        Key.MapEnd(Seq(0)),

        Key.MapStart(Seq(1)),
        Key.MapEntriesStart(Seq(1)),
        Key.MapEntry(Seq(1), 1),
        Key.MapEntriesEnd(Seq(1)),
        Key.SubMapsStart(Seq(1)),
        Key.SubMap(Seq(1), 1000),
        Key.SubMapsEnd(Seq(1)),
        Key.MapEnd(Seq(1)),

        Key.MapStart(Seq(100)),
        Key.MapEntriesStart(Seq(100)),
        Key.MapEntry(Seq(100), 2),
        Key.MapEntry(Seq(100), 3),
        Key.MapEntry(Seq(100), 4),
        Key.MapEntry(Seq(100), 5),
        Key.MapEntriesEnd(Seq(100)),
        Key.SubMapsStart(Seq(100)),
        Key.SubMap(Seq(100), 1000),
        Key.SubMap(Seq(100), 2000),
        Key.SubMap(Seq(100), 3000),
        Key.SubMapsEnd(Seq(100)),
        Key.MapEnd(Seq(100)),

        Key.MapStart(Seq(2, 3)),
        Key.MapEntriesStart(Seq(2, 3)),
        Key.MapEntry(Seq(2, 3), 2),
        Key.MapEntry(Seq(2, 3), 3),
        Key.MapEntriesEnd(Seq(2, 3)),
        Key.SubMapsStart(Seq(2, 3)),
        Key.SubMap(Seq(2, 3), 1000),
        Key.SubMap(Seq(2, 3), 2000),
        Key.SubMapsEnd(Seq(2, 3)),
        Key.MapEnd(Seq(2, 3))
      )

      //shuffle and create a list
      val map = SortedSet[Key[Int]](Random.shuffle(keys): _*)(mapKeyOrder)

      //key-values should
      map.toList shouldBe keys
    }

    "ordering MapKeys in the order of Start, Entry & End when keys are large String" in {
      val order = KeyOrder(Ordering.by[Slice[Byte], String](_.readString())(Ordering.String))
      val mapKeySerializer = Key.serializer[String](StringSerializer)
      implicit val mapKeyOrder = Ordering.by[Key[String], Slice[Byte]](mapKeySerializer.write)(Key.ordering(order))

      val stringLength = 100000

      val randomString1 = "a" + randomCharacters(stringLength)
      val randomString2 = "b" + randomCharacters(stringLength)
      val randomString3 = "c" + randomCharacters(stringLength)
      val randomString4 = "d" + randomCharacters(stringLength)
      val randomString5 = "e" + randomCharacters(stringLength)

      val keys = Seq(
        Key.MapStart(Seq(randomString1)),
        Key.SubMapsStart(Seq(randomString1)),
        Key.SubMapsEnd(Seq(randomString1)),
        Key.MapEnd(Seq(randomString1)),

        Key.MapStart(Seq(randomString2)),
        Key.MapEntriesStart(Seq(randomString2)),
        Key.MapEntry(Seq(randomString2), randomString3),
        Key.MapEntry(Seq(randomString2), randomString4),
        Key.MapEntry(Seq(randomString2), randomString5),
        Key.MapEntriesEnd(Seq(randomString2)),
        Key.SubMapsStart(Seq(randomString2)),
        Key.SubMap(Seq(randomString2), randomString3),
        Key.SubMap(Seq(randomString2), randomString4),
        Key.SubMap(Seq(randomString2), randomString5),
        Key.SubMapsEnd(Seq(randomString2)),
        Key.MapEnd(Seq(randomString2)),

        Key.MapStart(Seq(randomString3)),
        Key.MapEntry(Seq(randomString3), randomString3),
        Key.MapEntry(Seq(randomString3), randomString4),
        Key.MapEntry(Seq(randomString3), randomString5),
        Key.MapEnd(Seq(randomString3))
      )

      //shuffle and create a list
      val map = SortedSet[Key[String]](Random.shuffle(keys): _*)(mapKeyOrder)

      //key-values should
      map.toList shouldBe keys
    }

    "remove duplicate key-values" in {
      val order = KeyOrder(Ordering.by[Slice[Byte], Int](_.readInt())(Ordering.Int))
      val mapKeySerializer = Key.serializer[Int](IntSerializer)
      implicit val mapKeyOrder = Ordering.by[Key[Int], Slice[Byte]](mapKeySerializer.write)(Key.ordering(order))

      val keys = Seq(
        Key.MapStart(Seq(0)),
        Key.MapEntriesStart(Seq(0)),
        Key.MapEntriesEnd(Seq(0)),
        Key.MapEnd(Seq(0)),
        Key.MapStart(Seq(0)),
        Key.MapEntriesStart(Seq(0)),
        Key.MapEntriesEnd(Seq(0)),
        Key.MapEnd(Seq(0)),

        Key.MapStart(Seq(2)),
        Key.MapEntriesStart(Seq(2)),
        Key.MapEntry(Seq(2), 2),
        Key.MapEntry(Seq(2), 2),
        Key.MapEntriesEnd(Seq(2)),
        Key.SubMapsStart(Seq(2)),
        Key.SubMap(Seq(2), 1000),
        Key.SubMap(Seq(2), 1000),
        Key.SubMapsEnd(Seq(2)),
        Key.MapEnd(Seq(2)),

        Key.MapStart(Seq(100)),
        Key.MapEntry(Seq(100), 4),
        Key.MapEntry(Seq(100), 5),
        Key.MapEnd(Seq(100))
      )

      //shuffle and create a list
      val map = SortedSet[Key[Int]](Random.shuffle(keys): _*)(mapKeyOrder)

      val expected = Seq(
        Key.MapStart(Seq(0)),
        Key.MapEntriesStart(Seq(0)),
        Key.MapEntriesEnd(Seq(0)),
        Key.MapEnd(Seq(0)),

        Key.MapStart(Seq(2)),
        Key.MapEntriesStart(Seq(2)),
        Key.MapEntry(Seq(2), 2),
        Key.MapEntriesEnd(Seq(2)),
        Key.SubMapsStart(Seq(2)),
        Key.SubMap(Seq(2), 1000),
        Key.SubMapsEnd(Seq(2)),
        Key.MapEnd(Seq(2)),

        Key.MapStart(Seq(100)),
        Key.MapEntry(Seq(100), 4),
        Key.MapEntry(Seq(100), 5),
        Key.MapEnd(Seq(100))
      )

      //key-values should
      map.toList shouldBe expected
    }
  }
}
