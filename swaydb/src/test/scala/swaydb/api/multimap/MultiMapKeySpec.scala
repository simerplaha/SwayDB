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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.api.multimap

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.MultiMapKey
import swaydb.core.TestData._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers.Serializer

import scala.collection.SortedSet
import scala.util.{Random, Try}

class MultiMapKeySpec extends AnyWordSpec with Matchers {

  "mapKeySerializer" should {
    def doAssert[T, K](key: MultiMapKey[T, K])(implicit keySerializer: Serializer[K],
                                               tableSerializer: Serializer[T]) = {
      val mapKeySerializer = MultiMapKey.serializer[T, K](keySerializer, tableSerializer)
      val wrote = mapKeySerializer.write(key)
      val read = mapKeySerializer.read(wrote)
      read shouldBe key
    }

    "write & read empty keys" in {
      doAssert[Int, Int](MultiMapKey.MapStart(0))
      doAssert[Int, Int](MultiMapKey.MapEntriesStart(0))
      doAssert[Int, Int](MultiMapKey.MapEntry(0, 100))
      doAssert[Int, Int](MultiMapKey.MapEntriesEnd(0))
      doAssert[Int, Int](MultiMapKey.SubMapsStart(0))
      doAssert[Int, Int](MultiMapKey.SubMap(0, 1000))
      doAssert[Int, Int](MultiMapKey.SubMapsEnd(0))
      doAssert[Int, Int](MultiMapKey.MapEnd(0))
    }

    "write & read MapKeys with Int key" in {
      doAssert[Int, Int](MultiMapKey.MapStart(1))
      doAssert[Int, Int](MultiMapKey.MapEntriesStart(1))
      doAssert[Int, Int](MultiMapKey.MapEntry(1, 100))
      doAssert[Int, Int](MultiMapKey.MapEntriesEnd(1))
      doAssert[Int, Int](MultiMapKey.SubMapsStart(1))
      doAssert[Int, Int](MultiMapKey.SubMap(1, 1000))
      doAssert[Int, Int](MultiMapKey.SubMapsEnd(1))
      doAssert[Int, Int](MultiMapKey.MapEnd(1))
    }


    "write & read MapKeys with Int String" in {
      doAssert[String, String](MultiMapKey.MapStart(1))
      doAssert[String, String](MultiMapKey.MapEntriesStart(1))
      doAssert[String, String](MultiMapKey.MapEntry(1, "one key"))
      doAssert[String, String](MultiMapKey.MapEntriesEnd(1))
      doAssert[String, String](MultiMapKey.SubMapsStart(1))
      doAssert[String, String](MultiMapKey.SubMap(1, "one sub map"))
      doAssert[String, String](MultiMapKey.SubMapsEnd(1))
      doAssert[String, String](MultiMapKey.MapEnd(1))
    }

    "write & read MapKeys with large single value" in {
      doAssert[String, String](MultiMapKey.MapStart(Long.MaxValue))
      doAssert[String, String](MultiMapKey.MapEntriesStart(Long.MaxValue))
      doAssert[String, String](MultiMapKey.MapEntriesEnd(Long.MaxValue))
      doAssert[String, String](MultiMapKey.MapEntry(Long.MaxValue, randomCharacters(100000)))
      doAssert[String, String](MultiMapKey.SubMapsStart(Long.MaxValue))
      doAssert[String, String](MultiMapKey.SubMap(Long.MaxValue, randomCharacters(100000)))
      doAssert[String, String](MultiMapKey.SubMapsEnd(Long.MaxValue))
      doAssert[String, String](MultiMapKey.MapEnd(Long.MaxValue))
    }

    "write & read MapKeys with Double" in {
      doAssert[Double, Double](MultiMapKey.MapStart(Long.MaxValue))
      doAssert[Double, Double](MultiMapKey.MapEntriesStart(Long.MaxValue))
      doAssert[Double, Double](MultiMapKey.MapEntry(Long.MaxValue, Double.MaxValue))
      doAssert[Double, Double](MultiMapKey.MapEntriesEnd(Long.MaxValue))
      doAssert[Double, Double](MultiMapKey.SubMapsStart(Long.MaxValue))
      doAssert[Double, Double](MultiMapKey.SubMap(Long.MaxValue, Double.MaxValue))
      doAssert[Double, Double](MultiMapKey.SubMapsEnd(Long.MaxValue))
      doAssert[Double, Double](MultiMapKey.MapEnd(Long.MaxValue))
    }
  }

  "ordering" should {
    "ordering MapKeys in the order of Start, Entry & End" in {
      val order = KeyOrder(Ordering.by[Slice[Byte], Int](_.readInt())(Ordering.Int))
      val mapKeySerializer = MultiMapKey.serializer[Int, Int](IntSerializer, IntSerializer)
      implicit val mapKeyOrder = Ordering.by[MultiMapKey[Int, Int], Slice[Byte]](mapKeySerializer.write)(MultiMapKey.ordering(order))

      val keys =
        Seq(
          MultiMapKey.MapStart(0),
          MultiMapKey.SubMapsStart(0),
          MultiMapKey.SubMapsEnd(0),
          MultiMapKey.MapEnd(0),

          MultiMapKey.MapStart(1),
          MultiMapKey.SubMapsStart(1),
          MultiMapKey.SubMapsEnd(1),
          MultiMapKey.MapEnd(1),

          MultiMapKey.MapStart(2),
          MultiMapKey.MapEntriesStart(2),
          MultiMapKey.MapEntry(2, 1),
          MultiMapKey.MapEntriesEnd(2),
          MultiMapKey.SubMapsStart(2),
          MultiMapKey.SubMap(2, 1000),
          MultiMapKey.SubMapsEnd(2),
          MultiMapKey.MapEnd(2),

          MultiMapKey.MapStart(100),
          MultiMapKey.MapEntriesStart(100),
          MultiMapKey.MapEntry(100, 2),
          MultiMapKey.MapEntry(100, 3),
          MultiMapKey.MapEntry(100, 4),
          MultiMapKey.MapEntry(100, 5),
          MultiMapKey.MapEntriesEnd(100),
          MultiMapKey.SubMapsStart(100),
          MultiMapKey.SubMap(100, 1000),
          MultiMapKey.SubMap(100, 2000),
          MultiMapKey.SubMap(100, 3000),
          MultiMapKey.SubMapsEnd(100),
          MultiMapKey.MapEnd(100),

          MultiMapKey.MapStart(200),
          MultiMapKey.MapEntriesStart(200),
          MultiMapKey.MapEntry(200, 2),
          MultiMapKey.MapEntry(200, 3),
          MultiMapKey.MapEntriesEnd(200),
          MultiMapKey.SubMapsStart(200),
          MultiMapKey.SubMap(200, 1000),
          MultiMapKey.SubMap(200, 2000),
          MultiMapKey.SubMapsEnd(200),
          MultiMapKey.MapEnd(200)
        )

      //shuffle and create a list
      val map = SortedSet[MultiMapKey[Int, Int]](Random.shuffle(keys): _*)(mapKeyOrder)

      //key-values should
      map.toList shouldBe keys
    }

    "ordering MapKeys in the order of Start, Entry & End when keys are large String" in {
      val order = KeyOrder(Ordering.by[Slice[Byte], String](_.readString())(Ordering.String))
      val mapKeySerializer = MultiMapKey.serializer[String, String](StringSerializer, StringSerializer)
      implicit val mapKeyOrder = Ordering.by[MultiMapKey[String, String], Slice[Byte]](mapKeySerializer.write)(MultiMapKey.ordering(order))

      val stringLength = 100000

      val randomString1 = "a" + randomCharacters(stringLength)
      val randomString2 = "b" + randomCharacters(stringLength)
      val randomString3 = "c" + randomCharacters(stringLength)
      val randomString4 = "d" + randomCharacters(stringLength)
      val randomString5 = "e" + randomCharacters(stringLength)

      val keys =
        Seq(
          MultiMapKey.MapStart(1),
          MultiMapKey.SubMapsStart(1),
          MultiMapKey.SubMapsEnd(1),
          MultiMapKey.MapEnd(1),

          MultiMapKey.MapStart(2),
          MultiMapKey.MapEntriesStart(2),
          MultiMapKey.MapEntry(2, randomString1),
          MultiMapKey.MapEntry(2, randomString2),
          MultiMapKey.MapEntry(2, randomString3),
          MultiMapKey.MapEntriesEnd(2),
          MultiMapKey.SubMapsStart(2),
          MultiMapKey.SubMap(2, randomString3),
          MultiMapKey.SubMap(2, randomString4),
          MultiMapKey.SubMap(2, randomString5),
          MultiMapKey.SubMapsEnd(2),
          MultiMapKey.MapEnd(2),

          MultiMapKey.MapStart(3),
          MultiMapKey.MapEntry(3, randomString3),
          MultiMapKey.MapEntry(3, randomString4),
          MultiMapKey.MapEntry(3, randomString5),
          MultiMapKey.MapEnd(3)
        )

      //shuffle and create a list
      val map = SortedSet[MultiMapKey[String, String]](Random.shuffle(keys): _*)(mapKeyOrder)

      //key-values should
      map.toList shouldBe keys
    }

    "remove duplicate key-values" in {
      val order = KeyOrder(Ordering.by[Slice[Byte], Int](_.readInt())(Ordering.Int))
      val mapKeySerializer = MultiMapKey.serializer[Int, Int](IntSerializer, IntSerializer)
      implicit val mapKeyOrder = Ordering.by[MultiMapKey[Int, Int], Slice[Byte]](mapKeySerializer.write)(MultiMapKey.ordering(order))

      val keys: Seq[() => MultiMapKey[Int, Int]] =
        Seq(
          () => MultiMapKey.MapStart(0),
          () => MultiMapKey.MapEntriesStart(0),
          () => MultiMapKey.MapEntriesEnd(0),
          () => MultiMapKey.MapEnd(0),
          () => MultiMapKey.MapStart(0),
          () => MultiMapKey.MapEntriesStart(0),
          () => MultiMapKey.MapEntriesEnd(0),
          () => MultiMapKey.MapEnd(0),

          () => MultiMapKey.MapStart(2),
          () => MultiMapKey.MapEntriesStart(2),
          () => MultiMapKey.MapEntry(2, 2),
          () => MultiMapKey.MapEntry(2, 2),
          () => MultiMapKey.MapEntriesEnd(2),
          () => MultiMapKey.SubMapsStart(2),
          () => MultiMapKey.SubMap(2, 1000), //depending on the order of inserion for this test either
          () => MultiMapKey.SubMap(2, 1000), //3 or 4 gets selected but in reality subMaps with same key will not have different subMapIds.
          () => MultiMapKey.SubMapsEnd(2),
          () => MultiMapKey.MapEnd(2),

          () => MultiMapKey.MapStart(100),
          () => MultiMapKey.MapEntry(100, 4),
          () => MultiMapKey.MapEntry(100, 5),
          () => MultiMapKey.MapEnd(100)
        )

      //shuffle and create a list
      val map = SortedSet[MultiMapKey[Int, Int]](Random.shuffle(keys).map(_ ()): _*)(mapKeyOrder)

      val expected1 =
        Seq(
          MultiMapKey.MapStart(0),
          MultiMapKey.MapEntriesStart(0),
          MultiMapKey.MapEntriesEnd(0),
          MultiMapKey.MapEnd(0),

          MultiMapKey.MapStart(2),
          MultiMapKey.MapEntriesStart(2),
          MultiMapKey.MapEntry(2, 2),
          MultiMapKey.MapEntriesEnd(2),
          MultiMapKey.SubMapsStart(2),
          MultiMapKey.SubMap(2, 1000),
          MultiMapKey.SubMapsEnd(2),
          MultiMapKey.MapEnd(2),

          MultiMapKey.MapStart(100),
          MultiMapKey.MapEntry(100, 4),
          MultiMapKey.MapEntry(100, 5),
          MultiMapKey.MapEnd(100)
        )

      val expected2 =
        Seq(
          MultiMapKey.MapStart(0),
          MultiMapKey.MapEntriesStart(0),
          MultiMapKey.MapEntriesEnd(0),
          MultiMapKey.MapEnd(0),

          MultiMapKey.MapStart(2),
          MultiMapKey.MapEntriesStart(2),
          MultiMapKey.MapEntry(2, 2),
          MultiMapKey.MapEntriesEnd(2),
          MultiMapKey.SubMapsStart(2),
          MultiMapKey.SubMap(2, 1000),
          MultiMapKey.SubMapsEnd(2),
          MultiMapKey.MapEnd(2),

          MultiMapKey.MapStart(100),
          MultiMapKey.MapEntry(100, 4),
          MultiMapKey.MapEntry(100, 5),
          MultiMapKey.MapEnd(100)
        )

      //key-values should
      Try(map.toList shouldBe expected1) getOrElse {
        map.toList shouldBe expected2
      }
    }
  }
}
