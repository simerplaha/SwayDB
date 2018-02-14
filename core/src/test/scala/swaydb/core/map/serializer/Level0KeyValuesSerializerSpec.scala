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

package swaydb.core.map.serializer

import java.util.concurrent.ConcurrentSkipListMap

import swaydb.core.TestBase
import swaydb.core.data.ValueType
import swaydb.core.io.reader.Reader
import swaydb.core.map.MapEntry
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._
import scala.collection.JavaConverters._

class Level0KeyValuesSerializerSpec extends TestBase {

  implicit val ordering = KeyOrder.default

  "KeyValuesMemoryMapSerializer" should {

    val dir = testSegmentFile
    implicit val serializer = Level0KeyValuesSerializer(ordering)

    "write add key value of ValueType.Add" in {
      val addEntry = MapEntry.Add[Slice[Byte], (ValueType, Option[Slice[Byte]])](1, (ValueType.Add, Option(1)))

      val slice = Slice.create[Byte](addEntry.entryBytesSize)

      addEntry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      val readEntry = serializer.read(Reader(slice)).assertGet
      readEntry shouldBe addEntry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], (ValueType, Option[Slice[Byte]])](ordering)
      readEntry applyTo skipList
      val scalaSkipList = skipList.asScala

      scalaSkipList should have size 1
      val (headKey, (headValueType, headValue)) = scalaSkipList.head
      headKey shouldBe 1
      headValueType shouldBe ValueType.Add
      headValue.assertGet shouldBe 1

    }

    "write add key value of ValueType.Remove" in {
      val addEntry = MapEntry.Add[Slice[Byte], (ValueType, Option[Slice[Byte]])](1, (ValueType.Remove, Option(1)))

      val slice = Slice.create[Byte](addEntry.entryBytesSize)

      addEntry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      val readEntry = serializer.read(Reader(slice)).assertGet
      readEntry shouldBe addEntry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], (ValueType, Option[Slice[Byte]])](ordering)
      readEntry applyTo skipList
      val scalaSkipList = skipList.asScala

      scalaSkipList should have size 1
      val (headKey, (headValueType, headValue)) = scalaSkipList.head
      headKey shouldBe 1
      headValueType shouldBe ValueType.Remove
      headValue.assertGet shouldBe 1
    }

    "write remove key value" in {
      val mapEntry = MapEntry.Remove(1: Slice[Byte])

      val slice = Slice.create[Byte](mapEntry.entryBytesSize)

      mapEntry writeTo slice
      //this ensures that bytesRequiredFor is returning the correct size
      slice.isFull shouldBe true

      val readEntry = serializer.read(Reader(slice)).assertGet
      readEntry shouldBe mapEntry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], (ValueType, Option[Slice[Byte]])](ordering)
      readEntry applyTo skipList
      skipList.asScala shouldBe empty
    }

    "write add ++ remove map entry" in {
      val mapEntry =
        MapEntry.Add[Slice[Byte], (ValueType, Option[Slice[Byte]])](1, (ValueType.Add, Option(1))) ++
          MapEntry.Add[Slice[Byte], (ValueType, Option[Slice[Byte]])](2, (ValueType.Add, Option(2))) ++
          MapEntry.Add[Slice[Byte], (ValueType, Option[Slice[Byte]])](3, (ValueType.Add, Option(3))) ++
          MapEntry.Add[Slice[Byte], (ValueType, Option[Slice[Byte]])](3, (ValueType.Remove, Option(3))) ++
          MapEntry.Remove[Slice[Byte], (ValueType, Option[Slice[Byte]])](1)

      val slice = Slice.create[Byte](mapEntry.entryBytesSize)

      mapEntry writeTo slice
      //this ensures that bytesRequiredFor is returning the correct size
      slice.isFull shouldBe true

      val readEntry = serializer.read(Reader(slice)).assertGet
      val skipList = new ConcurrentSkipListMap[Slice[Byte], (ValueType, Option[Slice[Byte]])](ordering)
      readEntry applyTo skipList

      val scalaSkipList = skipList.asScala
      scalaSkipList should have size 2

      val (headKey, (headValueType, headValue)) = scalaSkipList.head
      headKey shouldBe 2
      headValueType shouldBe ValueType.Add
      headValue.assertGet shouldBe 2

      val (lastKey, (lastValueType, lastValue)) = scalaSkipList.last
      lastKey shouldBe 3
      lastValueType shouldBe ValueType.Remove
      lastValue.assertGet shouldBe 3
    }

    "report corruption" in {
      val addEntry =
        MapEntry.Add[Slice[Byte], (ValueType, Option[Slice[Byte]])](1, (ValueType.Add, Option(1))) ++
          MapEntry.Add[Slice[Byte], (ValueType, Option[Slice[Byte]])](2, (ValueType.Remove, Option(2)))

      val slice = Slice.create[Byte](addEntry.entryBytesSize)

      addEntry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      serializer.read(Reader(slice.drop(1))).failed.assertGet shouldBe a[ArrayIndexOutOfBoundsException]
    }

  }
}
