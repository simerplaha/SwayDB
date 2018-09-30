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

package swaydb.core.merge

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.CommonAssertions
import swaydb.core.data.{Memory, Transient, Value}
import swaydb.core.segment.merge.SegmentMerger
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class KeyValueMerger4_Remove_None_Spec extends WordSpec with Matchers with CommonAssertions {

  implicit val order = KeyOrder.default
  implicit val compression = groupingStrategy

  "Merging Remove(None) in any other randomly selected key-value" should {
    "always remove old key-value" in {

      (1 to 1000) foreach {
        i =>
          val newKeyValue = Memory.Remove(i, None)
          val oldKeyValue = randomFixedKeyValue(i)

          (newKeyValue, oldKeyValue).merge shouldBe newKeyValue
      }
    }

    "Dsads" in {

      val remove = Memory.Range(485, 4335, None, Value.Remove(None))
      val update = Memory.Put(3329, 3329, None)
      val put = Memory.Put(3329, 3329, None)
      val expire = Memory.Range(132, 3329, None, Value.Remove(1.day.fromNow))
      val expire2 = Memory.Remove(3329, 1.day.fromNow)

      val result =
        SegmentMerger.merge(
          Slice(update, put, expire, expire2),
          Slice(remove),
          10.seconds
        )

      result foreach {
        case range: Transient.Range =>
          println {
            s"Range(${range.fromKey.readInt()}, ${range.toKey.readInt()}, ${range.fromValue}, ${range.rangeValue})"
          }
      }

    }

    "Remove" in {
      val removeRange: Memory.Response = Memory.Range(1, 3, None, Value.Remove(None))
      val remove = Memory.Remove(3)

      val puts =
        (1 to 10) map {
          i =>
            Memory.Put(i, i.toString)
        }

      val expireRange = Memory.Range(1, 2, None, Value.Remove(100.days.fromNow))
      val expire = Memory.Remove(3, 100.days.fromNow)

      val removeRange2 = Memory.Range(4, 6, None, Value.Remove(None))
      val remove2 = Memory.Remove(6)

      val expireRange2 = Memory.Range(4, 5, None, Value.Remove(100.days.fromNow))
      val expire2 = Memory.Remove(5, 100.days.fromNow)

      val result =
      (Seq(remove) ++ puts ++ Seq(expireRange, expire, removeRange2, remove2) ++ puts ++ Seq(expireRange2, expire2)).foldLeft(Iterable(removeRange)) {
        case (oldKeyValues, newKeyValue) =>
          SegmentMerger.merge(Slice(newKeyValue), Slice(oldKeyValues.toArray), 10.seconds).toMemoryResponse
      }

      result foreach println


      //      val result = SegmentMerger.merge(Slice(Memory.Put(1, 1)), Slice(Memory.Range(1, 100, None, Value.Remove(None))), 10.seconds)
      //
      //      result foreach println

    }

  }
}
