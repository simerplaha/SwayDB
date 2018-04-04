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

package swaydb.core

import swaydb.core.data.KeyValue.{ReadOnly, WriteOnly}
import swaydb.core.data.KeyValue.WriteOnly.Fixed
import swaydb.core.data.Transient.{Put, Range, Remove}
import swaydb.core.data.{Memory, Persistent, _}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
import swaydb.core.map.serializer.RangeValueSerializers._

import scala.reflect.ClassTag

trait TestData extends TryAssert {

  def randomCharacters(size: Int = 10) = Random.alphanumeric.take(size).mkString

  def randomBytes(size: Int = 10) = Array.fill(size)(randomByte())

  def randomByteChunks(size: Int = 10, sizePerChunk: Int = 10): Seq[Slice[Byte]] =
    (1 to size) map {
      _ =>
        Slice(randomBytes(sizePerChunk))
    }

  def randomBytesSlice(size: Int = 10) = Slice(randomBytes(size))

  def randomByte() = (Random.nextInt(256) - 128).toByte

  //  implicit class ToTuple(keyValues: Slice[KeyValue]) {
  //    def tuple: Iterable[KeyValueResponse] =
  //      keyValues.map {
  //        keyValue =>
  //          (keyValue.key, keyValue.getOrFetchValue)
  //      }
  //  }

  def ints(numbers: Int): Int =
    (1 to numbers).foldLeft("") {
      case (concat, _) =>
        concat + Math.abs(Random.nextInt(9)).toString
    }.toInt

  def randomInt(minus: Int = 0) = Math.abs(Random.nextInt(Int.MaxValue)) - minus - 1

  def randomIntKeyStringValues(count: Int = 5,
                               startId: Option[Int] = None,
                               addToValue: Int = 0,
                               valueSize: Int = 50,
                               nonValue: Boolean = false,
                               addRandomDeletes: Boolean = false,
                               addRandomRanges: Boolean = false): Slice[KeyValue.WriteOnly] =
    randomIntKeyValues(count, startId, addToValue, Some(valueSize), nonValue, addRandomDeletes, addRandomRanges)

  def randomIntKeyValues(count: Int = 5,
                         startId: Option[Int] = None,
                         addToValue: Int = 0,
                         valueSize: Option[Int] = None,
                         nonValue: Boolean = false,
                         addRandomDeletes: Boolean = false,
                         addRandomRanges: Boolean = false): Slice[KeyValue.WriteOnly] = {
    val slice = Slice.create[KeyValue.WriteOnly](count)
    //            var key = 1
    var key = startId getOrElse randomInt(minus = count)
    val until = key + count
    while (key < until) {
      if (nonValue) {
        slice add Transient.Put(key, previous = slice.lastOption, falsePositiveRate = 0.1)
        key = key + 1
      } else if ((addRandomDeletes || addRandomRanges) && Random.nextBoolean()) {
        if (addRandomDeletes) {
          slice add Remove(key, previous = slice.lastOption, falsePositiveRate = 0.1)
          key = key + 1
        }
        if (addRandomRanges) {
          val value: Slice[Byte] = valueSize.map(size => Random.nextString(size): Slice[Byte]).getOrElse(randomInt(): Slice[Byte])
          val toKey = key + 10
          val fromValue =
            if (Random.nextBoolean()) {
              if (Random.nextBoolean()) Some(Value.Remove) else Some(Value.Put(value))
            } else {
              None
            }

          val rangeValue =
            if (Random.nextBoolean()) {
              Value.Remove
            } else {
              Value.Put(value)
            }
          slice add Transient.Range[Value, Value](key, toKey, fromValue, rangeValue, previous = slice.lastOption, falsePositiveRate = 0.1)
          //randomly skip the Range's toKey for the next key.
          if (Random.nextBoolean())
            key = toKey
          else
            key = toKey + 1
        }
      } else {
        val value: Slice[Byte] = valueSize.map(size => Random.nextString(size): Slice[Byte]).getOrElse(randomInt(): Slice[Byte])
        slice add Transient.Put(key, value = value, previous = slice.lastOption, falsePositiveRate = 0.1)
        key = key + 1
      }
    }
    slice.close()
  }


  def randomIntKeys(count: Int = 5,
                    startId: Option[Int] = None): Slice[KeyValue.WriteOnly] =
    randomIntKeyValues(count = count, startId = startId, nonValue = true)
}

object TestData extends TestData