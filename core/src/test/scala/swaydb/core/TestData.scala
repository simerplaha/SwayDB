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

import swaydb.core.data.Transient.Remove
import swaydb.core.data.{KeyValueWriteOnly, Transient}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

trait TestData {

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

  def randomIntKeyValues(count: Int = 5,
                         startId: Int = 1,
                         addToValue: Int = 0,
                         nonValue: Boolean = false,
                         addRandomDeletes: Boolean = false): Slice[KeyValueWriteOnly] = {
    val slice = Slice.create[KeyValueWriteOnly](count)
    val startFrom = randomInt(minus = count)
    //    val startFrom = 1
    for (key <- startFrom until startFrom + count) {
      if (nonValue)
        slice add Transient.Put(key = key, previous = slice.lastOption, falsePositiveRate = 0.1)
      else if (addRandomDeletes && Random.nextBoolean())
        slice add Remove(key = key, previous = slice.lastOption, falsePositiveRate = 0.1)
      else
        slice add Transient.Put(key = key, value = randomInt(), previous = slice.lastOption, falsePositiveRate = 0.1)
    }
    slice
  }

  def randomIntKeyStringValues(count: Int = 5,
                               startId: Int = 1,
                               valueSize: Int = 50,
                               addRandomDeletes: Boolean = false): Slice[KeyValueWriteOnly] = {
    val slice = Slice.create[KeyValueWriteOnly](count)
    val startFrom = randomInt(minus = count)
    //            val startFrom = 1
    for (key <- startFrom until startFrom + count) {
      val keyValue =
        if (addRandomDeletes && Random.nextBoolean())
          Remove(key = key, previous = slice.lastOption, falsePositiveRate = 0.1)
        else
          Transient.Put(key = key, value = Random.nextString(valueSize), previous = slice.lastOption, falsePositiveRate = 0.1)
      slice add keyValue
    }
    slice
  }

  def sortedIntKeyStringValue(count: Int = 5,
                              startId: Int = 1,
                              keySize: Int = 16.bytes,
                              valueSize: Int = 50.bytes): List[(Int, String)] = {
    val map = mutable.Map.empty[Int, String]
    while (map.size < count) {
      val key: Int = ints(keySize / ByteSizeOf.int)
      val value = Random.nextString(valueSize)
      map.put(key, value)
    }
    map.toList.sortBy(_._1)
  }

  def sortedIntKeys(count: Int = 5,
                    keySize: Int = 16.bytes): Seq[Int] = {
    val keys = ListBuffer.empty[Int]
    while (keys.size < count) {
      val key: Int = ints(keySize / ByteSizeOf.int)
      keys += key
    }
    keys.sorted
  }

  def randomIntKeys(count: Int = 5,
                    startId: Int = 1): Slice[KeyValueWriteOnly] =
    randomIntKeyValues(count = count, startId = startId, nonValue = true)
}

object TestData extends TestData