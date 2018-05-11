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
import swaydb.core.data.KeyValue.WriteOnly.Overwrite
import swaydb.core.data.Transient.{Put, Range, Remove}
import swaydb.core.data.Value.{FromValue, RangeValue}
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

import scala.concurrent.duration._
import scala.reflect.ClassTag

trait TestData extends TryAssert {

  def randomly[T](f: => T): Option[T] =
    if (Random.nextBoolean())
      Some(f)
    else
      None

  def randomStringOption: Option[Slice[Byte]] =
    if (Random.nextBoolean())
      Some(randomCharacters())
    else
      None

  def randomDeadlineOption: Option[Deadline] =
    if (Random.nextBoolean())
      Some(randomIntMax(30).seconds.fromNow)
    else if (Random.nextBoolean())
    //minus a minimum of 10.seconds for expired deadline. So test cases assertions to not fail if deadline has nanoseconds difference.
      Some(0.seconds.fromNow - (randomIntMax(30) + 10).seconds)
    else
      None

  def randomFixedKeyValue(key: Slice[Byte]): Memory.Fixed =
    if (Random.nextBoolean())
      Memory.Put(key, randomStringOption, randomDeadlineOption)
    else if (Random.nextBoolean())
      Memory.Remove(key, randomDeadlineOption)
    else
      Memory.Update(key, randomStringOption, randomDeadlineOption)

  def randomRangeKeyValue(from: Slice[Byte], to: Slice[Byte]): Memory.Range =
    Memory.Range(from, to, randomFromValueOption(), randomRangeValue())

  def randomRangeValueOption(from: Slice[Byte], to: Slice[Byte]): Option[Memory.Range] =
    if (Random.nextBoolean())
      Some(randomRangeKeyValue(from, to))
    else
      None

  def randomFromValueOption(): Option[Value.FromValue] =
    if (Random.nextBoolean())
      Some(randomFromValue())
    else
      None

  def randomFromValue(): Value.FromValue =
    if (Random.nextBoolean())
      Value.Put(randomStringOption, randomDeadlineOption)
    else if (Random.nextBoolean())
      Value.Remove(randomDeadlineOption)
    else
      Value.Update(randomStringOption, randomDeadlineOption)

  def randomRangeValue(): Value.RangeValue =
    if (Random.nextBoolean())
      Value.Remove(randomDeadlineOption)
    else
      Value.Update(randomStringOption, randomDeadlineOption)

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

  def randomIntMax(max: Int = Int.MaxValue) = Math.abs(Random.nextInt(max))

  def randomIntKeyStringValues(count: Int = 5,
                               startId: Option[Int] = None,
                               valueSize: Int = 50,
                               nonValue: Boolean = false,
                               addRandomRemoves: Boolean = false,
                               addRandomRanges: Boolean = false,
                               addRandomRemoveDeadlines: Boolean = false,
                               addRandomPutDeadlines: Boolean = false): Slice[KeyValue.WriteOnly] =
    randomIntKeyValues(
      count = count,
      startId = startId,
      valueSize = Some(valueSize),
      nonValue = nonValue,
      addRandomRemoves = addRandomRemoves,
      addRandomRanges = addRandomRanges,
      addRandomRemoveDeadlines = addRandomRemoveDeadlines,
      addRandomPutDeadlines = addRandomPutDeadlines
    )

  def randomizedIntKeyValues(count: Int = 5,
                             startId: Option[Int] = None,
                             valueSize: Int = 50,
                             nonValue: Boolean = false): Slice[KeyValue.WriteOnly] =
    randomIntKeyValues(
      count = count,
      startId = startId,
      valueSize = Some(valueSize),
      nonValue = nonValue,
      addRandomRemoves = Random.nextBoolean(),
      addRandomRanges = Random.nextBoolean(),
      addRandomRemoveDeadlines = Random.nextBoolean(),
      addRandomPutDeadlines = Random.nextBoolean()
    )

  def randomIntKeyValues(count: Int = 20,
                         startId: Option[Int] = None,
                         valueSize: Option[Int] = None,
                         nonValue: Boolean = false,
                         addRandomRemoves: Boolean = false,
                         addRandomRemoveDeadlines: Boolean = false,
                         addRandomPutDeadlines: Boolean = false,
                         addRandomRanges: Boolean = false): Slice[KeyValue.WriteOnly] = {
    val slice = Slice.create[KeyValue.WriteOnly](count)
    //            var key = 1
    var key = startId getOrElse randomInt(minus = count)
    val until = key + count
    while (key < until) {
      if (nonValue) {
        slice add Transient.Put(key, previous = slice.lastOption, if (addRandomPutDeadlines && Random.nextBoolean()) Some(10.seconds.fromNow) else None)
        key = key + 1
      } else if ((addRandomRemoves || addRandomRanges) && Random.nextBoolean()) {
        if (addRandomRemoves) {
          slice add Remove(key, previous = slice.lastOption, if (addRandomRemoveDeadlines && Random.nextBoolean()) Some(10.seconds.fromNow) else None)
          key = key + 1
        }
        if (addRandomRanges) {
          //          val value: Slice[Byte] = valueSize.map(size => Random.nextString(size): Slice[Byte]).getOrElse(randomInt(): Slice[Byte])
          val toKey = key + 10
          slice add Transient.Range[FromValue, RangeValue](key, toKey, randomFromValueOption(), randomRangeValue(), previous = slice.lastOption, falsePositiveRate = 0.1)
          //randomly skip the Range's toKey for the next key.
          if (Random.nextBoolean())
            key = toKey
          else
            key = toKey + randomIntMax(5)
        }
      } else {
        val value: Slice[Byte] = valueSize.map(size => Random.nextString(size): Slice[Byte]).getOrElse(randomInt(): Slice[Byte])
        slice add Transient.Put(key, value = value, previous = slice.lastOption, falsePositiveRate = 0.1)
        key = key + 1
      }
    }
    slice.close()
  }

  def randomIntKeys(count: Int = 20,
                    startId: Option[Int] = None): Slice[KeyValue.WriteOnly] =
    randomIntKeyValues(count = count, startId = startId, nonValue = true)
}

object TestData extends TestData