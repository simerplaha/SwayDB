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

import swaydb.compression.CompressionInternal
import swaydb.core.data.Value.{FromValue, RangeValue}
import swaydb.core.data.{Memory, _}
import swaydb.core.map.serializer.RangeValueSerializers._
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._
import scala.util.Random

trait TestData extends TryAssert {

  def randomStringOption: Option[Slice[Byte]] =
    if (Random.nextBoolean())
      Some(randomCharacters())
    else
      None

  def randomDeadlineOption: Option[Deadline] =
    if (Random.nextBoolean())
      Some(randomDeadline())
    else
      None

  def randomDeadline(): Deadline =
    if (Random.nextBoolean())
      randomIntMax(30).seconds.fromNow
    else
      0.seconds.fromNow - (randomIntMax(30) + 10).seconds

  def randomPutKeyValue(key: Slice[Byte],
                        value: Option[Slice[Byte]] = randomStringOption): Memory.Fixed =
    Memory.Put(key, value, randomDeadlineOption)

  def randomRemoveKeyValue(key: Slice[Byte],
                           deadline: Option[Deadline] = randomDeadlineOption): Memory.Remove =
    Memory.Remove(key, deadline)

  def randomFixedKeyValue(key: Slice[Byte],
                          value: Option[Slice[Byte]] = randomStringOption): Memory.Fixed =
    if (Random.nextBoolean())
      Memory.Put(key, value, randomDeadlineOption)
    else if (Random.nextBoolean())
      Memory.Remove(key, randomDeadlineOption)
    else
      Memory.Update(key, value, randomDeadlineOption)

  def randomCompression(minCompressionPercentage: Double = Double.MinValue): CompressionInternal =
    CompressionInternal.random(minCompressionPercentage = minCompressionPercentage)

  def randomCompressionLZ4OrSnappy(minCompressionPercentage: Double = Double.MinValue): CompressionInternal =
    CompressionInternal.randomLZ4OrSnappy(minCompressionPercentage = minCompressionPercentage)

  def randomCompressionLZ4(minCompressionPercentage: Double = Double.MinValue): CompressionInternal =
    CompressionInternal.randomLZ4(minCompressionPercentage = minCompressionPercentage)

  def randomRangeKeyValue(from: Slice[Byte],
                          to: Slice[Byte],
                          fromValue: Option[FromValue] = randomFromValueOption(),
                          rangeValue: RangeValue = randomRangeValue()): Memory.Range =
    Memory.Range(from, to, fromValue, rangeValue)

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

  def randomIntMax(max: Int = Int.MaxValue) =
    Math.abs(Random.nextInt(max))

  def randomIntMaxOption(max: Int = Int.MaxValue) =
    if (Random.nextBoolean())
      Some(randomIntMax(max))
    else
      None

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
                             nonValue: Boolean = false,
                             addRandomRemoves: Boolean = Random.nextBoolean(),
                             addRandomRanges: Boolean = Random.nextBoolean(),
                             addRandomRemoveDeadlines: Boolean = Random.nextBoolean(),
                             addRandomPutDeadlines: Boolean = Random.nextBoolean(),
                             addRandomGroups: Boolean = Random.nextBoolean()): Slice[KeyValue.WriteOnly] =
    randomIntKeyValues(
      count = count,
      startId = startId,
      valueSize = Some(valueSize),
      nonValue = nonValue,
      addRandomRemoves = addRandomRemoves,
      addRandomRanges = addRandomRanges,
      addRandomRemoveDeadlines = addRandomRemoveDeadlines,
      addRandomPutDeadlines = addRandomPutDeadlines,
      addRandomGroups = addRandomGroups
    )

  def groupsOnly(count: Int = 5,
                 startId: Option[Int] = None,
                 valueSize: Int = 50,
                 nonValue: Boolean = false): Slice[KeyValue.WriteOnly] =
    randomIntKeyValues(
      count = count,
      startId = startId,
      valueSize = Some(valueSize),
      nonValue = nonValue,
      addRandomGroups = true
    )

  def randomIntKeyValues(count: Int = 20,
                         startId: Option[Int] = None,
                         valueSize: Option[Int] = None,
                         nonValue: Boolean = false,
                         addRandomRemoves: Boolean = false,
                         addRandomRemoveDeadlines: Boolean = false,
                         addRandomPutDeadlines: Boolean = false,
                         addRandomRanges: Boolean = false,
                         addRandomGroups: Boolean = false): Slice[KeyValue.WriteOnly] = {
    //    println(
    //      s"""
    //        |nonValue : $nonValue
    //        |addRandomRemoves : $addRandomRemoves
    //        |addRandomRemoveDeadlines : $addRandomRemoveDeadlines
    //        |addRandomPutDeadlines : $addRandomPutDeadlines
    //        |addRandomRanges : $addRandomRanges
    //        |addRandomGroups : $addRandomGroups
    //      """.stripMargin)

    val slice = Slice.create[KeyValue.WriteOnly](count + 50) //extra space because addRandomRanges and random Groups can be added for Fixed and Range key-values in the same iteration.
    //            var key = 1
    var key = startId getOrElse randomInt(minus = count)
    val until = key + count
    var hasOne = false
    while (key < until || !hasOne) {
      hasOne = true
      if (nonValue) {
        slice add Transient.Put(
          key,
          previous = slice.lastOption,
          deadline = if (addRandomPutDeadlines && Random.nextBoolean()) Some(10.seconds.fromNow) else None,
          compressDuplicateValues = true
        )
        key = key + 1
      } else if ((addRandomRemoves || addRandomRanges || addRandomGroups) && Random.nextBoolean()) {
        if (addRandomRemoves) {
          slice add Transient.Remove(key, 0.1, slice.lastOption, deadline = if (addRandomRemoveDeadlines && Random.nextBoolean()) Some(10.seconds.fromNow) else None)

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
        if (addRandomGroups) {
          //create a Random group with the inner key-values the same as count of this group.
          val groupKeyValues =
            randomIntKeyValues(
              count = randomIntMax((count max 10) min 50),
              startId = Some(key),
              valueSize = valueSize,
              nonValue = nonValue,
              addRandomRemoves = addRandomRemoves,
              addRandomRemoveDeadlines = addRandomRemoveDeadlines,
              addRandomPutDeadlines = addRandomPutDeadlines,
              addRandomRanges = addRandomRanges,
              addRandomGroups = false //do not create more inner groups.
            )

          val groupToAdd = Transient.Group(groupKeyValues, randomCompression(), randomCompression(), 0.1, previous = slice.lastOption).assertGet
          slice add groupToAdd
          //randomly skip the Group's toKey for the next key. Next key should not be the same as toKey so add a minimum of 1 to next key.
          if (Random.nextBoolean())
            key = groupToAdd.maxKey.maxKey.readInt() + 1
          else
            key = groupToAdd.maxKey.maxKey.readInt() + 1 + randomIntMax(5)
        }
      } else {
        //        val value: Slice[Byte] = valueSize.map(size => Random.nextString(size): Slice[Byte]).getOrElse(randomInt(): Slice[Byte])
        slice add Transient.Put(
          key,
          value = key,
          previous = slice.lastOption,
          falsePositiveRate = 0.1,
          compressDuplicateValues = true
        )
        key = key + 1
      }
    }
    slice.close()
  }

  def randomIntKeys(count: Int = 20,
                    startId: Option[Int] = None): Slice[KeyValue.WriteOnly] =
    randomIntKeyValues(count = count, startId = startId, nonValue = true)

  def randomGroup(keyValues: Slice[KeyValue.WriteOnly] = randomizedIntKeyValues(),
                  keyCompression: CompressionInternal = randomCompression(),
                  valueCompression: CompressionInternal = randomCompression(),
                  falsePositiveRate: Double = 0.1,
                  previous: Option[KeyValue.WriteOnly] = None): Transient.Group =
    Transient.Group(
      keyValues = keyValues,
      indexCompression = keyCompression,
      valueCompression = valueCompression,
      falsePositiveRate = falsePositiveRate,
      previous = previous
    ).assertGet

}

object TestData extends TestData