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

package swaydb.core.segment.format.a.index

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{KeyValue, Persistent}
import swaydb.core.segment.format.a.{KeyMatcher, OffsetBase}
import swaydb.core.util.Bytes
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Try

/**
  * HashIndex.
  */
private[core] object HashIndex extends LazyLogging {

  val formatID: Byte = 1.toByte

  case class Offset(start: Int, size: Int) extends OffsetBase

  final case class State(var hit: Int,
                         var miss: Int,
                         writeAbleLargestValueSize: Int,
                         headerSize: Int,
                         maxProbe: Int,
                         bytes: Slice[Byte])

  def init(maxProbe: Int,
           keyCount: Int,
           largestValue: Int,
           size: Int): Option[HashIndex.State] =
    if (size <= 4) //formatId, maxProbe, hit, miss
      None
    else
      Some {
        val writeAbleLargestValueSize = Bytes.sizeOf(largestValue + 1)
        HashIndex.State(
          hit = 0,
          miss = 0,
          maxProbe = maxProbe,
          headerSize = headerSize(keyCounts = keyCount, writeAbleLargestValueSize = writeAbleLargestValueSize),
          writeAbleLargestValueSize = writeAbleLargestValueSize,
          bytes = Slice.create[Byte](size)
        )
      }

  def init(maxProbe: Int,
           keyValues: Iterable[KeyValue.WriteOnly]): Option[State] =
    init(
      maxProbe = maxProbe,
      keyCount = keyValues.last.stats.segmentUniqueKeysCount,
      largestValue = keyValues.last.stats.thisKeyValuesAccessIndexOffset,
      size = keyValues.last.stats.segmentHashIndexSize
    )

  def headerSize(keyCounts: Int,
                 writeAbleLargestValueSize: Int): Int = {
    val headerSize =
      ByteSizeOf.byte + //formatId
        ByteSizeOf.int + //max probe
        (Bytes.sizeOf(keyCounts) * 2) + //hit & miss rate
        Bytes.sizeOf(writeAbleLargestValueSize) + //largest value size
        ByteSizeOf.int //allocated bytes

    Bytes.sizeOf(headerSize) +
      headerSize
  }

  /**
    * Number of bytes required to build a high probability index.
    */
  def optimalBytesRequired(keyCounts: Int,
                           largestValue: Int,
                           compensate: Int => Int): Int = {
    val writeAbleLargestValueSize = Bytes.sizeOf(largestValue + 1) //largest value is +1 because 0s are reserved.

    val bytesWithOutCompensation =
      headerSize(
        keyCounts = keyCounts,
        writeAbleLargestValueSize = writeAbleLargestValueSize
      ) + (keyCounts * (writeAbleLargestValueSize + 1)) //+1 to skip left & right 0 start-end markers.

    bytesWithOutCompensation +
      Try(compensate(bytesWithOutCompensation)).getOrElse(0) //optionally add more space or remove.
  }

  def writeHeader(state: State): IO[Unit] =
    IO {
      //it's important to move to 0 to write to head of the file.
      state.bytes moveWritePosition 0
      state.bytes add formatID
      state.bytes addInt state.maxProbe
      state.bytes addIntUnsigned state.hit
      state.bytes addIntUnsigned state.miss
      state.bytes addIntUnsigned state.writeAbleLargestValueSize
      state.bytes addIntUnsigned state.headerSize
      state.bytes addInt state.bytes.size
      if (state.bytes.currentWritePosition > state.headerSize)
        throw new Exception(s"Calculated header size was invalid. Used: ${state.bytes.currentWritePosition - 1}. Expected: ${state.headerSize} ")
    }

  def read(offset: Offset, reader: Reader): IO[HashIndex] = {
    val movedReader = reader.moveTo(offset.start)
    for {
      formatID <- movedReader.get()
      maxProbe <- movedReader.readInt()
      hit <- movedReader.readIntUnsigned()
      miss <- movedReader.readIntUnsigned()
      largestValueSize <- movedReader.readIntUnsigned()
      headerSize <- movedReader.readIntUnsigned()
      allocatedBytes <- movedReader.readInt()
      index <-
      if (formatID != HashIndex.formatID)
        IO.Failure(IO.Error.Fatal(new Exception(s"Invalid formatID: $formatID. Expected: ${HashIndex.formatID}")))
      else
        IO.Success(
          HashIndex(
            offset = offset,
            formatId = formatID,
            maxProbe = maxProbe,
            hit = hit,
            miss = miss,
            writeAbleLargestValueSize = largestValueSize,
            headerSize = headerSize,
            allocatedBytes = allocatedBytes
          )
        )
    } yield index
  }

  def adjustHash(hash: Int, totalBlockSpace: Int, headerSize: Int, writeAbleLargestValueSize: Int) = {
    //adjust the hashIndex such that it does not overwrite the header bytes.
    //add +1 to offset left and right 0 bytes.
    val indexBlock = totalBlockSpace - writeAbleLargestValueSize - headerSize
    //+1 to headerSize for skipping the first nonHeader 0 byte which could be an index marker.
    ((hash & Int.MaxValue) % indexBlock) + headerSize
  }

  /**
    * Mutates the slice and adds writes the indexOffset to it's hash index.
    */
  def write(key: Slice[Byte],
            value: Int,
            state: State): IO[Boolean] = {

    //add 1 to each offset to avoid 0 offsets.
    //0 bytes are reserved as empty bucket markers.
    val valuePlusOne = value + 1
    //+1 to reserve left 0 byte,
    val bytesRequired = Bytes.sizeOf(valuePlusOne) + 1

    val hash = key.##
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    @tailrec
    def doWrite(key: Slice[Byte], probe: Int): Boolean =
      if (probe >= state.maxProbe) {
        //println(s"Key: ${key.readInt()}: write index: miss probe: $probe")
        state.miss += 1
        false
      } else {
        val hashIndex =
          adjustHash(
            hash = hash1 + probe * hash2,
            totalBlockSpace = state.bytes.size,
            headerSize = state.headerSize,
            writeAbleLargestValueSize = state.writeAbleLargestValueSize
          )

        val existing = state.bytes.take(hashIndex, bytesRequired + 1) //read one more to not overwrite next zero.
        if (existing.forall(_ == 0)) {
          state.bytes moveWritePosition (hashIndex + 1)
          state.bytes addIntUnsigned valuePlusOne
          state.hit += 1
          //println(s"Key: ${key.readInt()}: write hashIndex: $hashIndex probe: $probe, value: $value, valueBytes: ${Slice.writeIntUnsigned(valuePlusOne)} = success")
          true
        } else {
          //println(s"Key: ${key.readInt()}: write hashIndex: $hashIndex probe: $probe, value: $value, valueBytes: ${Slice.writeIntUnsigned(valuePlusOne)} = failure")
          doWrite(key = key, probe = probe + 1)
        }
      }

    if (state.bytes.size == 0)
      IO.`false`
    else
      IO(doWrite(key, 0))
  }

  /**
    * Finds a key in the hash index.
    *
    * @param assertValue performs find or forward fetch from the currently being read sorted index's hash block.
    */
  private[index] def find[V](key: Slice[Byte],
                             offset: HashIndex.Offset,
                             hashIndex: HashIndex,
                             reader: Reader,
                             assertValue: Int => IO[Option[V]]): IO[Option[V]] = {

    val hash = key.##
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    @tailrec
    def doFind(probe: Int, checkedHashIndexes: mutable.HashSet[Int]): IO[Option[V]] =
      if (probe > hashIndex.maxProbe) {
        IO.none
      } else {
        val index =
          adjustHash(
            hash = hash1 + probe * hash2,
            totalBlockSpace = hashIndex.allocatedBytes,
            headerSize = hashIndex.headerSize,
            writeAbleLargestValueSize = hashIndex.writeAbleLargestValueSize
          )
        if (checkedHashIndexes contains index) //do not check the same index again.
          doFind(probe + 1, checkedHashIndexes)
        else
          reader
            .moveTo(offset.start + index)
            .read(hashIndex.bytesToReadPerIndex) match {
            case IO.Success(possibleValueBytes) =>
              if (possibleValueBytes.head != 0) {
                //println(s"Key: ${key.readInt()}: read hashIndex: $index probe: $probe = failure - invalid start offset.")
                doFind(probe + 1, checkedHashIndexes)
              } else {
                possibleValueBytes
                  .dropHead()
                  .readIntUnsigned() match {
                  case IO.Success(possibleValue) =>
                    //submit the indexOffset removing the add 1 offset to avoid overlapping bytes.
                    //println(s"Key: ${key.readInt()}: read hashIndex: $index probe: $probe, sortedIndex: ${possibleValue - 1} = reading now!")

                    if (possibleValue == 0)
                      doFind(probe + 1, checkedHashIndexes)
                    else
                      assertValue(possibleValue - 1) match {
                        case success @ IO.Success(Some(_)) =>
                          //println(s"Key: ${key.readInt()}: read hashIndex: $index probe: $probe, sortedIndex: ${possibleValue - 1} = success")
                          success

                        case IO.Success(None) =>
                          //println(s"Key: ${key.readInt()}: read hashIndex: $index probe: $probe: sortedIndex: ${possibleValue - 1} = not found")
                          doFind(probe + 1, checkedHashIndexes += index)

                        case IO.Failure(error) =>
                          IO.Failure(error)
                      }

                  case IO.Failure(error) =>
                    IO.Failure(error)
                }
              }

            case IO.Failure(error) =>
              IO.Failure(error)
          }
      }

    doFind(0, mutable.HashSet.empty)
  }

  private[a] def get(matcher: KeyMatcher.GetNextPrefixCompressed,
                     reader: Reader,
                     hashIndex: HashIndex,
                     sortedIndexOffset: SortedIndex.Offset): IO[Option[Persistent]] =
    find(
      key = matcher.key,
      offset = hashIndex.offset,
      reader = reader,
      hashIndex = hashIndex,
      assertValue =
        sortedIndexOffsetValue =>
          SortedIndex.findAndMatchOrNext(
            matcher = matcher,
            fromOffset = sortedIndexOffset.start + sortedIndexOffsetValue,
            reader = reader,
            offset = sortedIndexOffset
          )
    )
}

case class HashIndex(offset: HashIndex.Offset,
                     formatId: Int,
                     maxProbe: Int,
                     hit: Int,
                     miss: Int,
                     writeAbleLargestValueSize: Int,
                     headerSize: Int,
                     allocatedBytes: Int) {
  val bytesToReadPerIndex = writeAbleLargestValueSize + 1 //+1 to read header 0 byte.
}