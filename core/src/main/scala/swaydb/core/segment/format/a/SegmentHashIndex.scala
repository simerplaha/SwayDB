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

package swaydb.core.segment.format.a

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.KeyValue
import swaydb.core.util.Bytes
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec
import scala.collection.{SortedSet, mutable}
import scala.util.Try

/**
  * HashIndex.
  */
object SegmentHashIndex extends LazyLogging {

  val formatID: Byte = 0.toByte

  val headerSize =
    ByteSizeOf.byte + // format ID
      ByteSizeOf.int + //max probe
      ByteSizeOf.int + //hit rate
      ByteSizeOf.int + //miss rate
      ByteSizeOf.boolean //has range

  final case class State(var hit: Int,
                         var miss: Int,
                         maxProbe: Int,
                         bytes: Slice[Byte],
                         commonRangePrefixesCount: SortedSet[Int])

  object Header {
    def create(maxProbe: Int,
               hit: Int,
               miss: Int,
               rangeIndexingEnabled: Boolean): Header =
      new Header(
        formatId = formatID,
        maxProbe = maxProbe,
        hit = hit,
        miss = miss,
        rangeIndexingEnabled = rangeIndexingEnabled
      )
  }

  case class Header(formatId: Int,
                    maxProbe: Int,
                    hit: Int,
                    miss: Int,
                    rangeIndexingEnabled: Boolean)

  /**
    * Number of bytes required to build a high probability index.
    */
  def optimalBytesRequired(hashIndexItemsCount: Int,
                           largestSortedIndexOffset: Int,
                           minimumNumberOfKeyValues: Int,
                           compensate: Int => Int): Int =
    if (hashIndexItemsCount <= minimumNumberOfKeyValues) {
      headerSize
    } else {
      //number of bytes required for hash indexes. +1 to skip 0 empty markers.
      val bytesWithOutCompensation = hashIndexItemsCount * Bytes.sizeOf(largestSortedIndexOffset + 1)
      val bytesRequired =
        headerSize +
          (ByteSizeOf.int + 1) + //give it another 5 bytes incase the hash index is the last index. Since varints are written a max of 5 bytes can be taken for an in with large index.
          bytesWithOutCompensation +
          Try(compensate(bytesWithOutCompensation)).getOrElse(0) //optionally add compensation space or remove.

      //in case compensation returns negative, reserve enough bytes for the header.
      bytesRequired max headerSize
    }

  /**
    * Number of bytes required to build a high probability index.
    */
  def optimalBytesRequired(lastKeyValue: KeyValue.WriteOnly,
                           minimumNumberOfKeyValues: Int,
                           compensate: Int => Int): Int =
    optimalBytesRequired(
      hashIndexItemsCount = lastKeyValue.stats.position,
      minimumNumberOfKeyValues = minimumNumberOfKeyValues,
      largestSortedIndexOffset = lastKeyValue.stats.thisKeyValuesHashIndexesSortedIndexOffset,
      compensate = compensate
    )

  def writeHeader(state: State): IO[Slice[Byte]] =
    IO {
      //it's important to move to 0 to write to head of the file.
      state.bytes moveWritePositionUnsafe 0
      state.bytes add formatID
      state.bytes addIntUnsigned state.maxProbe
      state.bytes addInt state.hit
      state.bytes addInt state.miss
      state.bytes addBoolean false //range indexing is not implemented.
    }

  def readHeader(reader: Reader): IO[Header] =
    for {
      formatID <- reader.get()
      maxProbe <- reader.readIntUnsigned()
      hit <- reader.readInt()
      miss <- reader.readInt()
      hasRange <- reader.readBoolean()
    } yield
      Header(
        formatId = formatID,
        maxProbe = maxProbe,
        hit = hit,
        miss = miss,
        rangeIndexingEnabled = hasRange
      )

  def generateHashIndex(key: Slice[Byte],
                        totalBlockSpace: Int,
                        probe: Int) =
    if (totalBlockSpace <= headerSize) {
      headerSize //if there are no bytes reserved for hashIndex, just return the next hashIndex to be an overflow.
    } else {
      val hash = key.##
      val hash1 = hash >>> 32
      val hash2 = (hash << 32) >> 32
      val computedHash = hash1 + probe * hash2
      //create a hash with reserved header bytes removed.
      //add headerSize of offset the output index skipping header bytes.
      //similar to optimalBytesRequired adding 5 bytes to add space for last indexes, here we remove those bytes to
      //generate indexes accounting for the last index being the largest integer with 5 bytes.
      ((computedHash & Int.MaxValue) % (totalBlockSpace - (ByteSizeOf.int + 1) - headerSize)) + headerSize
    }

  /**
    * Mutates the slice and adds writes the indexOffset to it's hash index.
    */
  def write(key: Slice[Byte],
            toKey: Option[Slice[Byte]],
            sortedIndexOffset: Int,
            state: State): IO[Unit] = {

    //add 1 to each offset to avoid 0 offsets.
    //0 bytes are reserved as empty bucket markers.
    val indexOffsetPlusOne = sortedIndexOffset + 1

    @tailrec
    def doWrite(key: Slice[Byte], probe: Int): Unit =
      if (probe >= state.maxProbe) {
        //println(s"Key: ${key.readInt()}: write index: miss probe: $probe")
        state.miss += 1
      } else {
        val index = generateHashIndex(key, state.bytes.size, probe)
        val indexBytesRequired = Bytes.sizeOf(indexOffsetPlusOne)
        if (index + indexBytesRequired >= state.bytes.size) {
          state.miss += 1
        } else if (state.bytes.take(index, indexBytesRequired).forall(_ == 0)) {
          state.bytes moveWritePositionUnsafe index
          state.bytes addIntUnsigned indexOffsetPlusOne
          state.hit += 1
          //println(s"Key: ${key.readInt()}: write hashIndex: $index probe: $probe, sortedIndexOffset: $sortedIndexOffset, writeBytes: ${Slice.writeIntUnsigned(sortedIndexOffset)} = success")
        } else {
          //println(s"Key: ${key.readInt()}: write hashIndex: $index probe: $probe, sortedIndexOffset: $sortedIndexOffset, writeBytes: ${Slice.writeIntUnsigned(sortedIndexOffset)} = failure")
          doWrite(key = key, probe = probe + 1)
        }
      }

    if (state.bytes.size == 0)
      IO.unit
    else
      toKey map {
        toKey =>
          IO {
            doWrite(key, 0)
            val commonPrefixBytes = Bytes.commonPrefixBytes(key, toKey)
            //TODO - temporary assert to make sure that common bytes.
            assert(state.commonRangePrefixesCount.contains(commonPrefixBytes.size))
            doWrite(commonPrefixBytes, 0)
          }
      } getOrElse IO(doWrite(key, 0))
  }

  /**
    * Finds a key in the hash index.
    *
    * @param get performs get or forward fetch from the currently being read sorted index's hash block.
    */
  def find[K <: KeyValue](key: Slice[Byte],
                          hashIndexReader: Reader,
                          hashIndexStartOffset: Int,
                          hashIndexSize: Int,
                          maxProbe: Int,
                          get: Int => IO[Option[K]]): IO[Option[K]] = {

    @tailrec
    def doFind(probe: Int, checkedHashIndexes: mutable.HashSet[Int]): IO[Option[K]] =
      if (probe > maxProbe) {
        IO.none
      } else {
        val hashIndex = generateHashIndex(key, hashIndexSize, probe)
        if (checkedHashIndexes contains hashIndex) //do not check the same index again.
          doFind(probe + 1, checkedHashIndexes)
        else
          hashIndexReader.moveTo(hashIndexStartOffset + hashIndex).readIntUnsigned() match {
            case IO.Success(possibleSortedIndexOffset) =>
              if (possibleSortedIndexOffset == 0) {
                doFind(probe + 1, checkedHashIndexes)
              } else {
                //submit the indexOffset removing the add 1 offset to avoid overlapping bytes.
                //println(s"Key: ${key.readInt()}: read hashIndex: $hashIndex probe: $probe, sortedIndex: ${possibleSortedIndexOffset - 1} = reading now!")
                get(possibleSortedIndexOffset - 1) match {
                  case success @ IO.Success(Some(_)) =>
                    //println(s"Key: ${key.readInt()}: read hashIndex: $hashIndex probe: $probe, sortedIndex: ${possibleSortedIndexOffset - 1} = success")
                    success

                  case IO.Success(None) =>
                    //println(s"Key: ${key.readInt()}: read hashIndex: $hashIndex probe: $probe: sortedIndex: ${possibleSortedIndexOffset - 1} = not found")
                    doFind(probe + 1, checkedHashIndexes += hashIndex)

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
}

