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
import swaydb.core.segment.format.a.OffsetBase
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

  val headerSize =
    ByteSizeOf.byte + // format ID
      ByteSizeOf.int + //max probe
      ByteSizeOf.int + //hit rate
      ByteSizeOf.int + //miss rate
      ByteSizeOf.boolean //has range

  final case class State(var hit: Int,
                         var miss: Int,
                         maxProbe: Int,
                         bytes: Slice[Byte])

  def init(maxProbe: Int,
           size: Int): Option[State] =
    if (size <= headerSize)
      None
    else
      Some(
        HashIndex.State(
          hit = 0,
          miss = 0,
          maxProbe = maxProbe,
          bytes = Slice.create[Byte](size)
        )
      )

  object Header {
    def apply(state: State): Header =
      new Header(
        formatId = formatID,
        maxProbe = state.maxProbe,
        hit = state.hit,
        miss = state.miss
      )
  }

  case class Header(formatId: Int,
                    maxProbe: Int,
                    hit: Int,
                    miss: Int)

  /**
    * Number of bytes required to build a high probability index.
    */
  def optimalBytesRequired(keyCounts: Int,
                           largestValue: Int,
                           compensate: Int => Int): Int = {
    //number of bytes required for hash indexes. +1 to skip 0 empty markers.
    val bytesWithOutCompensation = keyCounts * Bytes.sizeOf(largestValue + 1)
    val bytesRequired =
      headerSize +
        (ByteSizeOf.int + 1) + //give it another 5 bytes incase the hash index is the last index. Since varints are written a max of 5 bytes can be taken for an in with large index.
        bytesWithOutCompensation +
        Try(compensate(bytesWithOutCompensation)).getOrElse(0) //optionally add compensation space or remove.

    //in case compensation returns negative, reserve enough bytes for the header.
    bytesRequired max headerSize
  }

  def writeHeader(state: State): IO[Slice[Byte]] =
    IO {
      //it's important to move to 0 to write to head of the file.
      state.bytes moveWritePosition 0
      state.bytes add formatID
      state.bytes addIntUnsigned state.maxProbe
      state.bytes addInt state.hit
      state.bytes addInt state.miss
      state.bytes addBoolean false //range indexing is not implemented.
    }

  def readHeader(offset: Offset, reader: Reader): IO[Header] = {
    val movedReader = reader.moveTo(offset.start)
    for {
      formatID <- movedReader.get()
      maxProbe <- movedReader.readIntUnsigned()
      hit <- movedReader.readInt()
      miss <- movedReader.readInt()
    } yield
      Header(
        formatId = formatID,
        maxProbe = maxProbe,
        hit = hit,
        miss = miss
      )
  }

  def adjustHash(hash: Int, totalBlockSpace: Int) =
  //add headerSize of offset the output index skipping header bytes.
  //similar to optimalBytesRequired adding 5 bytes to add space for last indexes, here we remove those bytes to
  //generate indexes accounting for the last index being the largest integer with 5 bytes.
    ((hash & Int.MaxValue) % (totalBlockSpace - (ByteSizeOf.int + 1) - headerSize)) + headerSize

  /**
    * Mutates the slice and adds writes the indexOffset to it's hash index.
    */
  def write(key: Slice[Byte],
            value: Int,
            state: State): IO[Boolean] = {

    //add 1 to each offset to avoid 0 offsets.
    //0 bytes are reserved as empty bucket markers.
    val valuePlusOne = value + 1

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
        val index = adjustHash(hash1 + probe * hash2, state.bytes.size)
        val indexBytesRequired = Bytes.sizeOf(valuePlusOne)
        if (state.bytes.take(index, indexBytesRequired).forall(_ == 0)) {
          state.bytes moveWritePosition index
          state.bytes addIntUnsigned valuePlusOne
          state.hit += 1
          true
          //println(s"Key: ${key.readInt()}: write hashIndex: $index probe: $probe, sortedIndexOffset: $sortedIndexOffset, writeBytes: ${Slice.writeIntUnsigned(sortedIndexOffset)} = success")
        } else {
          //println(s"Key: ${key.readInt()}: write hashIndex: $index probe: $probe, sortedIndexOffset: $sortedIndexOffset, writeBytes: ${Slice.writeIntUnsigned(sortedIndexOffset)} = failure")
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
  def find[V](key: Slice[Byte],
              offset: Offset,
              header: Header,
              hashIndexReader: Reader,
              assertValue: Int => IO[Option[V]]): IO[Option[V]] = {

    val hash = key.##
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    @tailrec
    def doFind(probe: Int, checkedHashIndexes: mutable.HashSet[Int]): IO[Option[V]] =
      if (probe > header.maxProbe) {
        IO.none
      } else {
        val hashIndex = adjustHash(hash1 + probe * hash2, offset.size)
        if (checkedHashIndexes contains hashIndex) //do not check the same index again.
          doFind(probe + 1, checkedHashIndexes)
        else
          hashIndexReader.moveTo(offset.start + hashIndex).readIntUnsigned() match {
            case IO.Success(possibleValue) =>
              if (possibleValue == 0) {
                doFind(probe + 1, checkedHashIndexes)
              } else {
                //submit the indexOffset removing the add 1 offset to avoid overlapping bytes.
                //println(s"Key: ${key.readInt()}: read hashIndex: $hashIndex probe: $probe, sortedIndex: ${possibleSortedIndexOffset - 1} = reading now!")
                assertValue(possibleValue - 1) match {
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
