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
import swaydb.data.IO._
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec
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

  object WriteResult {
    val empty = WriteResult(0, 0, 0, Slice.emptyBytes)
    val emptyIO = IO.Success(empty)
  }
  final case class WriteResult(var hit: Int,
                               var miss: Int,
                               maxProbe: Int,
                               bytes: Slice[Byte])

  case class Header(formatId: Int,
                    maxProbe: Int,
                    hit: Int,
                    miss: Int,
                    rangeIndexingEnabled: Boolean)

  /**
    * Number of bytes required to build a high probability index.
    */
  def optimalBytesRequired(lastKeyValuePosition: Int,
                           lastKeyValueIndexOffset: Int,
                           minimumNumberOfKeyValues: Int,
                           compensate: Int => Int): Int = {
    if (lastKeyValuePosition <= minimumNumberOfKeyValues) {
      headerSize
    } else {
      //number of bytes required for hash indexes. +1 to skip 0 empty markers.
      val bytesWithOutCompensation = lastKeyValuePosition * Bytes.sizeOf(lastKeyValueIndexOffset + 1)
      val bytesRequired =
        headerSize +
          (ByteSizeOf.int + 1) + //give it another 5 bytes incase the hash index is the last index. Since varints are written a max of 5 bytes can be taken for an in with large index.
          bytesWithOutCompensation +
          Try(compensate(bytesWithOutCompensation)).getOrElse(0) //optionally add compensation space or remove.

      //in case compensation returns negative, reserve enough bytes for the header.
      bytesRequired max headerSize
    }
  }

  /**
    * Number of bytes required to build a high probability index.
    */
  def optimalBytesRequired(lastKeyValue: KeyValue.WriteOnly,
                           minimumNumberOfKeyValues: Int,
                           compensate: Int => Int): Int =
    optimalBytesRequired(
      lastKeyValuePosition = lastKeyValue.stats.position,
      minimumNumberOfKeyValues = minimumNumberOfKeyValues,
      lastKeyValueIndexOffset = lastKeyValue.stats.thisKeyValuesHashIndexesSortedIndexOffset,
      compensate = compensate
    )

  def write(keyValues: Iterable[KeyValue.WriteOnly],
            maxProbe: Int,
            minimumNumberOfKeyValues: Int,
            compensate: Int => Int): IO[WriteResult] =
    keyValues.lastOption map {
      last =>
        write(
          keyValues = keyValues,
          bytes =
            Slice.create[Byte](
              optimalBytesRequired(
                lastKeyValue = last,
                minimumNumberOfKeyValues = minimumNumberOfKeyValues,
                compensate = compensate
              )
            ),
          maxProbe = maxProbe
        )
    } getOrElse {
      logger.warn("Hash index not created. Empty key-values submitted.")
      WriteResult.emptyIO
    }

  def write(keyValues: Iterable[KeyValue.WriteOnly],
            bytes: Slice[Byte],
            maxProbe: Int): IO[WriteResult] =
    if (bytes.size == 0)
      WriteResult.emptyIO
    else
      keyValues.foldLeftIO(WriteResult(0, 0, maxProbe, bytes)) {
        case (result, keyValue) =>
          write(
            key = keyValue.key,
            sortedIndexOffset = keyValue.stats.thisKeyValuesHashIndexesSortedIndexOffset,
            bytes = bytes,
            maxProbe = maxProbe
          ) map {
            added =>
              if (added)
                result.hit += 1
              else
                result.miss += 1
              result
          }
      } flatMap {
        writeResult =>
          //it's important to move to 0 to write to head of the file.
          writeHeader(writeResult, bytes) map {
            _ =>
              writeResult
          }
      }

  def writeHeader(writeResult: WriteResult, bytes: Slice[Byte]): IO[Slice[Byte]] =
    IO {
      //it's important to move to 0 to write to head of the file.
      bytes moveWritePositionUnsafe 0
      bytes add formatID
      bytes addIntUnsigned writeResult.maxProbe
      bytes addInt writeResult.hit
      bytes addInt writeResult.miss
      bytes addBoolean false //range indexing is not implemented.
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
                        probe: Int) = {
    if (totalBlockSpace <= headerSize)
      headerSize //if there are no bytes reserved for hashIndex, just return the next hashIndex to be an overflow.
    else {
      val hash = key.##
      val hash1 = hash >>> 32
      val hash2 = (hash << 32) >> 32
      val computedHash = hash1 + probe * hash2
      //create a hash with reserved header bytes removed.
      //add headerSize of offset the output index skipping header bytes.
      //similar to optimalBytesRequired adding 5 bytes to add space for last indexes, here we remove those bytes to
      //generate indexes accounting for the last index being the larget integer with 5 bytes..
      ((computedHash & Int.MaxValue) % (totalBlockSpace - (ByteSizeOf.int + 1) - headerSize)) + headerSize
    }
  }

  /**
    * Mutates the slice and adds writes the indexOffset to it's hash index.
    */
  def write(key: Slice[Byte],
            sortedIndexOffset: Int,
            bytes: Slice[Byte],
            maxProbe: Int): IO[Boolean] = {

    //add 1 to each offset to avoid 0 offsets.
    //0 bytes are reserved as empty bucket markers.
    val indexOffsetPlusOne = sortedIndexOffset + 1

    @tailrec
    def doWrite(probe: Int): IO[Boolean] =
      if (probe >= maxProbe) {
        //println(s"Key: ${key.readInt()}: write index: miss probe: $probe")
        IO.`false`
      } else {
        val index = generateHashIndex(key, bytes.size, probe)

        val indexBytesRequired = Bytes.sizeOf(indexOffsetPlusOne)
        if (index + indexBytesRequired >= bytes.size)
          IO.`false`
        else if (bytes.take(index, indexBytesRequired).forall(_ == 0))
          IO {
            bytes moveWritePositionUnsafe index
            bytes addIntUnsigned indexOffsetPlusOne
            //println(s"Key: ${key.readInt()}: write hashIndex: $index probe: $probe, sortedIndexOffset: $sortedIndexOffset, writeBytes: ${Slice.writeIntUnsigned(sortedIndexOffset)} = success")
            true
          }
        else {
          //println(s"Key: ${key.readInt()}: write hashIndex: $index probe: $probe, sortedIndexOffset: $sortedIndexOffset, writeBytes: ${Slice.writeIntUnsigned(sortedIndexOffset)} = failure")
          doWrite(probe = probe + 1)
        }
      }

    if (bytes.size == 0)
      IO.`false`
    else
      doWrite(0)
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
    def doFind(probe: Int): IO[Option[K]] =
      if (probe > maxProbe) {
        IO.none
      } else {
        val hashIndex = generateHashIndex(key, hashIndexSize, probe)
        hashIndexReader.moveTo(hashIndexStartOffset + hashIndex).readIntUnsigned() match {
          case IO.Success(possibleSortedIndexOffset) =>
            if (possibleSortedIndexOffset == 0)
              doFind(probe + 1)
            else
            //submit the indexOffset removing the add 1 offset to avoid overlapping bytes.
            //println(s"Key: ${key.readInt()}: read hashIndex: $hashIndex probe: $probe, sortedIndex: ${possibleSortedIndexOffset - 1} = reading now!")
              get(possibleSortedIndexOffset - 1) match {
                case success @ IO.Success(Some(_)) =>
                  //println(s"Key: ${key.readInt()}: read hashIndex: $hashIndex probe: $probe, sortedIndex: ${possibleSortedIndexOffset - 1} = success")
                  success

                case IO.Success(None) =>
                  //println(s"Key: ${key.readInt()}: read hashIndex: $hashIndex probe: $probe: sortedIndex: ${possibleSortedIndexOffset - 1} = not found")
                  doFind(probe + 1)

                case IO.Failure(error) =>
                  IO.Failure(error)
              }

          case IO.Failure(error) =>
            IO.Failure(error)
        }
      }

    doFind(0)
  }
}
