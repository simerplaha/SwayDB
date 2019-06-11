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
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec

/**
  * HashIndex with linear probing.
  */
object SegmentHashIndex extends LazyLogging {

  val formatID: Byte = 0.toByte

  object WriteResult {
    val empty = WriteResult(0, 0, Slice.emptyBytes)
    val emptyIO = IO.Success(empty)
  }
  final case class WriteResult(var hit: Int,
                               var miss: Int,
                               bytes: Slice[Byte])

  /**
    * Number of bytes required to build a high probability index.
    */
  def optimalBytesRequired(keyValues: Iterable[KeyValue.WriteOnly],
                           compensate: Int): Int =
    (keyValues.last.stats.position * Bytes.sizeOf(keyValues.last.stats.thisKeyValuesIndexOffset)) + //number of bytes required for has indexes
      compensate //optionally add some more space or remove.

  def write(keyValues: Iterable[KeyValue.WriteOnly],
            probe: Int,
            compensate: Int): IO[WriteResult] =
    write(
      keyValues = keyValues,
      bytes = Slice.create[Byte](optimalBytesRequired(keyValues, compensate)),
      probe = probe
    )

  def write(keyValues: Iterable[KeyValue.WriteOnly],
            bytes: Slice[Byte],
            probe: Int): IO[WriteResult] =
    if (bytes.size == 0)
      WriteResult.emptyIO
    else
      keyValues.foldLeftIO(WriteResult(0, 0, bytes)) {
        case (result, keyValue) =>
          write(
            key = keyValue.key,
            //add 1 to each offset to avoid 0 offsets.
            //0 bytes indicate empty bucket and cannot actually be a value.
            indexOffset = keyValue.stats.thisKeyValuesIndexOffset + 1,
            bytes = bytes,
            maxProbe = probe
          ) map {
            added =>
              if (added)
                result.hit += 1
              else
                result.miss += 1
              result
          }
      }

  def hashIndex(key: Slice[Byte],
                hashIndexByteSize: Int,
                probe: Int) = {
    val hash = key.##
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32
    val computedHash = hash1 + probe * hash2
    (computedHash & Int.MaxValue) % ((hashIndexByteSize - ByteSizeOf.int) max hashIndexByteSize)
  }

  /**
    * Mutates the slice and adds writes the indexOffset to it's hash index.
    */
  def write(key: Slice[Byte],
            indexOffset: Int,
            bytes: Slice[Byte],
            maxProbe: Int): IO[Boolean] = {

    @tailrec
    def doWrite(probe: Int): IO[Boolean] =
      if (probe >= maxProbe) {
        IO.`false`
      } else {
        val index = hashIndex(key, bytes.size, probe)
        val indexBytesRequired = Bytes.sizeOf(indexOffset)
        if (index + indexBytesRequired >= bytes.toOffset)
          IO.`false`
        else if (bytes.take(index, indexBytesRequired).forall(_ == 0))
          IO {
            bytes moveTo index
            bytes addIntUnsigned indexOffset
            true
          }
        else
          doWrite(probe = probe + 1)
      }

    if (bytes.size == 0)
      IO.`false`
    else if (indexOffset == 0)
      IO.Failure(IO.Error.Fatal(new Exception("indexOffset cannot be zero."))) //0 is reserved to be for non-empty buckets.
    else
      doWrite(0)
  }

  /**
    * Finds a key in the hash index using linear probing.
    */
  def find[K <: KeyValue](key: Slice[Byte],
                          slice: Slice[Byte],
                          maxProbe: Int,
                          finder: Int => IO[Option[K]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[K]] = {
    import keyOrder._

    @tailrec
    def doFind(probe: Int): IO[Option[K]] =
      if (probe >= maxProbe) {
        IO.none
      } else {
        val index = hashIndex(key, slice.size, probe)
        slice.take(index, ByteSizeOf.int).readIntUnsigned() match {
          case IO.Success(possibleIndexOffset) =>
            //submit the indexOffset removing the add 1 offset to avoid overlapping bytes.
            finder(possibleIndexOffset - 1) match {
              case success @ IO.Success(foundMayBe) =>
                foundMayBe match {
                  case Some(keyValue) if keyValue.key equiv key =>
                    success

                  case Some(_) | None =>
                    doFind(probe + 1)
                }
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
