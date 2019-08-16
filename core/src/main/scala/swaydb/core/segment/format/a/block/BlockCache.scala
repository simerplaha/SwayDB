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

package swaydb.core.segment.format.a.block

import swaydb.core.io.file.DBFile
import swaydb.data.slice.Slice
import swaydb.{Error, IO}

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import swaydb.Error.Segment.ErrorHandler

object BlockCache {

  class State(val file: DBFile,
              val blockSize: Int,
              val map: TrieMap[Int, Slice[Byte]])

  def seekSize(size: Int, blockSize: Int): Int = {
    val doubleBlockSize: Double = blockSize
    (doubleBlockSize * Math.ceil(Math.abs(size / doubleBlockSize))).toInt
  }

  private def readAndCache(size: Int, position: Int, state: State): IO[Error.Segment, Slice[Byte]] =
    state
      .file
      .read(position = position, size = seekSize(size, state.blockSize))
      .map {
        bytes =>
          var index = 1
          bytes.groupedSlice(state.blockSize) foreach {
            bytes =>
              state.map.put(position * index, bytes.unslice())
              index += 1
          }
          bytes
      }

  @tailrec
  private[block] def doSeek(position: Int,
                            size: Int,
                            bytes: Slice[Byte],
                            state: State): IO[Error.Segment, Slice[Byte]] = {
    val keyPosition = position / state.blockSize

    state.map.get(keyPosition) match {
      case Some(fromCache) =>
        val cachedBytes = fromCache.take(keyPosition + position - 1, size)
        val mergedBytes =
          if (bytes.isEmpty)
            cachedBytes
          else
            bytes ++ cachedBytes

        if (cachedBytes.size == size)
          IO.Success(mergedBytes)
        else
          doSeek(
            position = keyPosition + state.blockSize,
            size = size - cachedBytes.size,
            bytes = mergedBytes,
            state = state
          )


      case None =>
        readAndCache(
          position = keyPosition,
          size = size,
          state = state
        ) match {
          case IO.Success(seekedBytes) =>
            if (bytes.isEmpty)
              IO.Success(seekedBytes.take(size))
            else
              IO.Success(bytes ++ seekedBytes.take(size))


          case IO.Failure(error) =>
            IO.Failure(error)
        }
    }
  }

  def getOrSeek(position: Int,
                size: Int,
                state: State): IO[Error.Segment, Slice[Byte]] =
    doSeek(
      position = position,
      size = size,
      bytes = Slice.emptyBytes,
      state = state
    )
}
