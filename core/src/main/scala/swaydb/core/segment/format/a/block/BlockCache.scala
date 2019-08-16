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

  def init(file: DBFile,
           blockSize: Int) =
    new State(
      file = file,
      blockSize = blockSize,
      map = new TrieMap[Int, Slice[Byte]]()
    )

  class State(val file: DBFile,
              val blockSize: Int,
              val map: TrieMap[Int, Slice[Byte]]) {
    def clear() =
      map.clear()

    val blockSizeDouble: Double = blockSize
  }

  def seekSize(keyPosition: Int, size: Int, state: State): IO[Error.IO, Int] =
    state.file.fileSize map {
      fileSize =>
        val seekSize = (state.blockSizeDouble * Math.ceil(Math.abs(size / state.blockSizeDouble))).toInt
        (fileSize.toInt - keyPosition) min seekSize
    }

  def seekPosition(position: Int, state: State): Int =
    (state.blockSizeDouble * Math.ceil(Math.abs(position / state.blockSizeDouble))).toInt

  sealed trait IOEffect {
    def readAndCache(keyPosition: Int, size: Int, state: State): IO[Error.Segment, Slice[Byte]]
  }

  implicit object IOEffect extends IOEffect {
    def readAndCache(keyPosition: Int, size: Int, state: State): IO[Error.Segment, Slice[Byte]] =
      seekSize(
        keyPosition = keyPosition,
        size = size,
        state = state
      ) flatMap {
        seekSize =>
          state
            .file
            .read(
              position = keyPosition,
              size = seekSize
            )
            .map {
              bytes =>
                var index = 1
                bytes.groupedSlice(bytes.size / state.blockSize) foreach {
                  bytes =>
                    state.map.put(keyPosition * index, bytes.unslice())
                    index += 1
                }
                bytes.take(size)
            }
      }
  }

  @tailrec
  private[block] def doSeek(position: Int,
                            size: Int,
                            bytes: Slice[Byte],
                            state: State)(implicit effect: IOEffect): IO[Error.Segment, Slice[Byte]] = {
    val keyPosition = seekPosition(position, state)

    state.map.get(keyPosition) match {
      case Some(fromCache) =>
        val cachedBytes = fromCache.take(position - keyPosition, size)
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
          )(effect)


      case None =>
        effect.readAndCache(
          keyPosition = keyPosition,
          size = size,
          state = state
        ) match {
          case IO.Success(seekedBytes) =>
            if (bytes.isEmpty)
              IO.Success(seekedBytes)
            else
              IO.Success(bytes ++ seekedBytes)


          case IO.Failure(error) =>
            IO.Failure(error)
        }
    }
  }

  def getOrSeek(position: Int,
                size: Int,
                state: State)(implicit effect: IOEffect): IO[Error.Segment, Slice[Byte]] =
    doSeek(
      position = position,
      size = size,
      bytes = Slice.emptyBytes,
      state = state
    )(effect)
}
