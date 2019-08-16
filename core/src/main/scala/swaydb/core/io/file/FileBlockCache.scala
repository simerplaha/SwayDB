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

package swaydb.core.io.file

import swaydb.data.slice.Slice
import swaydb.{Error, IO}

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import swaydb.Error.IO.ErrorHandler

private[file] object FileBlockCache {

  def init(file: DBFileType,
           blockSize: Int) =
    new State(
      file = file,
      blockSize = blockSize,
      map = TrieMap[Int, Slice[Byte]]()
    )

  class State(val file: DBFileType,
              val blockSize: Int,
              val map: TrieMap[Int, Slice[Byte]]) {
    def clear() =
      map.clear()

    val blockSizeDouble: Double = blockSize
  }

  def seekSize(keyPosition: Int, size: Int, state: State): IO[Error.IO, Int] =

    state.file.fileSize map {
      fileSize =>
        val seekSize =
          if (state.blockSize <= 0)
            size
          else
            (state.blockSizeDouble * Math.ceil(Math.abs(size / state.blockSizeDouble))).toInt
        ((fileSize.toInt - keyPosition) min seekSize) max 0
    }

  def seekPosition(position: Int, state: State): Int =
    if (state.blockSize <= 0)
      position
    else
      (state.blockSizeDouble * Math.floor(Math.abs(position / state.blockSizeDouble))).toInt

  sealed trait IOEffect {
    def readAndCache(keyPosition: Int, size: Int, state: State): IO[Error.IO, Slice[Byte]]
  }

  implicit object IOEffect extends IOEffect {
    def readAndCache(keyPosition: Int, size: Int, state: State): IO[Error.IO, Slice[Byte]] =
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
                if (state.blockSize <= 0) {
                  bytes
                } else if (bytes.isEmpty) {
                  Slice.emptyBytes
                } else if (bytes.size <= state.blockSize) {
                  state.map.put(keyPosition, bytes.unslice())
                  bytes
                } else {
                  var index = 0
                  var position = keyPosition
                  val splits = Math.ceil(bytes.size / state.blockSizeDouble)
                  while (index < splits) {
                    val bytesToPut = bytes.take(index * state.blockSize, state.blockSize)
                    state.map.put(position, bytesToPut)
                    position = position + bytesToPut.size
                    index += 1
                  }
                  bytes
                }
            }
      }
  }

  @tailrec
  private[file] def doSeek(position: Int,
                           size: Int,
                           bytes: Slice[Byte],
                           state: State)(implicit effect: IOEffect): IO[Error.IO, Slice[Byte]] = {
    val keyPosition = seekPosition(position, state)

    state.map.get(keyPosition) match {
      case Some(fromCache) =>
        val cachedBytes = fromCache.take(position - keyPosition, size)
        val mergedBytes =
          if (bytes.isEmpty)
            cachedBytes
          else
            bytes ++ cachedBytes

        if (cachedBytes.isEmpty || cachedBytes.size == size)
          IO.Success(mergedBytes)
        else
          doSeek(
            position = position + cachedBytes.size,
            size = size - cachedBytes.size,
            bytes = mergedBytes,
            state = state
          )(effect)


      case None =>
        effect.readAndCache(
          keyPosition = keyPosition,
          size = position - keyPosition + size,
          state = state
        ) match {
          case IO.Success(seekedBytes) =>
            val bytesToReturn = seekedBytes.take(position - keyPosition, size)
            if (bytes.isEmpty)
              IO.Success(bytesToReturn)
            else
              IO.Success(bytes ++ bytesToReturn)

          case IO.Failure(error) =>
            IO.Failure(error)
        }
    }
  }

  def getOrSeek(position: Int,
                size: Int,
                state: State)(implicit effect: IOEffect): IO[Error.IO, Slice[Byte]] =
    doSeek(
      position = position,
      size = size,
      bytes = Slice.emptyBytes,
      state = state
    )(effect)
}
