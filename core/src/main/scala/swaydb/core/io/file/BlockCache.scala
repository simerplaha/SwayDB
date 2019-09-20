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

import swaydb.Error.IO.ExceptionHandler
import swaydb.core.actor.MemorySweeper
import swaydb.core.util.HashedMap
import swaydb.data.slice.Slice
import swaydb.{Error, IO}

import scala.annotation.tailrec

private[core] object BlockCache {

  class Key(val fileId: Long, val position: Int) {
    override def equals(that: Any): Boolean =
      that match {
        case other: Key =>
          this.fileId == other.fileId &&
            this.position == other.position

        case _ =>
          false
      }

    override def hashCode(): Int = {
      val code = fileId + position
      (code ^ (code >>> 32)).toInt
    }
  }

  def init(memorySweeper: MemorySweeper): Option[BlockCache.State] =
    memorySweeper match {
      case MemorySweeper.Disabled =>
        None

      case enabled: MemorySweeper.Enabled =>
        enabled match {
          case block: MemorySweeper.BlockSweeper =>
            Some(BlockCache.init(block))

          case _: MemorySweeper.KeyValueSweeper =>
            None

          case both: MemorySweeper.Both =>
            Some(BlockCache.init(both))
        }
    }

  def init(memorySweeper: MemorySweeper.BlockSweeper) =
    new State(
      blockSize = memorySweeper.blockSize,
      sweeper = memorySweeper,
      map = HashedMap.concurrent[BlockCache.Key, Slice[Byte]]()
    )

  def init(memorySweeper: MemorySweeper.Both) =
    new State(
      blockSize = memorySweeper.blockSize,
      sweeper = memorySweeper,
      map = HashedMap.concurrent[BlockCache.Key, Slice[Byte]]()
    )

  class State(val blockSize: Int,
              val sweeper: MemorySweeper.Block,
              private[BlockCache] val map: HashedMap.Concurrent[BlockCache.Key, Slice[Byte]]) {
    val blockSizeDouble: Double = blockSize

    def clear() =
      map.clear()

    def remove(key: BlockCache.Key) =
      map remove key
  }

  def seekSize(keyPosition: Int,
               size: Int,
               file: DBFileType,
               state: State): IO[Error.IO, Int] =
    file.fileSize map {
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

  sealed trait BlockIO {
    def seek(keyPosition: Int,
             size: Int,
             file: DBFileType,
             state: State): IO[Error.IO, Slice[Byte]]
  }

  implicit object BlockIO extends BlockIO {
    def seek(keyPosition: Int,
             size: Int,
             file: DBFileType,
             state: State): IO[Error.IO, Slice[Byte]] =
      seekSize(
        keyPosition = keyPosition,
        size = size,
        file = file,
        state = state
      ) flatMap {
        seekSize =>
          file
            .read(
              position = keyPosition,
              size = seekSize
            )
            .flatMap {
              bytes =>
                if (state.blockSize <= 0) {
                  IO.Right(bytes)
                } else if (bytes.isEmpty) {
                  IO.emptyBytes
                } else if (bytes.size <= state.blockSize) {
                  val key = new Key(file.blockCacheFileId, keyPosition)
                  val value = bytes.unslice()
                  state.map.put(key, value)
                  state.sweeper.add(key, value, state.map)
                  IO.Right(bytes)
                } else {
                  var index = 0
                  var position = keyPosition
                  val splits = Math.ceil(bytes.size / state.blockSizeDouble)
                  while (index < splits) {
                    val bytesToPut = bytes.take(index * state.blockSize, state.blockSize)
                    val key = new Key(file.blockCacheFileId, position)
                    state.map.put(key, bytesToPut)
                    state.sweeper.add(key, bytesToPut, state.map)
                    position = position + bytesToPut.size
                    index += 1
                  }
                  IO.Right(bytes)
                }
            }
      }
  }

  @tailrec
  private def getOrSeek(position: Int,
                        size: Int,
                        headBytes: Slice[Byte],
                        file: DBFileType,
                        state: State)(implicit blockIO: BlockIO): IO[Error.IO, Slice[Byte]] = {
    val keyPosition = seekPosition(position, state)

    state.map.get(new Key(file.blockCacheFileId, keyPosition)) match {
      case Some(fromCache) =>
        val seekedBytes = fromCache.take(position - keyPosition, size)

        val mergedBytes =
          if (headBytes.isEmpty)
            seekedBytes
          else
            headBytes ++ seekedBytes

        if (seekedBytes.isEmpty || seekedBytes.size == size)
          IO.Right(mergedBytes)
        else
          getOrSeek(
            position = position + seekedBytes.size,
            size = size - seekedBytes.size,
            headBytes = mergedBytes,
            file = file,
            state = state
          )(blockIO)

      case None =>
        blockIO.seek(
          keyPosition = keyPosition,
          file = file,
          size = position - keyPosition + size,
          state = state
        ) flatMap {
          seekedBytes =>
            val bytesToReturn = seekedBytes.take(position - keyPosition, size)

            if (headBytes.isEmpty)
              IO.Right(bytesToReturn)
            else
              IO.Right(headBytes ++ bytesToReturn)
        }
    }
  }

  def getOrSeek(position: Int,
                size: Int,
                file: DBFileType,
                state: State)(implicit effect: BlockIO): IO[Error.IO, Slice[Byte]] =
    getOrSeek(
      position = position,
      size = size,
      file = file,
      headBytes = Slice.emptyBytes,
      state = state
    )(effect)
}
