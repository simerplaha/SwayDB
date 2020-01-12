/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

import swaydb.core.actor.MemorySweeper
import swaydb.core.util.HashedMap
import swaydb.data.slice.{Slice, SliceOption}

import scala.annotation.tailrec

private[core] object BlockCache {

  //  var diskSeeks = 0
  //  var memorySeeks = 0
  //  var splitsCount = 0

  final case class Key(fileId: Long, position: Int)

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

          case both: MemorySweeper.All =>
            Some(BlockCache.init(both))
        }
    }

  def init(memorySweeper: MemorySweeper.BlockSweeper) =
    new State(
      blockSize = memorySweeper.blockSize,
      sweeper = memorySweeper,
      skipBlockCacheSeekSize = memorySweeper.skipBlockCacheSeekSize,
      map = HashedMap.concurrent[BlockCache.Key, Slice[Byte], SliceOption[Byte]](Slice.Null)
    )

  def init(memorySweeper: MemorySweeper.All) =
    new State(
      blockSize = memorySweeper.blockSize,
      sweeper = memorySweeper,
      skipBlockCacheSeekSize = memorySweeper.skipBlockCacheSeekSize,
      map = HashedMap.concurrent[BlockCache.Key, Slice[Byte], SliceOption[Byte]](Slice.Null)
    )

  class State(val blockSize: Int,
              val skipBlockCacheSeekSize: Int,
              val sweeper: MemorySweeper.Block,
              private[file] val map: HashedMap.Concurrent[BlockCache.Key, Slice[Byte], SliceOption[Byte]]) {
    val blockSizeDouble: Double = blockSize

    def clear() =
      map.clear()

    def remove(key: BlockCache.Key) =
      map remove key
  }

  def seekSize(keyPosition: Int,
               size: Int,
               file: DBFileType,
               state: State): Int = {
    val fileSize = file.fileSize
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
             state: State): Slice[Byte]
  }

  implicit object BlockIO extends BlockIO {
    def seek(keyPosition: Int,
             size: Int,
             file: DBFileType,
             state: State): Slice[Byte] = {
      val seekedSize =
        seekSize(
          keyPosition = keyPosition,
          size = size,
          file = file,
          state = state
        )

      val bytes =
        file
          .read(
            position = keyPosition,
            size = seekedSize
          )

      //      diskSeeks += 1
      if (state.blockSize <= 0) {
        bytes
      } else if (bytes.isEmpty) {
        Slice.emptyBytes
      } else if (bytes.size <= state.blockSize) {
        val key = Key(file.blockCacheFileId, keyPosition)
        val value = bytes.unslice()
        state.map.put(key, value)
        state.sweeper.add(key, value, state.map)
        bytes
      } else {
        //        splitsCount += 1
        var index = 0
        var position = keyPosition
        val splits = Math.ceil(bytes.size / state.blockSizeDouble)
        while (index < splits) {
          val bytesToPut = bytes.take(index * state.blockSize, state.blockSize)
          val key = Key(file.blockCacheFileId, position)
          state.map.put(key, bytesToPut)
          state.sweeper.add(key, bytesToPut, state.map)
          position = position + bytesToPut.size
          index += 1
        }
        bytes
      }
    }
  }

  @tailrec
  private def getOrSeek(position: Int,
                        size: Int,
                        headBytes: Slice[Byte],
                        file: DBFileType,
                        state: State)(implicit blockIO: BlockIO): Slice[Byte] = {
    //TODO - create an array of size of n bytes and append to it instead of ++
    val keyPosition = seekPosition(position, state)
    state.map.get(Key(file.blockCacheFileId, keyPosition)) match {
      case fromCache: Slice[Byte] =>
        //        println(s"Memory seek size: $size")
        //        memorySeeks += 1
        val seekedBytes = fromCache.take(position - keyPosition, size)

        val mergedBytes =
          if (headBytes == null)
            seekedBytes
          else
            headBytes ++ seekedBytes

        if (seekedBytes.isEmpty || seekedBytes.size == size)
          mergedBytes
        else
          getOrSeek(
            position = position + seekedBytes.size,
            size = size - seekedBytes.size,
            headBytes = mergedBytes,
            file = file,
            state = state
          )(blockIO)

      case Slice.Null =>
        //        println(s"Disk seek size: $size")
        val seekedBytes =
          blockIO.seek(
            keyPosition = keyPosition,
            file = file,
            size = position - keyPosition + size,
            state = state
          )

        val bytesToReturn =
          seekedBytes.take(position - keyPosition, size)

        if (headBytes == null)
          bytesToReturn
        else
          headBytes ++ bytesToReturn
    }
  }

  def getOrSeek(position: Int,
                size: Int,
                file: DBFileType,
                state: State)(implicit effect: BlockIO): Slice[Byte] =
    if (size >= state.skipBlockCacheSeekSize) //if the seek size is too large then skip block cache and perform direct IO.
      file
        .read(
          position = position,
          size = size
        )
    else
      getOrSeek(
        position = position,
        size = size,
        file = file,
        headBytes = null,
        state = state
      )(effect)
}
