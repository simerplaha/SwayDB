/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.io.file

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.actor.MemorySweeper
import swaydb.core.util.HashedMap
import swaydb.data.slice.{Slice, SliceOption}

import scala.annotation.tailrec

/**
 * Stores all the read bytes given the configured disk blockSize.
 */
private[core] object BlockCache extends LazyLogging {

  //  var diskSeeks = 0
  //  var memorySeeks = 0
  //  var splitsCount = 0

  def init(memorySweeper: MemorySweeper): Option[BlockCache.State] =
    memorySweeper match {
      case MemorySweeper.Off =>
        None

      case enabled: MemorySweeper.On =>
        enabled match {
          case block: MemorySweeper.Block =>
            Some(BlockCache.fromBlock(block))

          case _: MemorySweeper.KeyValueSweeper =>
            None
        }
    }

  def init(memorySweeper: Option[MemorySweeper.Block]): Option[BlockCache.State] =
    memorySweeper.map(fromBlock)

  def fromBlock(memorySweeper: MemorySweeper.Block): BlockCache.State =
    new State(
      blockSize = memorySweeper.blockSize,
      sweeper = memorySweeper,
      skipBlockCacheSeekSize = memorySweeper.skipBlockCacheSeekSize,
      map =
        HashedMap.concurrent[Long, SliceOption[Byte], Slice[Byte]](
          nullValue = Slice.Null,
          initialCapacity = Some(memorySweeper.cacheSize / memorySweeper.blockSize)
        )
    )

  class State(val blockSize: Int,
              val skipBlockCacheSeekSize: Int,
              val sweeper: MemorySweeper.Block,
              val map: HashedMap.Concurrent[Long, SliceOption[Byte], Slice[Byte]]) {
    val blockSizeDouble: Double = blockSize

    def clear() =
      map.clear()

    def remove(key: Long) =
      map remove key
  }

  def close(blockCache: Option[BlockCache.State]): Unit =
    blockCache.foreach(close)

  def close(blockCache: BlockCache.State): Unit = {
    logger.info("Cleared BlockCache!")
    blockCache.clear()
    blockCache.sweeper.terminateAndClear()
  }

  def seekSize(lowerFilePosition: Int,
               size: Int,
               source: BlockCacheSource,
               state: State): Int = {
    val sourceSize = source.blockCacheMaxBytes
    val seekSize =
      if (state.blockSize <= 0)
        size
      else
        (state.blockSizeDouble * Math.ceil(Math.abs(size / state.blockSizeDouble))).toInt

    ((sourceSize.toInt - lowerFilePosition) min seekSize) max 0
  }

  def seekPosition(position: Int, state: State): Int =
    if (state.blockSize <= 0)
      position
    else
      (state.blockSizeDouble * Math.floor(Math.abs(position / state.blockSizeDouble))).toInt

  sealed trait BlockIO {
    def seek(keyPosition: Int,
             size: Int,
             source: BlockCacheSource,
             state: State): Slice[Byte]
  }

  implicit object BlockIO extends BlockIO {
    def seek(keyPosition: Int,
             size: Int,
             source: BlockCacheSource,
             state: State): Slice[Byte] = {
      val seekedSize =
        seekSize(
          lowerFilePosition = keyPosition,
          size = size,
          source = source,
          state = state
        )

      val bytes =
        source
          .readFromSource(
            position = keyPosition,
            size = seekedSize
          )

      //      diskSeeks += 1
      if (state.blockSize <= 0) {
        bytes
      } else if (bytes.isEmpty) {
        Slice.emptyBytes
      } else if (bytes.size <= state.blockSize) {
        val value = bytes.unslice()
        state.map.put(keyPosition, value)
        state.sweeper.add(keyPosition, value, state.map)
        bytes
      } else {
        //        splitsCount += 1
        var index = 0
        var position = keyPosition
        val splits = Math.ceil(bytes.size / state.blockSizeDouble)
        while (index < splits) {
          val bytesToPut = bytes.take(index * state.blockSize, state.blockSize)
          state.map.put(position, bytesToPut)
          state.sweeper.add(position, bytesToPut, state.map)
          position = position + bytesToPut.size
          index += 1
        }
        bytes
      }
    }
  }

  /**
   * TODO - create an array of size of n bytes and append to it instead of ++
   */
  @tailrec
  private def getOrSeek(position: Int,
                        size: Int,
                        headBytes: Slice[Byte],
                        source: BlockCacheSource,
                        state: State)(implicit blockIO: BlockIO): Slice[Byte] = {
    val keyPosition = seekPosition(position, state)
    state.map.get(keyPosition) match {
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
            source = source,
            state = state
          )(blockIO)

      case Slice.Null =>
        //        println(s"Disk seek size: $size")
        val seekedBytes =
          blockIO.seek(
            keyPosition = keyPosition,
            source = source,
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
                source: BlockCacheSource,
                state: State)(implicit effect: BlockIO): Slice[Byte] =
    if (size >= state.skipBlockCacheSeekSize) //if the seek size is too large then skip block cache and perform direct IO.
      source
        .readFromSource(
          position = position,
          size = size
        )
    else
      getOrSeek(
        position = position,
        size = size,
        source = source,
        headBytes = null,
        state = state
      )(effect)
}
