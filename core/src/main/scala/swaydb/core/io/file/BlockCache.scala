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

  final case class Key(sourceId: Long, position: Int)

  def init(memorySweeper: MemorySweeper): Option[BlockCache.State] =
    memorySweeper match {
      case MemorySweeper.Off =>
        None

      case enabled: MemorySweeper.On =>
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
      map =
        HashedMap.concurrent[BlockCache.Key, SliceOption[Byte], Slice[Byte]](
          nullValue = Slice.Null,
          initialCapacity = Some(memorySweeper.cacheSize / memorySweeper.blockSize)
        )
    )

  def init(memorySweeper: MemorySweeper.All) =
    new State(
      blockSize = memorySweeper.blockSize,
      sweeper = memorySweeper,
      skipBlockCacheSeekSize = memorySweeper.skipBlockCacheSeekSize,
      map =
        HashedMap.concurrent[BlockCache.Key, SliceOption[Byte], Slice[Byte]](
          nullValue = Slice.Null,
          initialCapacity = Some(memorySweeper.cacheSize / memorySweeper.blockSize)
        )
    )

  class State(val blockSize: Int,
              val skipBlockCacheSeekSize: Int,
              val sweeper: MemorySweeper.Block,
              private[file] val map: HashedMap.Concurrent[BlockCache.Key, SliceOption[Byte], Slice[Byte]]) {
    val blockSizeDouble: Double = blockSize

    def clear() =
      map.clear()

    def remove(key: BlockCache.Key) =
      map remove key
  }

  def close(blockCache: Option[BlockCache.State]): Unit =
    blockCache.foreach(close)

  def close(blockCache: BlockCache.State): Unit = {
    logger.info("Cleared BlockCache!")
    blockCache.clear()
    blockCache.sweeper.terminateAndClear()
  }

  def seekSize(keyPosition: Int,
               size: Int,
               source: BlockCacheSource,
               state: State): Int = {
    val sourceSize = source.size
    val seekSize =
      if (state.blockSize <= 0)
        size
      else
        (state.blockSizeDouble * Math.ceil(Math.abs(size / state.blockSizeDouble))).toInt

    ((sourceSize.toInt - keyPosition) min seekSize) max 0
  }

  def seekPosition(position: Int, state: State): Int =
    if (state.blockSize <= 0)
      position
    else
      (state.blockSizeDouble * Math.floor(Math.abs(position / state.blockSizeDouble))).toInt

  sealed trait BlockIO {
    def seek(sourceId: Long,
             cachePosition: Int,
             keyPosition: Int,
             size: Int,
             source: BlockCacheSource,
             state: State): Slice[Byte]
  }

  implicit object BlockIO extends BlockIO {
    def seek(sourceId: Long,
             cachePosition: Int,
             keyPosition: Int,
             size: Int,
             source: BlockCacheSource,
             state: State): Slice[Byte] = {
      val seekedSize =
        seekSize(
          keyPosition = keyPosition,
          size = size,
          source = source,
          state = state
        )

      val bytes =
        source
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
        val key = Key(sourceId, cachePosition)
        val value = bytes.unslice()
        state.map.put(key, value)
        state.sweeper.add(key, value, state.map)
        bytes
      } else {
        //        splitsCount += 1
        var index = 0
        var currentKeyCachePosition = cachePosition
        val splits = Math.ceil(bytes.size / state.blockSizeDouble)
        while (index < splits) {
          val bytesToPut = bytes.take(index * state.blockSize, state.blockSize)
          val key = Key(sourceId, currentKeyCachePosition)
          state.map.put(key, bytesToPut)
          state.sweeper.add(key, bytesToPut, state.map)
          currentKeyCachePosition = currentKeyCachePosition + bytesToPut.size
          index += 1
        }
        bytes
      }
    }
  }

  /**
   * Fetched bytes from cache accounting for Segments being transferred from one
   * file to another.
   *
   * TODO - create an array of size of n bytes and append to it instead of ++
   *
   * @param sourceId     unique ID of a logical group of bytes eg: Segment.
   * @param paddingLeft  how much the same logical bytes have been moved
   *                     from position 0 to into another file. Eg: if SegmentA
   *                     was at position 10 and was moved into SegmentB (Many) at position 20
   *                     SegmentB should access the cache with paddingLeft set as 20 which
   *                     will allow SegmentB reading bytes already cached by SegmentA.
   * @param filePosition the actual position within the file.
   * @param size         the size of bytes to read.
   * @param headBytes    bytes that should be prepended to the final bytes.
   * @param source       where the bytes are read from.
   * @param state        cache state
   * @param blockIO      IO type.
   * @return final bytes.
   */
  @tailrec
  private def getOrSeek(sourceId: Long,
                        paddingLeft: Int,
                        filePosition: Int,
                        size: Int,
                        headBytes: Slice[Byte],
                        source: BlockCacheSource,
                        state: State)(implicit blockIO: BlockIO): Slice[Byte] = {
    val cachePosition = seekPosition(filePosition - paddingLeft, state)

    state.map.get(Key(sourceId, cachePosition)) match {
      case fromCache: Slice[Byte] =>
        //        println(s"Memory seek size: $size")
        //        memorySeeks += 1
        val seekedBytes = fromCache.take(filePosition - paddingLeft - cachePosition, size)

        val mergedBytes =
          if (headBytes == null)
            seekedBytes
          else
            headBytes ++ seekedBytes

        if (seekedBytes.isEmpty || seekedBytes.size == size)
          mergedBytes
        else
          getOrSeek(
            sourceId = sourceId,
            paddingLeft = paddingLeft,
            filePosition = filePosition + seekedBytes.size,
            size = size - seekedBytes.size,
            headBytes = mergedBytes,
            source = source,
            state = state
          )(blockIO)

      case Slice.Null =>
        val ioPosition = seekPosition(filePosition, state)

        //        println(s"Disk seek size: $size")
        val seekedBytes =
          blockIO.seek(
            sourceId = sourceId,
            cachePosition = cachePosition,
            keyPosition = ioPosition,
            source = source,
            size = filePosition - ioPosition + size,
            state = state
          )

        val bytesToReturn =
          seekedBytes.take(filePosition - ioPosition, size)

        if (headBytes == null)
          bytesToReturn
        else
          headBytes ++ bytesToReturn
    }
  }

  def getOrSeek(sourceId: Long,
                paddingLeft: Int,
                position: Int,
                size: Int,
                source: BlockCacheSource,
                state: State)(implicit effect: BlockIO): Slice[Byte] =
    if (size >= state.skipBlockCacheSeekSize) //if the seek size is too large then skip block cache and perform direct IO.
      source
        .read(
          position = position,
          size = size
        )
    else
      getOrSeek(
        sourceId = sourceId,
        paddingLeft = paddingLeft,
        filePosition = position,
        size = size,
        source = source,
        headBytes = null,
        state = state
      )(effect)
}
