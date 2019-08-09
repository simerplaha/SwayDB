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

package swaydb.core.segment.format.a.block.reader

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ErrorHandler
import swaydb.{Error, IO}
import swaydb.core.segment.format.a.block.BlockOffset
import swaydb.data.slice.{Reader, Slice}

private[reader] object BlockReader extends LazyLogging {

  def apply(offset: BlockOffset,
            blockSize: Int,
            reader: Reader[swaydb.Error.Segment]): BlockReader.State =
    new State(
      offset = offset,
      blockSize = blockSize,
      position = 0,
      previousReadEndPosition = 0,
      isSequentialRead = true,
      cache = BlockReaderCache.init(0, Slice.emptyBytes),
      reader = reader
    )

  class State(val offset: BlockOffset,
              val blockSize: Int,
              val cache: BlockReaderCache.State,
              val reader: Reader[swaydb.Error.Segment],
              var isSequentialRead: Boolean, //var that stores the guess if the previous read was sequential.
              var position: Int,
              var previousReadEndPosition: Int) {

    val isFile = reader.isFile

    val size = offset.size

    def updatePreviousEndPosition(): Unit =
      previousReadEndPosition = position - 1

    def remaining: Int =
      size - position

    def moveTo(position: Int) =
      this.position = position

    def cachedBytes =
      cache.bytes

    def hasMore: Boolean =
      hasAtLeast(1)

    def hasAtLeast(atLeastSize: Long): Boolean =
      hasAtLeast(position, atLeastSize)

    def hasAtLeast(fromPosition: Long, atLeastSize: Long): Boolean =
      (size - fromPosition) >= atLeastSize
  }

  def readFromCache(size: Int, state: State): Slice[Byte] =
    BlockReaderCache.read(
      position = state.position,
      size = size,
      state = state.cache
    )

  def isSequentialRead(fromCache: Slice[Byte], state: State): Boolean = {
    val isSeq =
      fromCache.nonEmpty || //if there was data fetch from cache
        (state.cache.toOffset + 1) == state.position || { //if position is the start of the next block.
        state.isFile && {
          state.previousReadEndPosition == 0 || //if this is the initial read.
            state.position - state.previousReadEndPosition == 0 //if position is continuation of previous read's end position
        }
      }
    state.isSequentialRead = isSeq
    isSeq
  }

  def isSequentialRead(size: Int, state: State): Boolean =
    isSequentialRead(
      fromCache = readFromCache(size, state),
      state = state
    )

  def readRandom(size: Int, state: State): IO[Error.Segment, Slice[Byte]] = {
    val remaining = state.remaining
    if (remaining <= 0)
      IO.emptyBytes
    else
      state
        .reader
        .moveTo(state.offset.start + state.position)
        .read(size min remaining)
        .map {
          bytes =>
            val actualSize = size min remaining
            state.position += actualSize
            state.updatePreviousEndPosition()
            bytes
        }
  }

  def readSequential(size: Int, fromCache: Slice[Byte], state: State): IO[Error.Segment, Slice[Byte]] =
    if (size <= fromCache.size) {
      IO {
        logger.debug(s"${this.getClass.getName} #${this.hashCode()}: Path: ${state.reader.path}, Offset: Seek from cache: ${fromCache.size}.bytes")
        state.position += size
        fromCache take size
      }
    } else {
      val remaining = state.remaining
      if (remaining <= 0) {
        IO.emptyBytes
      } else {
        //if reads are random do not read full block size lets the reads below decide how much to read.
        //read full block on random reads is very slow.
        //adjust the seek size to be a multiple of blockSize.
        //also check if there are too many cache misses.
        val blockSizeToRead =
        if (state.blockSize <= 0)
          size
        else
          state.blockSize.toDouble * Math.ceil(Math.abs((size - fromCache.size) / state.blockSize.toDouble))
        //read the blockSize if there are enough bytes or else only read only the remaining.
        val actualBlockReadSize = blockSizeToRead.toInt min (remaining - fromCache.size)
        //skip bytes already read from the blockCache.
        val nextBlockReadPosition = state.offset.start + state.position + fromCache.size

        state
          .reader
          .moveTo(nextBlockReadPosition)
          .read(actualBlockReadSize)
          .map {
            bytes =>

              /**
               * [[size]] can be larger than [[blockSize]]. If the seeks are smaller than [[blockSize]]
               * then cache the entire bytes since it's known that a minimum of [[blockSize]] is allowed to be cached.
               * If seeks are too large then cache only the extra tail bytes which are currently un-read by the client.
               */
              if (state.blockSize > 0) {
                logger.debug(s"${this.getClass.getName} #${this.hashCode()}: Path: ${state.reader.path}, ${state.offset.getClass.getSimpleName}: ${nextBlockReadPosition} Seek from disk: ${bytes.size}.bytes.")
                if (bytes.size <= state.blockSize)
                  BlockReaderCache.set(nextBlockReadPosition - state.offset.start, bytes, state.cache)
                else
                  BlockReaderCache.set(nextBlockReadPosition - state.offset.start + size, bytes.drop(size).unslice(), state.cache)
              }

              val actualSize = size min remaining
              state.position += actualSize

              state.updatePreviousEndPosition()

              if (fromCache.isEmpty)
                bytes take size
              else
                fromCache ++ bytes.take(actualSize - fromCache.size)
          }
      }
    }

  def get(state: State): IO[swaydb.Error.Segment, Int] =
    if (state.isFile)
      read(1, state).map(_.head)
    else if (state.hasMore)
      state.
        reader
        .moveTo(state.offset.start + state.position)
        .get()
        .map {
          byte =>
            state.position += 1
            byte
        }
    else
      IO.Failure(swaydb.Error.Unknown(s"Has no more bytes. Position: ${state.position}"))

  def read(size: Int, state: State): IO[swaydb.Error.Segment, Slice[Byte]] = {
    val fromCache = readFromCache(size, state)
    if (isSequentialRead(fromCache, state))
      readSequential(size, fromCache, state)
    else
      readRandom(size, state)
  }

  def readFullBlock(state: State): IO[swaydb.Error.Segment, Slice[Byte]] =
    state.
      reader
      .moveTo(state.offset.start)
      .read(state.offset.size)
}
