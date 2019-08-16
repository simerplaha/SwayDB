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

import swaydb.core.segment.format.a.block.BlockOffset
import swaydb.core.util.SkipList
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Reader, Slice}
import swaydb.{Error, IO}

object BlockCache extends App {

  implicit val order = KeyOrder(Ordering.Int)

  class State(val offset: BlockOffset,
              val blockSize: Int,
              val cache: BlockReaderCache.State,
              val reader: Reader[swaydb.Error.Segment],
              var isSequentialRead: Boolean, //var that stores the guess if the previous read was sequential.
              var position: Int,
              var previousReadEndPosition: Int,
              val skipList: SkipList.Concurrent[Int, Slice[Byte]]) {

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

  def nextOffset(size: Int, state: State): BlockOffset = {
    val blockSizeToRead =
      if (state.blockSize <= 0)
        size
      else
        state.blockSize.toDouble * Math.ceil(Math.abs(size / state.blockSize.toDouble))
    //read the blockSize if there are enough bytes or else only read only the remaining.
    val actualBlockReadSize = blockSizeToRead.toInt min state.remaining
    //skip bytes already read from the blockCache.
    val nextBlockReadPosition = state.offset.start + state.position
    BlockOffset(actualBlockReadSize, nextBlockReadPosition)
  }

  def readIO(size: Int, state: State): IO[Error.Segment, Slice[Byte]] = {
    val offset = nextOffset(size, state)
    state
      .reader
      .moveTo(offset.start)
      .read(offset.size)
      .map {
        bytes =>
          state.skipList.put(offset.start, bytes)
          bytes.take(size)
      }
  }

  def readMemory(size: Int, state: State) = {
    def doRead() = {
      state.skipList.floorKeyValue(state.position) match {
        case Some(floorBytes) =>

        case None =>
      }

      state.skipList.floorKeyValue(state.position) map {
        case (position, floorBytes) => {
          val bytes = floorBytes.drop(state.position - position).take(size)
          if (bytes.size == size)
            bytes
          else
            state.skipList.higher(state.position + bytes.size) map {
              higherBytes =>
                bytes ++ higherBytes.take(size - bytes.size)
            }
        }
      } getOrElse IO.none
    }
    ???
  }

  def read(size: Int,
           state: State) =
    state.skipList.floorKeyValue(state.position) map {
      case (position, cacheBytes) =>
        val bytes = cacheBytes.drop(state.position - position).take(size)
        if (bytes.size < size) {
          state.moveTo(state.position + cacheBytes.size)
          readIO(size - bytes.size, state) map {
            diskSeekBytes =>
              bytes ++ diskSeekBytes
          }
        }
        else
          ???
    } getOrElse {
      readIO(size, state)
    }
}
