/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.block

import com.typesafe.scalalogging.LazyLogging
import swaydb.cache.Cache
import swaydb.core.sweeper.MemorySweeper
import swaydb.utils.HashedMap
import swaydb.slice.{Slice, SliceOption, SliceRO, Slices}
import swaydb.utils.Options

import java.util.concurrent.ConcurrentHashMap
import scala.annotation.tailrec

/**
 * Stores all the read bytes given the configured disk blockSize.
 */
private[core] object BlockCache extends LazyLogging {

  //  var diskSeeks = 0
  //  var memorySeeks = 0
  //  var splitsCount = 0

  def forSearch(maxCacheSizeOrZero: Int, memorySweeper: MemorySweeper): Option[BlockCacheState] =
    memorySweeper match {
      case MemorySweeper.Off =>
        None

      case enabled: MemorySweeper.On =>
        enabled match {
          case block: MemorySweeper.Block =>
            forSearch(maxCacheSizeOrZero = maxCacheSizeOrZero, blockSweeper = Some(block))

          case _: MemorySweeper.KeyValueSweeper =>
            None
        }
    }

  def forSearch(maxCacheSizeOrZero: Int, blockSweeper: Option[MemorySweeper.Block]): Option[BlockCacheState] =
    blockSweeper flatMap {
      sweeper =>
        if (sweeper.disableForSearchIO)
          None
        else
          Some(fromBlock(maxCacheSizeOrZero, sweeper))
    }

  /**
   * @param maxCacheSizeOrZero anything <= 0 sets to use the default initial size.
   */
  private def fromBlock(maxCacheSizeOrZero: Int, memorySweeper: MemorySweeper.Block): BlockCacheState = {
    val initialCapacityInt = {
      val longCapacity: Long =
        maxCacheSizeOrZero / memorySweeper.blockSize

      val intCapacity = longCapacity.toInt

      if (intCapacity < 0) {
        logger.warn(s"WARNING! Initial capacity for BlockCache's HashMap is invalid: $longCapacity.bytes. Using ${classOf[ConcurrentHashMap[_, _]].getSimpleName}'s default initial capacity.")
        None //Too many slots. This should not happen otherwise a single Segment could get a HashMap with very large initial size.
      } else if (intCapacity == 0) {
        Options.one //only one entry required.
      } else {
        Some(intCapacity)
      }
    }

    val map =
      Cache.noIO[Unit, HashedMap.Concurrent[Long, SliceOption[Byte], Slice[Byte]]](synchronised = true, stored = true, initial = None) {
        case (_, _) =>
          HashedMap.concurrent[Long, SliceOption[Byte], Slice[Byte]](
            nullValue = Slice.Null,
            initialCapacity = initialCapacityInt
          )
      }

    new BlockCacheState(
      blockSize = memorySweeper.blockSize,
      sweeper = memorySweeper,
      skipBlockCacheSeekSize = memorySweeper.skipBlockCacheSeekSize,
      mapCache = map
    )
  }

  def close(blockCache: Option[BlockCacheState]): Unit =
    blockCache.foreach(close)

  def close(blockCache: BlockCacheState): Unit = {
    logger.info("Cleared BlockCache!")
    blockCache.clear()
    blockCache.sweeper.terminateAndClear()
  }

  def seekSize(lowerFilePosition: Int,
               size: Int,
               source: BlockCacheSource,
               state: BlockCacheState): Int = {
    val sourceSize = source.blockCacheMaxBytes
    val seekSize =
      if (state.blockSize <= 0)
        size
      else
        (state.blockSizeDouble * Math.ceil(Math.abs(size / state.blockSizeDouble))).toInt

    ((sourceSize - lowerFilePosition) min seekSize) max 0
  }

  def seekPosition(position: Int, state: BlockCacheState): Int =
    if (state.blockSize <= 0)
      position
    else
      (state.blockSizeDouble * Math.floor(Math.abs(position / state.blockSizeDouble))).toInt

  /**
   * TODO - create an array of size of n bytes and append to it instead of ++
   */
  @tailrec
  private def getOrSeek(position: Int,
                        size: Int,
                        headBytes: SliceRO[Byte],
                        source: BlockCacheSource,
                        state: BlockCacheState,
                        blockCacheIO: BlockCacheIO): SliceRO[Byte] = {
    val keyPosition = seekPosition(position, state)
    state.mapCache.value(()).get(keyPosition) match {
      case fromCache: Slice[Byte] =>
        //        println(s"Memory seek size: $size")
        //        memorySeeks += 1
        val gotFromCache = fromCache.take(position - keyPosition, size)

        val mergedBytes =
          if (headBytes == null)
            gotFromCache
          else //head bytes exist. Merge headBytes and bytes from cache into Slices
            headBytes match {
              case headBytes: Slice[Byte] =>
                Slices(Array(headBytes, gotFromCache))

              case Slices(headBytes) =>
                Slices(headBytes :+ gotFromCache)
            }

        if (gotFromCache.isEmpty || gotFromCache.size == size)
          mergedBytes //got enough data to satisfy this request!
        else
          getOrSeek( //not enough. Keep reading!
            position = position + gotFromCache.size,
            size = size - gotFromCache.size,
            headBytes = mergedBytes,
            source = source,
            state = state,
            blockCacheIO = blockCacheIO
          )

      case Slice.Null => //not found in cache, go to disk.
        //        println(s"Disk seek size: $size")
        val diskBytes = //read from disk
          blockCacheIO.seek(
            keyPosition = keyPosition,
            size = position - keyPosition + size,
            source = source,
            state = state
          )

        val gotFromDisk = //take enough bytes to satisfy this read request.
          diskBytes.take(position - keyPosition, size)

        if (headBytes == null)
          gotFromDisk // head bytes are empty. Returns disk read bytes.
        else
          headBytes match { //head bytes exist so merge with disk read bytes and return.
            case headBytes: Slice[Byte] =>
              gotFromDisk match {
                case tailBytes: Slice[Byte] =>
                  Slices(Array(headBytes, tailBytes))

                case Slices(tailBytes) =>
                  Slices(headBytes +: tailBytes)
              }

            case Slices(headBytes) =>
              gotFromDisk match {
                case tailBytes: Slice[Byte] =>
                  Slices(headBytes :+ tailBytes)

                case Slices(tailBytes) =>
                  Slices(headBytes ++ tailBytes)
              }
          }
    }
  }

  def getOrSeek(position: Int,
                size: Int,
                source: BlockCacheSource,
                state: BlockCacheState)(implicit blockCacheIO: BlockCacheIO): Slice[Byte] =
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
        headBytes = null,
        source = source,
        state = state,
        blockCacheIO = blockCacheIO
      ).cut() //TODO - remove CUT and make this function return SliceRO
}
