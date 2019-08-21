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

package swaydb.core.queue

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{KeyValue, Memory, Persistent}
import swaydb.core.io.file.BlockCache
import swaydb.core.queue.Command.{WeighedKeyValue, WeighKeyValue}
import swaydb.core.util.SkipList
import swaydb.data.slice.Slice

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.ref.WeakReference

private sealed trait Command
private object Command {

  sealed trait KeyValueCommand extends Command {
    val keyValueRef: WeakReference[KeyValue.CacheAble]
    val skipListRef: WeakReference[SkipList[Slice[Byte], _]]
  }

  case class WeighKeyValue(keyValueRef: WeakReference[Persistent.SegmentResponse],
                           skipListRef: WeakReference[SkipList[Slice[Byte], _]]) extends KeyValueCommand

  case class WeighedKeyValue(keyValueRef: WeakReference[KeyValue.ReadOnly.Group],
                             skipListRef: WeakReference[SkipList[Slice[Byte], _]],
                             weight: Int) extends KeyValueCommand

  case class Block(key: Long, blockCache: BlockCache.State) extends Command
}

private[core] object MemorySweeper {

  def apply(cacheSize: Long, delay: FiniteDuration)(implicit ex: ExecutionContext): MemorySweeper =
    new MemorySweeperImpl(
      cacheSize = cacheSize,
      delay = delay
    )

  val none: Option[MemorySweeper] = None

  def keyValueWeigher(entry: Command): Long =
    entry match {
      case _: Command.Block =>
        1

      case WeighKeyValue(keyValue, _) =>
        keyValue.get map {
          keyValue =>
            MemorySweeper.weight(keyValue)
        } getOrElse 264L //264L for the weight of WeakReference itself.

      case custom: WeighedKeyValue =>
        custom.weight
    }

  def weight(keyValue: Persistent.SegmentResponse) = {
    val otherBytes = (Math.ceil(keyValue.key.size + keyValue.valueLength / 8.0) - 1.0) * 8
    //        if (keyValue.hasRemoveMayBe) (168 + otherBytes).toLong else (264 + otherBytes).toLong
    ((264 * 2) + otherBytes).toLong
  }

  def processCommand(command: Command)(implicit limiter: MemorySweeper) =
    command match {
      case Command.Block(key, blockCache) =>
        blockCache remove key

      case command: Command.KeyValueCommand =>
        for {
          skipList <- command.skipListRef.get
          keyValue <- command.keyValueRef.get
        } yield {
          keyValue match {
            case group: KeyValue.ReadOnly.Group =>

              /**
               * Before removing Group, check if removes cache key-values it is enough,
               * if it's already clear only then remove.
               */
              if (!group.isBlockCacheEmpty) {
                group.clearBlockCache()
                limiter.add(group, skipList)
              } else if (!group.isKeyValuesCacheEmpty) {
                group.clearCachedKeyValues()
                limiter.add(group, skipList)
              } else {
                group match {
                  case group: Memory.Group =>
                    //Memory.Group key-values are only uncompressed. DO NOT REMOVE THEM!
                    skipList.asInstanceOf[SkipList[Slice[Byte], Memory]].put(group.key, group.uncompress())

                  case group: Persistent.Group =>
                    skipList remove group.key
                }
              }

            case _: Persistent.SegmentResponse =>
              skipList remove keyValue.key
          }
        }
    }
}

private[core] sealed trait MemorySweeper {
  def add(keyValue: Persistent.SegmentResponse,
          skipList: SkipList[Slice[Byte], _]): Unit

  def add(keyValue: KeyValue.ReadOnly.Group,
          skipList: SkipList[Slice[Byte], _]): Unit

  def add(key: Long,
          blockCache: BlockCache.State): Unit

  def terminate(): Unit
}

private class MemorySweeperImpl(cacheSize: Long,
                                delay: FiniteDuration)(implicit ex: ExecutionContext) extends LazyLogging with MemorySweeper {

  implicit val self: MemorySweeperImpl = this

  /**
   * Lazy initialisation because this queue is not require for Memory database that do not use compression.
   */
  private lazy val queue =
    LimitQueue[Command](
      maxWeight = cacheSize,
      delay = delay,
      weigher = MemorySweeper.keyValueWeigher
    )(MemorySweeper.processCommand)

  def add(keyValue: Persistent.SegmentResponse,
          skipList: SkipList[Slice[Byte], _]): Unit =
    queue ! Command.WeighKeyValue(new WeakReference(keyValue), new WeakReference[SkipList[Slice[Byte], _]](skipList))

  /**
   * If there was failure reading the Group's header guess it's weight. Successful reads are priority over 100% cache's accuracy.
   * The cache's will eventually adjust to be accurate but until then guessed weights should be used. The accuracy of guessed
   * weights can also be used.
   */
  def add(keyValue: KeyValue.ReadOnly.Group,
          skipList: SkipList[Slice[Byte], _]): Unit = {
    val weight = keyValue.valueLength
    queue ! Command.WeighedKeyValue(new WeakReference(keyValue), new WeakReference[SkipList[Slice[Byte], _]](skipList), weight)
  }

  def add(key: Long,
          blockCache: BlockCache.State): Unit =
    queue ! Command.Block(key, blockCache)

  def terminate() =
    queue.terminate()
}
