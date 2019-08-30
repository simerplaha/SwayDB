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

package swaydb.core.actor

import swaydb.{Actor, ActorRef}
import swaydb.core.actor.Command.WeighedKeyValue
import swaydb.core.data.{KeyValue, Memory, Persistent}
import swaydb.core.io.file.BlockCache
import swaydb.core.util.{HashedMap, SkipList}
import swaydb.data.config.{ActorConfig, MemoryCache}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.ref.WeakReference

private[core] sealed trait Command
private[core] object Command {

  sealed trait KeyValueCommand extends Command {
    val keyValueRef: WeakReference[KeyValue.CacheAble]
    val skipListRef: WeakReference[SkipList[Slice[Byte], _]]
  }

  case class WeighKeyValue(keyValueRef: WeakReference[Persistent.SegmentResponse],
                           skipListRef: WeakReference[SkipList[Slice[Byte], _]]) extends KeyValueCommand

  case class WeighedKeyValue(keyValueRef: WeakReference[KeyValue.ReadOnly.Group],
                             skipListRef: WeakReference[SkipList[Slice[Byte], _]],
                             weight: Int) extends KeyValueCommand

  case class Block(key: BlockCache.Key,
                   valueSize: Int,
                   map: HashedMap.Concurrent[BlockCache.Key, Slice[Byte]]) extends Command
}

private[core] sealed trait MemorySweeper

/**
 * Cleared all cached data. [[MemorySweeper]] is not required for Memory only databases
 * and zero databases.
 */
private[core] object MemorySweeper {

  def apply(memoryCache: MemoryCache): Option[MemorySweeper.Enabled] =
    memoryCache match {
      case MemoryCache.Disable =>
        None

      case block: MemoryCache.EnableBlockCache =>
        Some(
          MemorySweeper.BlockSweeper(
            blockSize = block.blockSize,
            cacheSize = block.capacity,
            actorConfig = block.actorConfig
          )
        )
      case MemoryCache.EnableKeyValueCache(capacity, actorConfig) =>
        Some(
          MemorySweeper.KeyValueSweeper(
            cacheSize = capacity,
            actorConfig = actorConfig
          )
        )

      case MemoryCache.EnableBoth(blockSize, capacity, actorConfig) =>
        Some(
          MemorySweeper.Both(
            blockSize = blockSize,
            cacheSize = capacity,
            actorConfig = actorConfig
          )
        )
    }

  def weigher(entry: Command): Int =
    entry match {
      case command: Command.Block =>
        ByteSizeOf.long + command.valueSize + 264

      case command: Command.WeighKeyValue =>
        command.keyValueRef.get map {
          keyValue =>
            MemorySweeper.weight(keyValue).toInt
        } getOrElse 264 //264 for the weight of WeakReference itself.

      case custom: WeighedKeyValue =>
        custom.weight
    }

  def weight(keyValue: Persistent.SegmentResponse) = {
    val otherBytes = (Math.ceil(keyValue.key.size + keyValue.valueLength / 8.0) - 1.0) * 8
    //        if (keyValue.hasRemoveMayBe) (168 + otherBytes).toLong else (264 + otherBytes).toLong
    (264 * 2) + otherBytes
  }

  protected sealed trait SweeperImplementation {
    def cacheSize: Int

    def actorConfig: ActorConfig

    /**
     * Lazy initialisation because this actor is not require for Memory database that do not use compression.
     */
    val actor: ActorRef[Command, Unit] =
      Actor.cacheFromConfig[Command](
        stashCapacity = cacheSize,
        config = actorConfig,
        weigher = MemorySweeper.weigher
      ) {
        case (Command.Block(key, _, map), _) =>
          map remove key

        case (command: Command.KeyValueCommand, self) =>
          for {
            skipList <- command.skipListRef.get
            keyValue <- command.keyValueRef.get
          } yield {
            keyValue match {
              case group: swaydb.core.data.KeyValue.ReadOnly.Group =>

                /**
                 * Before removing Group, check if removes cache key-values it is enough,
                 * if it's already clear only then remove.
                 */
                if (!group.isBlockCacheEmpty) {
                  group.clearBlockCache()
                  self ! Command.WeighedKeyValue(new WeakReference(group), new WeakReference[SkipList[Slice[Byte], _]](skipList), keyValue.valueLength)
                } else if (!group.isKeyValuesCacheEmpty) {
                  group.clearCachedKeyValues()
                  self ! Command.WeighedKeyValue(new WeakReference(group), new WeakReference[SkipList[Slice[Byte], _]](skipList), keyValue.valueLength)
                } else {
                  group match {
                    case group: Memory.Group =>
                      //Memory.Group key-values are only uncompressed. DO NOT REMOVE THEM!
                      val uncompressedGroup = group.uncompress()
                      skipList.asInstanceOf[SkipList[Slice[Byte], Memory]].put(uncompressedGroup.key, uncompressedGroup)

                    case group: Persistent.Group =>
                      skipList remove group.key
                  }
                }

              case _: Persistent.SegmentResponse =>
                skipList remove keyValue.key
            }
          }
      }

    def terminate() =
      actor.terminateAndClear()
  }

  case object Disabled extends MemorySweeper

  sealed trait Enabled extends MemorySweeper {
    def terminate(): Unit
  }

  sealed trait Block extends Enabled {
    def actor: ActorRef[Command, Unit]

    def add(key: BlockCache.Key,
            value: Slice[Byte],
            map: HashedMap.Concurrent[BlockCache.Key, Slice[Byte]]): Unit =
      actor ! Command.Block(
        key = key,
        valueSize = value.size,
        map = map
      )
  }

  case class BlockSweeper(blockSize: Int,
                          cacheSize: Int,
                          actorConfig: ActorConfig) extends SweeperImplementation with Block

  sealed trait KeyValue extends Enabled {
    def actor: ActorRef[Command, Unit]

    def add(keyValue: Persistent.SegmentResponse,
            skipList: SkipList[Slice[Byte], _]): Unit =
      actor ! Command.WeighKeyValue(
        keyValueRef = new WeakReference(keyValue),
        skipListRef = new WeakReference[SkipList[Slice[Byte], _]](skipList)
      )

    /**
     * If there was failure reading the Group's header guess it's weight. Successful reads are priority over 100% cache's accuracy.
     * The cache's will eventually adjust to be accurate but until then guessed weights should be used. The accuracy of guessed
     * weights can also be used.
     */
    def add(keyValue: swaydb.core.data.KeyValue.ReadOnly.Group,
            skipList: SkipList[Slice[Byte], _]): Unit =
      actor ! Command.WeighedKeyValue(
        keyValueRef = new WeakReference(keyValue),
        skipListRef = new WeakReference[SkipList[Slice[Byte], _]](skipList),
        weight = keyValue.valueLength
      )
  }

  case class KeyValueSweeper(cacheSize: Int,
                             actorConfig: ActorConfig) extends SweeperImplementation with KeyValue

  case class Both(blockSize: Int,
                  cacheSize: Int,
                  actorConfig: ActorConfig) extends SweeperImplementation with Block with KeyValue
}
