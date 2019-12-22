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

import swaydb.core.data.{KeyValue, Persistent}
import swaydb.core.io.file.BlockCache
import swaydb.core.util.{HashedMap, SkipList}
import swaydb.data.config.{ActorConfig, MemoryCache}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.{Actor, ActorRef}

import scala.ref.WeakReference

private[core] sealed trait Command
private[core] object Command {

  sealed trait KeyValueCommand extends Command {
    val keyValueRef: WeakReference[KeyValue.CacheAble]
    val skipListRef: WeakReference[SkipList[_, _, Slice[Byte], _]]
  }

  private[actor] class KeyValue(val keyValueRef: WeakReference[Persistent],
                                val skipListRef: WeakReference[SkipList[_, _, Slice[Byte], _]]) extends KeyValueCommand

  private[actor] class Cache(val weight: Int,
                             val cache: WeakReference[swaydb.core.cache.Cache[_, _, _]]) extends Command

  private[actor] class BlockCache(val key: BlockCache.Key,
                                  val valueSize: Int,
                                  val map: HashedMap.Concurrent[BlockCache.Key, Slice[Byte]]) extends Command

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

      case block: MemoryCache.ByteCacheOnly =>
        Some(
          MemorySweeper.BlockSweeper(
            blockSize = block.minIOSeekSize,
            cacheSize = block.cacheCapacity,
            skipBlockCacheSeekSize = block.skipBlockCacheSeekSize,
            actorConfig = Some(block.sweeperActorConfig)
          )
        )

      case MemoryCache.KeyValueCacheOnly(capacity, maxKeyValuesPerSegment, actorConfig) =>
        Some(
          MemorySweeper.KeyValueSweeper(
            cacheSize = capacity,
            maxKeyValuesPerSegment = maxKeyValuesPerSegment,
            actorConfig = actorConfig
          )
        )

      case block: MemoryCache.All =>
        Some(
          MemorySweeper.All(
            blockSize = block.minIOSeekSize,
            cacheSize = block.cacheCapacity,
            skipBlockCacheSeekSize = block.skipBlockCacheSeekSize,
            sweepKeyValues = block.sweepCachedKeyValues,
            maxKeyValuesPerSegment = block.maxCachedKeyValueCountPerSegment,
            actorConfig = Some(block.sweeperActorConfig)
          )
        )
    }

  def weigher(entry: Command): Int =
    entry match {
      case command: Command.BlockCache =>
        ByteSizeOf.long + command.valueSize + 264

      case command: Command.Cache =>
        ByteSizeOf.long + command.weight + 264

      case command: Command.KeyValue =>
        command.keyValueRef.get map {
          keyValue =>
            MemorySweeper.weight(keyValue).toInt
        } getOrElse 264 //264 for the weight of WeakReference itself.
    }

  def weight(keyValue: Persistent) = {
    val otherBytes = (Math.ceil(keyValue.key.size + keyValue.valueLength / 8.0) - 1.0) * 8
    //        if (keyValue.hasRemoveMayBe) (168 + otherBytes).toLong else (264 + otherBytes).toLong
    (264 * 2) + otherBytes
  }

  protected sealed trait SweeperImplementation {
    def cacheSize: Int

    def actorConfig: Option[ActorConfig]

    val actor: Option[ActorRef[Command, Unit]] =
      actorConfig map {
        actorConfig =>
          Actor.cacheFromConfig[Command](
            stashCapacity = cacheSize,
            config = actorConfig,
            weigher = MemorySweeper.weigher
          ) {
            (command, _) =>
              command match {
                case command: Command.KeyValueCommand =>
                  for {
                    skipList <- command.skipListRef.get
                    keyValue <- command.keyValueRef.get
                  } yield {
                    skipList remove keyValue.key
                  }

                case block: Command.BlockCache =>
                  block.map remove block.key

                case cache: Command.Cache =>
                  cache.cache.get foreach (_.clear())
              }
          }
      }

    def terminate() =
      actor.foreach(_.terminateAndClear())
  }

  case object Disabled extends MemorySweeper

  sealed trait Enabled extends MemorySweeper {
    def terminate(): Unit
  }

  sealed trait Cache extends Enabled {
    def actor: Option[ActorRef[Command, Unit]]

    def add(weight: Int, cache: swaydb.core.cache.Cache[_, _, _]): Unit =
      actor foreach {
        actor =>
          actor send new Command.Cache(
            weight = weight,
            cache = new WeakReference[swaydb.core.cache.Cache[_, _, _]](cache)
          )
      }
  }

  sealed trait Block extends Cache {
    def actor: Option[ActorRef[Command, Unit]]

    def add(key: BlockCache.Key,
            value: Slice[Byte],
            map: HashedMap.Concurrent[BlockCache.Key, Slice[Byte]]): Unit =
      actor foreach {
        actor =>
          actor send new Command.BlockCache(
            key = key,
            valueSize = value.underlyingArraySize,
            map = map
          )
      }
  }

  case class BlockSweeper(blockSize: Int,
                          cacheSize: Int,
                          skipBlockCacheSeekSize: Int,
                          actorConfig: Option[ActorConfig]) extends SweeperImplementation with Block

  sealed trait KeyValue extends Enabled {
    def actor: Option[ActorRef[Command, Unit]]

    def sweepKeyValues: Boolean

    def maxKeyValuesPerSegment: Option[Int]

    def add(keyValue: Persistent,
            skipList: SkipList[_, _, Slice[Byte], _]): Unit =
      if (sweepKeyValues)
        actor foreach {
          actor =>
            actor send new Command.KeyValue(
              keyValueRef = new WeakReference(keyValue),
              skipListRef = new WeakReference[SkipList[_, _, Slice[Byte], _]](skipList)
            )
        }
  }

  case class KeyValueSweeper(cacheSize: Int,
                             maxKeyValuesPerSegment: Option[Int],
                             actorConfig: Option[ActorConfig]) extends SweeperImplementation with KeyValue {
    override val sweepKeyValues: Boolean = actorConfig.isDefined
  }

  case class All(blockSize: Int,
                 cacheSize: Int,
                 skipBlockCacheSeekSize: Int,
                 maxKeyValuesPerSegment: Option[Int],
                 sweepKeyValues: Boolean,
                 actorConfig: Option[ActorConfig]) extends SweeperImplementation with Block with KeyValue

}
