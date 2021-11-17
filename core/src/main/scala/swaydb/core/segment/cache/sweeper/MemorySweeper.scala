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

package swaydb.core.segment.cache.sweeper

import com.typesafe.scalalogging.LazyLogging
import swaydb.config.MemoryCache
import swaydb.core.cache.CacheNoIO
import swaydb.core.segment.data.Persistent
import swaydb.core.skiplist.SkipList
import swaydb.slice.{Slice, SliceOption}
import swaydb.utils.HashedMap
import swaydb.{ActorConfig, ActorRef, Glass}

import java.util.concurrent.ConcurrentSkipListMap
import scala.ref.WeakReference

private[core] sealed trait MemorySweeper

/**
 * Cleared all cached data. [[MemorySweeper]] is not required for Memory only databases
 * and zero databases.
 */
private[core] object MemorySweeper extends LazyLogging {

  def apply(memoryCache: MemoryCache): Option[MemorySweeper.On] =
    memoryCache match {
      case MemoryCache.Off =>
        None

      case block: MemoryCache.ByteCacheOnly =>
        Some(
          MemorySweeper.BlockSweeper(
            blockSize = block.minIOSeekSize,
            cacheSize = block.cacheCapacity,
            skipBlockCacheSeekSize = block.skipBlockCacheSeekSize,
            disableForSearchIO = block.disableForSearchIO,
            actorConfig = Some(block.actorConfig)
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
            maxKeyValuesPerSegment = block.maxCachedKeyValueCountPerSegment,
            sweepKeyValues = block.sweepCachedKeyValues,
            disableForSearchIO = block.disableForSearchIO,
            actorConfig = Some(block.actorConfig)
          )
        )
    }

  def close(sweeper: Option[MemorySweeper]): Unit =
    sweeper.foreach(close)

  def closeOn(sweeper: MemorySweeper.On): Unit =
    sweeper.actor foreach {
      actor =>
        logger.info("Clearing cached key-values")
        actor.terminateAndClear[Glass]()
    }

  def close(sweeper: MemorySweeper): Unit =
    sweeper match {
      case MemorySweeper.Off =>
        ()

      case enabled: MemorySweeper.On =>
        closeOn(enabled)
    }

  case object Off extends MemorySweeper

  sealed trait On extends MemorySweeper {
    def actor: Option[ActorRef[MemorySweeperCommand, Unit]]

    def terminateAndClear(): Unit

    def add(key: Slice[Byte],
            weight: Int,
            cache: ConcurrentSkipListMap[Slice[Byte], _]): Unit =
      if (actor.isDefined) {
        actor.get send new MemorySweeperCommand.SweepSkipListMap(
          key = key,
          weight = weight,
          cache = new WeakReference(cache)
        )
      } else {
        val exception = new Exception("Cache is not enabled")
        logger.error(exception.getMessage, exception)
        throw exception
      }
  }

  sealed trait Cache extends On {

    def add(weight: Int,
            cache: swaydb.core.cache.Cache[_, _, _]): Unit =
      if (actor.isDefined) {
        actor.get send new MemorySweeperCommand.SweepCache(
          weight = weight,
          cache = new WeakReference[swaydb.core.cache.Cache[_, _, _]](cache)
        )
      } else {
        val exception = new Exception("Cache is not enabled")
        logger.error(exception.getMessage, exception)
        throw exception
      }
  }

  sealed trait Block extends Cache {

    def blockSize: Int
    def cacheSize: Long
    def skipBlockCacheSeekSize: Int
    def disableForSearchIO: Boolean

    def add(key: Long,
            value: Slice[Byte],
            map: CacheNoIO[Unit, HashedMap.Concurrent[Long, SliceOption[Byte], Slice[Byte]]]): Unit =
      if (actor.isDefined) {
        actor.get send new MemorySweeperCommand.SweepBlockCache(
          key = key,
          valueSize = value.underlyingArraySize,
          map = map
        )
      } else {
        val exception = new Exception(s"${classOf[Block].getSimpleName} cache is not enabled")
        logger.error(exception.getMessage, exception)
        throw exception
      }
  }

  case class BlockSweeper(blockSize: Int,
                          cacheSize: Long,
                          skipBlockCacheSeekSize: Int,
                          disableForSearchIO: Boolean,
                          actorConfig: Option[ActorConfig]) extends MemorySweeperActor with Block

  sealed trait KeyValue extends On {

    def sweepKeyValues: Boolean

    def maxKeyValuesPerSegment: Option[Int]

    def add(keyValue: Persistent,
            skipList: SkipList[_, _, Slice[Byte], _]): Unit =
      if (sweepKeyValues)
        if (actor.isDefined) {
          actor.get send new MemorySweeperCommand.SweepKeyValue(
            keyValueRef = new WeakReference(keyValue),
            skipListRef = new WeakReference[SkipList[_, _, Slice[Byte], _]](skipList)
          )
        } else {
          val exception = new Exception(s"${classOf[KeyValue].getSimpleName} cache is not enabled")
          logger.error(exception.getMessage, exception)
          throw exception
        }
  }

  case class KeyValueSweeper(cacheSize: Long,
                             maxKeyValuesPerSegment: Option[Int],
                             actorConfig: Option[ActorConfig]) extends MemorySweeperActor with KeyValue {
    override val sweepKeyValues: Boolean = actorConfig.isDefined
  }

  case class All(blockSize: Int,
                 cacheSize: Long,
                 skipBlockCacheSeekSize: Int,
                 maxKeyValuesPerSegment: Option[Int],
                 sweepKeyValues: Boolean,
                 disableForSearchIO: Boolean,
                 actorConfig: Option[ActorConfig]) extends MemorySweeperActor with Block with KeyValue

}
