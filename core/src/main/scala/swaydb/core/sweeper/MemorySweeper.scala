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

package swaydb.core.sweeper

import com.typesafe.scalalogging.LazyLogging
import swaydb.ActorConfig.QueueOrder
import swaydb.cache.CacheNoIO
import swaydb.core.data.Persistent
import swaydb.utils.HashedMap
import swaydb.skiplist.SkipList
import swaydb.data.config.MemoryCache
import swaydb.slice.{Slice, SliceOption}
import swaydb.utils.ByteSizeOf
import swaydb.{Actor, ActorConfig, ActorRef, Glass}

import java.util.concurrent.ConcurrentSkipListMap
import scala.ref.WeakReference

private[core] sealed trait Command
private[core] object Command {

  private[sweeper] class KeyValue(val keyValueRef: WeakReference[Persistent],
                                  val skipListRef: WeakReference[SkipList[_, _, Slice[Byte], _]]) extends Command

  private[sweeper] class Cache(val weight: Int,
                               val cache: WeakReference[swaydb.cache.Cache[_, _, _]]) extends Command

  private[sweeper] class SkipListMap(val key: Slice[Byte],
                                     val weight: Int,
                                     val cache: WeakReference[ConcurrentSkipListMap[Slice[Byte], _]]) extends Command

  private[sweeper] class BlockCache(val key: Long,
                                    val valueSize: Int,
                                    val map: CacheNoIO[Unit, HashedMap.Concurrent[Long, SliceOption[Byte], Slice[Byte]]]) extends Command

}

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

  def close(sweeper: MemorySweeper): Unit =
    sweeper match {
      case MemorySweeper.Off =>
        ()

      case enabled: MemorySweeper.On =>
        close(enabled)
    }

  def close(sweeper: MemorySweeper.On): Unit =
    sweeper.actor foreach {
      actor =>
        logger.info("Clearing cached key-values")
        actor.terminateAndClear[Glass]()
    }

  def weigher(entry: Command): Int =
    entry match {
      case command: Command.BlockCache =>
        ByteSizeOf.long + command.valueSize + 264

      case command: Command.Cache =>
        ByteSizeOf.long + command.weight + 264

      case command: Command.SkipListMap =>
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
    def cacheSize: Long

    def actorConfig: Option[ActorConfig]

    val actor: Option[ActorRef[Command, Unit]] =
      actorConfig map {
        actorConfig =>
          Actor.cacheFromConfig[Command](
            config = actorConfig,
            stashCapacity = cacheSize,
            queueOrder = QueueOrder.FIFO,
            weigher = MemorySweeper.weigher
          ) {
            (command, _) =>
              command match {
                case command: Command.KeyValue =>
                  for {
                    skipList <- command.skipListRef.get
                    keyValue <- command.keyValueRef.get
                  } yield {
                    skipList remove keyValue.key
                  }

                case block: Command.BlockCache =>
                  val cacheOptional = block.map.get()
                  if (cacheOptional.isDefined) {
                    val cache = cacheOptional.get
                    cache.remove(block.key)
                    if (cache.isEmpty) block.map.clear()
                  }

                case ref: Command.SkipListMap =>
                  val cacheOptional = ref.cache.get
                  if (cacheOptional.isDefined)
                    cacheOptional.get.remove(ref.key)

                case cache: Command.Cache =>
                  val cacheOptional = cache.cache.get
                  if (cacheOptional.isDefined)
                    cacheOptional.get.clear()
              }
          }.start()
      }

    def terminateAndClear() =
      actor.foreach(_.terminateAndClear[Glass]())
  }

  case object Off extends MemorySweeper

  sealed trait On extends MemorySweeper {
    def actor: Option[ActorRef[Command, Unit]]

    def terminateAndClear(): Unit

    def add(key: Slice[Byte],
            weight: Int,
            cache: ConcurrentSkipListMap[Slice[Byte], _]): Unit =
      if (actor.isDefined) {
        actor.get send new Command.SkipListMap(
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

    def add(weight: Int, cache: swaydb.cache.Cache[_, _, _]): Unit =
      if (actor.isDefined) {
        actor.get send new Command.Cache(
          weight = weight,
          cache = new WeakReference[swaydb.cache.Cache[_, _, _]](cache)
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
        actor.get send new Command.BlockCache(
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
                          actorConfig: Option[ActorConfig]) extends SweeperImplementation with Block

  sealed trait KeyValue extends On {

    def sweepKeyValues: Boolean

    def maxKeyValuesPerSegment: Option[Int]

    def add(keyValue: Persistent,
            skipList: SkipList[_, _, Slice[Byte], _]): Unit =
      if (sweepKeyValues)
        if (actor.isDefined) {
          actor.get send new Command.KeyValue(
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
                             actorConfig: Option[ActorConfig]) extends SweeperImplementation with KeyValue {
    override val sweepKeyValues: Boolean = actorConfig.isDefined
  }

  case class All(blockSize: Int,
                 cacheSize: Long,
                 skipBlockCacheSeekSize: Int,
                 maxKeyValuesPerSegment: Option[Int],
                 sweepKeyValues: Boolean,
                 disableForSearchIO: Boolean,
                 actorConfig: Option[ActorConfig]) extends SweeperImplementation with Block with KeyValue

}
