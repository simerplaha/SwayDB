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

package swaydb.data.config

import java.util.Optional

import swaydb.data.config.builder.{ByteCacheOnlyBuilder, KeyValueCacheOnlyBuilder, MemoryCacheAllBuilder}

import scala.compat.java8.OptionConverters.RichOptionalGeneric

sealed trait MemoryCache

object MemoryCache {

  def disable: MemoryCache = Disable
  case object Disable extends MemoryCache

  sealed trait Enabled extends MemoryCache {
    def cacheCapacity: Int
  }

  sealed trait Block extends Enabled {
    val minIOSeekSize: Int
    val cacheCapacity: Int
    val actorConfig: ActorConfig
  }

  def byteCacheOnlyBuilder(): ByteCacheOnlyBuilder.Step0 =
    ByteCacheOnlyBuilder.builder()

  def keyValueCacheOnlyBuilder(): KeyValueCacheOnlyBuilder.Step0 =
    KeyValueCacheOnlyBuilder.builder()

  def allBuilder(): MemoryCacheAllBuilder.Step0 =
    MemoryCacheAllBuilder.builder()

  case class ByteCacheOnly(minIOSeekSize: Int,
                           skipBlockCacheSeekSize: Int,
                           cacheCapacity: Int,
                           actorConfig: ActorConfig) extends Block {
    def copyWithMinIOSeekSize(minIOSeekSize: Int) =
      this.copy(minIOSeekSize = minIOSeekSize)

    def copyWithSkipBlockCacheSeekSize(skipBlockCacheSeekSize: Int) =
      this.copy(skipBlockCacheSeekSize = skipBlockCacheSeekSize)

    def copyWithCacheCapacity(cacheCapacity: Int) =
      this.copy(cacheCapacity = cacheCapacity)

    def copyWithActorConfig(actorConfig: ActorConfig) =
      this.copy(actorConfig = actorConfig)
  }

  case class KeyValueCacheOnly(cacheCapacity: Int,
                               maxCachedKeyValueCountPerSegment: Option[Int],
                               actorConfig: Option[ActorConfig]) extends Enabled {
    def copyWithCacheCapacity(cacheCapacity: Int) =
      this.copy(cacheCapacity = cacheCapacity)

    def copyWithMaxCachedKeyValueCountPerSegment(maxCachedKeyValueCountPerSegment: Optional[Int]) =
      this.copy(maxCachedKeyValueCountPerSegment = maxCachedKeyValueCountPerSegment.asScala)

    def copyWithActorConfig(actorConfig: Optional[ActorConfig]) =
      this.copy(actorConfig = actorConfig.asScala)
  }

  case class All(minIOSeekSize: Int,
                 skipBlockCacheSeekSize: Int,
                 cacheCapacity: Int,
                 maxCachedKeyValueCountPerSegment: Option[Int],
                 sweepCachedKeyValues: Boolean,
                 actorConfig: ActorConfig) extends Enabled with Block {
    def copyWithMinIOSeekSize(minIOSeekSize: Int) =
      this.copy(minIOSeekSize = minIOSeekSize)

    def copyWithSkipBlockCacheSeekSize(skipBlockCacheSeekSize: Int) =
      this.copy(skipBlockCacheSeekSize = skipBlockCacheSeekSize)

    def copyWithCacheCapacity(cacheCapacity: Int) =
      this.copy(cacheCapacity = cacheCapacity)

    def copyWithMaxCachedKeyValueCountPerSegment(maxCachedKeyValueCountPerSegment: Optional[Int]) =
      this.copy(maxCachedKeyValueCountPerSegment = maxCachedKeyValueCountPerSegment.asScala)

    def copyWithSweepCachedKeyValues(sweepCachedKeyValues: Boolean) =
      this.copy(sweepCachedKeyValues = sweepCachedKeyValues)

    def copyWithActorConfig(actorConfig: ActorConfig) =
      this.copy(actorConfig = actorConfig)
  }
}
