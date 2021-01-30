/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import swaydb.ActorConfig
import swaydb.data.config.builder.{ByteCacheOnlyBuilder, KeyValueCacheOnlyBuilder, MemoryCacheAllBuilder}

import java.util.Optional
import scala.compat.java8.OptionConverters.RichOptionalGeneric

sealed trait MemoryCache

case object MemoryCache {

  def off: MemoryCache = Off
  case object Off extends MemoryCache

  sealed trait On extends MemoryCache {
    def cacheCapacity: Long
  }

  sealed trait Block extends On {
    val minIOSeekSize: Int
    val cacheCapacity: Long
    /**
     * Disables caching bytes created by IO.
     */
    val disableForSearchIO: Boolean
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
                           cacheCapacity: Long,
                           disableForSearchIO: Boolean,
                           actorConfig: ActorConfig) extends Block {
    def copyWithMinIOSeekSize(minIOSeekSize: Int) =
      this.copy(minIOSeekSize = minIOSeekSize)

    def copyWithSkipBlockCacheSeekSize(skipBlockCacheSeekSize: Int) =
      this.copy(skipBlockCacheSeekSize = skipBlockCacheSeekSize)

    def copyWithCacheCapacity(cacheCapacity: Long) =
      this.copy(cacheCapacity = cacheCapacity)

    def copyWithDisableForSearchIO(disableForSearchIO: Boolean) =
      this.copy(disableForSearchIO = disableForSearchIO)

    def copyWithActorConfig(actorConfig: ActorConfig) =
      this.copy(actorConfig = actorConfig)
  }

  case class KeyValueCacheOnly(cacheCapacity: Long,
                               maxCachedKeyValueCountPerSegment: Option[Int],
                               actorConfig: Option[ActorConfig]) extends On {
    def copyWithCacheCapacity(cacheCapacity: Long) =
      this.copy(cacheCapacity = cacheCapacity)

    def copyWithMaxCachedKeyValueCountPerSegment(maxCachedKeyValueCountPerSegment: Optional[Int]) =
      this.copy(maxCachedKeyValueCountPerSegment = maxCachedKeyValueCountPerSegment.asScala)

    def copyWithActorConfig(actorConfig: Optional[ActorConfig]) =
      this.copy(actorConfig = actorConfig.asScala)
  }

  case class All(minIOSeekSize: Int,
                 skipBlockCacheSeekSize: Int,
                 cacheCapacity: Long,
                 disableForSearchIO: Boolean,
                 maxCachedKeyValueCountPerSegment: Option[Int],
                 sweepCachedKeyValues: Boolean,
                 actorConfig: ActorConfig) extends On with Block {
    def copyWithMinIOSeekSize(minIOSeekSize: Int) =
      this.copy(minIOSeekSize = minIOSeekSize)

    def copyWithSkipBlockCacheSeekSize(skipBlockCacheSeekSize: Int) =
      this.copy(skipBlockCacheSeekSize = skipBlockCacheSeekSize)

    def copyWithCacheCapacity(cacheCapacity: Long) =
      this.copy(cacheCapacity = cacheCapacity)

    def copyWithMaxCachedKeyValueCountPerSegment(maxCachedKeyValueCountPerSegment: Optional[Int]) =
      this.copy(maxCachedKeyValueCountPerSegment = maxCachedKeyValueCountPerSegment.asScala)

    def copyWithDisableForIO(disableForSearchIO: Boolean) =
      this.copy(disableForSearchIO = disableForSearchIO)

    def copyWithSweepCachedKeyValues(sweepCachedKeyValues: Boolean) =
      this.copy(sweepCachedKeyValues = sweepCachedKeyValues)

    def copyWithActorConfig(actorConfig: ActorConfig) =
      this.copy(actorConfig = actorConfig)
  }
}
