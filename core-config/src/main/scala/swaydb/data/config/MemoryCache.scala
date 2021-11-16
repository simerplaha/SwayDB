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
