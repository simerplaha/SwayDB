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
 */

package swaydb.data.config

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

sealed trait MemoryCache

object MemoryCache {

  case object Disable extends MemoryCache

  object Enabled {
    def default(minIOSeekSize: Int,
                skipBlockCacheSeekSize: Int,
                memorySize: Int,
                interval: FiniteDuration,
                ec: ExecutionContext) =
      ByteCacheOnly(
        minIOSeekSize = minIOSeekSize,
        skipBlockCacheSeekSize = skipBlockCacheSeekSize,
        cacheCapacity = memorySize,
        actorConfig =
          ActorConfig.TimeLoop(
            delay = interval,
            ec = ec
          )
      )
  }

  sealed trait Enabled extends MemoryCache {
    def cacheCapacity: Int
  }

  sealed trait Block extends Enabled {
    val minIOSeekSize: Int
    val cacheCapacity: Int
    val actorConfig: ActorConfig
  }

  case class ByteCacheOnly(minIOSeekSize: Int,
                           skipBlockCacheSeekSize: Int,
                           cacheCapacity: Int,
                           actorConfig: ActorConfig) extends Block

  case class KeyValueCacheOnly(cacheCapacity: Int,
                               maxCachedKeyValueCountPerSegment: Option[Int],
                               actorConfig: Option[ActorConfig]) extends Enabled

  case class All(minIOSeekSize: Int,
                 skipBlockCacheSeekSize: Int,
                 cacheCapacity: Int,
                 maxCachedKeyValueCountPerSegment: Option[Int],
                 sweepCachedKeyValues: Boolean,
                 actorConfig: ActorConfig) extends Enabled with Block
}
