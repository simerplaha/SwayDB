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

package swaydb.data.config.builder

import swaydb.data.config.{ActorConfig, MemoryCache}

class ByteCacheOnlyBuilder {
  private var minIOSeekSize: Int = _
  private var skipBlockCacheSeekSize: Int = _
  private var cacheCapacity: Int = _
  private var disableForSearchIO: Boolean = _
}

object ByteCacheOnlyBuilder {

  class Step0(builder: ByteCacheOnlyBuilder) {
    def minIOSeekSize(minIOSeekSize: Int) = {
      builder.minIOSeekSize = minIOSeekSize
      new Step1(builder)
    }
  }

  class Step1(builder: ByteCacheOnlyBuilder) {
    def skipBlockCacheSeekSize(skipBlockCacheSeekSize: Int) = {
      builder.skipBlockCacheSeekSize = skipBlockCacheSeekSize
      new Step2(builder)
    }
  }

  class Step2(builder: ByteCacheOnlyBuilder) {
    def cacheCapacity(cacheCapacity: Int) = {
      builder.cacheCapacity = cacheCapacity
      new Step3(builder)
    }
  }

  class Step3(builder: ByteCacheOnlyBuilder) {
    def disableForSearchIO(disableForSearchIO: Boolean) = {
      builder.disableForSearchIO = disableForSearchIO
      new Step4(builder)
    }
  }

  class Step4(builder: ByteCacheOnlyBuilder) {
    def actorConfig(actorConfig: ActorConfig) =
      MemoryCache.ByteCacheOnly(
        minIOSeekSize = builder.minIOSeekSize,
        skipBlockCacheSeekSize = builder.skipBlockCacheSeekSize,
        cacheCapacity = builder.cacheCapacity,
        disableForSearchIO = builder.disableForSearchIO,
        actorConfig = actorConfig
      )
  }

  def builder() = new Step0(new ByteCacheOnlyBuilder())
}
