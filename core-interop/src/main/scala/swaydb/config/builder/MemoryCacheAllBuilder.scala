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

package swaydb.config.builder

import swaydb.ActorConfig
import swaydb.config.MemoryCache
import swaydb.utils.Java.OptionalConverter

import java.util.Optional

class MemoryCacheAllBuilder {
  private var minIOSeekSize: Int = _
  private var skipBlockCacheSeekSize: Int = _
  private var cacheCapacity: Int = _
  private var maxCachedKeyValueCountPerSegment: Option[Int] = _
  private var sweepCachedKeyValues: Boolean = _
  private var disableForSearchIO: Boolean = _
}

object MemoryCacheAllBuilder {

  class Step0(builder: MemoryCacheAllBuilder) {
    def minIOSeekSize(minIOSeekSize: Int) = {
      builder.minIOSeekSize = minIOSeekSize
      new Step1(builder)
    }
  }

  class Step1(builder: MemoryCacheAllBuilder) {
    def skipBlockCacheSeekSize(skipBlockCacheSeekSize: Int) = {
      builder.skipBlockCacheSeekSize = skipBlockCacheSeekSize
      new Step2(builder)
    }
  }

  class Step2(builder: MemoryCacheAllBuilder) {
    def cacheCapacity(cacheCapacity: Int) = {
      builder.cacheCapacity = cacheCapacity
      new Step3(builder)
    }
  }

  class Step3(builder: MemoryCacheAllBuilder) {
    def maxCachedKeyValueCountPerSegment(maxCachedKeyValueCountPerSegment: Optional[Integer]) = {
      builder.maxCachedKeyValueCountPerSegment = maxCachedKeyValueCountPerSegment.asScala.asInstanceOf[Option[Int]]
      new Step4(builder)
    }
  }

  class Step4(builder: MemoryCacheAllBuilder) {
    def sweepCachedKeyValues(sweepCachedKeyValues: Boolean) = {
      builder.sweepCachedKeyValues = sweepCachedKeyValues
      new Step5(builder)
    }
  }

  class Step5(builder: MemoryCacheAllBuilder) {
    def disableForSearchIO(disableForSearchIO: Boolean) = {
      builder.disableForSearchIO = disableForSearchIO
      new Step6(builder)
    }
  }

  class Step6(builder: MemoryCacheAllBuilder) {
    def actorConfig(actorConfig: ActorConfig) =
      MemoryCache.All(
        minIOSeekSize = builder.minIOSeekSize,
        skipBlockCacheSeekSize = builder.skipBlockCacheSeekSize,
        cacheCapacity = builder.cacheCapacity,
        disableForSearchIO = builder.disableForSearchIO,
        maxCachedKeyValueCountPerSegment = builder.maxCachedKeyValueCountPerSegment,
        sweepCachedKeyValues = builder.sweepCachedKeyValues,
        actorConfig = actorConfig
      )
  }

  def builder() = new Step0(new MemoryCacheAllBuilder())
}
