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

package swaydb.data.config.builder

import swaydb.ActorConfig
import swaydb.data.config.MemoryCache
import swaydb.utils.Java.OptionalConverter

import java.util.Optional

class KeyValueCacheOnlyBuilder {
  private var cacheCapacity: Int = _
  private var maxCachedKeyValueCountPerSegment: Option[Int] = _
}

object KeyValueCacheOnlyBuilder {

  class Step0(builder: KeyValueCacheOnlyBuilder) {
    def cacheCapacity(cacheCapacity: Int) = {
      builder.cacheCapacity = cacheCapacity
      new Step1(builder)
    }
  }

  class Step1(builder: KeyValueCacheOnlyBuilder) {
    def maxCachedKeyValueCountPerSegment(maxCachedKeyValueCountPerSegment: Option[Int]) = {
      builder.maxCachedKeyValueCountPerSegment = maxCachedKeyValueCountPerSegment
      new Step2(builder)
    }

    def maxCachedKeyValueCountPerSegment(maxCachedKeyValueCountPerSegment: Optional[Int]) = {
      builder.maxCachedKeyValueCountPerSegment = maxCachedKeyValueCountPerSegment.asScala
      new Step2(builder)
    }
  }

  class Step2(builder: KeyValueCacheOnlyBuilder) {
    def actorConfig(actorConfig: Option[ActorConfig]) =
      MemoryCache.KeyValueCacheOnly(
        cacheCapacity = builder.cacheCapacity,
        maxCachedKeyValueCountPerSegment = builder.maxCachedKeyValueCountPerSegment,
        actorConfig = actorConfig
      )
  }

  def builder() = new Step0(new KeyValueCacheOnlyBuilder())
}
