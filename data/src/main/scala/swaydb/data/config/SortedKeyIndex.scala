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

import swaydb.Compression
import swaydb.data.config.builder.SortedKeyIndexBuilder
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.Java.JavaFunction

import scala.jdk.CollectionConverters._

sealed trait SortedKeyIndex
object SortedKeyIndex {

  def builder(): SortedKeyIndexBuilder.Step0 =
    SortedKeyIndexBuilder.builder()

  case class On(prefixCompression: PrefixCompression,
                enablePositionIndex: Boolean,
                optimiseForReverseIteration: Boolean,
                blockIOStrategy: IOAction => IOStrategy,
                compressions: UncompressedBlockInfo => Iterable[Compression]) extends SortedKeyIndex {
    def copyWithPrefixCompression(prefixCompression: PrefixCompression) =
      this.copy(prefixCompression = prefixCompression)

    def copyWithEnablePositionIndex(enablePositionIndex: Boolean) =
      this.copy(enablePositionIndex = enablePositionIndex)

    def copyWithOptimiseForReverseIteration(optimise: Boolean) =
      this.copy(optimiseForReverseIteration = optimise)

    def copyWithBlockIOStrategy(blockIOStrategy: JavaFunction[IOAction, IOStrategy]) =
      this.copy(blockIOStrategy = blockIOStrategy.apply)

    def copyWithCompressions(compressions: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      this.copy(compressions = info => compressions.apply(info).asScala)
  }
}
