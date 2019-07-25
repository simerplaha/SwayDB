/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.configs.level

import swaydb.data.api.grouping.{GroupGroupingStrategy, KeyValueGroupingStrategy}
import swaydb.data.config._
import swaydb.data.util.StorageUnits._

object DefaultGroupingStrategy {

  /**
   * Default grouping Strategy the last Level of the Persistent configuration. It uses 3 three compression types
   * with minimum compression requirement of 10%.
   *
   * All there compression libraries are used and compressions are executed in their order until a successful compression is achieved.
   * 1. LZ4's fastest Java instance with Fast compressor and decompressor.
   * 2. Snappy
   * 3. UnCompressedGroup - No compression, key-values are just grouped.
   *
   * After key-values are Grouped, the Groups can also be grouped which will result in nested Groups. Although nested Groups
   * can give high compression but they it can also have .
   * Either [[GroupGroupingStrategy.Count]] or [[GroupGroupingStrategy.Size]] can be used check documentation on the website for more info.
   *
   * By default currently nested Group compression is not used because the default file sizes are too small (2.mb) to be creating nested Groups.
   */
  def apply(groupKeyValuesAtSize: Int = 1.mb,
            minCompressionPercentage: Double = 10.0) =
    KeyValueGroupingStrategy.Size( //Grouping strategy for key-values
      //when the size of keys and values reaches 1.mb, do grouping!
      size = groupKeyValuesAtSize,
      sortedIndex =
        SortedKeyIndex.Enable(
          prefixCompression =
            PrefixCompression.Enable(
              resetCount = Some(10)
            ),
          enablePositionIndex = true,
          ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = ioAction.isCompressed),
          compressions = _ => Seq.empty
        ),
      hashIndex =
        RandomKeyIndex.Enable(
          tries = 5,
          minimumNumberOfKeys = 2,
          minimumNumberOfHits = 2,
          allocateSpace = _.requiredSpace * 2,
          ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = ioAction.isCompressed),
          compression = _ => Seq.empty
        ),
      binarySearchIndex =
        BinarySearchKeyIndex.FullIndex(
          minimumNumberOfKeys = 5,
          ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = ioAction.isCompressed),
          compression = _ => Seq.empty
        ),
      bloomFilter =
        MightContainIndex.Enable(
          falsePositiveRate = 0.001,
          minimumNumberOfKeys = 10,
          ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = ioAction.isCompressed),
          compression = _ => Seq.empty
        ),
      values =
        ValuesConfig(
          compressDuplicateValues = true,
          compressDuplicateRangeValues = true,
          ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = ioAction.isCompressed),
          compression = _ => Seq.empty
        ),
      applyGroupingOnCopy = false,
      groupIO = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = ioAction.isCompressed),
      groupCompressions = _ => Seq.empty,
      groupGroupingStrategy = None
    )
}