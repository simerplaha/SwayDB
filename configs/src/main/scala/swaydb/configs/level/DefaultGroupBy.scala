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

import swaydb.data.api.grouping.GroupBy
import swaydb.data.config._
import swaydb.data.util.StorageUnits._

object DefaultGroupBy {

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
   * IO [[GroupBy.Groups]] can be used check documentation on the website for more info.
   *
   * By default currently nested Group compression is not used because the default file sizes are too small (2.mb) to be creating nested Groups.
   */
  def apply(minCompressionPercentage: Double = 10.0): GroupBy.KeyValues =
    GroupBy.KeyValues( //Grouping strategy for key-values
      //when the size of keys and values reaches 1.mb, do grouping!
      count = 100,
      size = None,
      sortedIndex =
        SortedKeyIndex.Enable(
          prefixCompression = PrefixCompression.Disable(normaliseIndexForBinarySearch = true, enablePartialRead = true),
          enablePositionIndex = true,
          ioStrategy = ioAction => IOStrategy.ConcurrentIO(cacheOnAccess = ioAction.isCompressed),
          compressions = _ => Seq.empty
        ),
      hashIndex =
        RandomKeyIndex.Enable(
          maxProbe = 2,
          minimumNumberOfKeys = 2,
          minimumNumberOfHits = 2,
          copyKeys = true,
          allocateSpace = _.requiredSpace * 2,
          ioStrategy = ioAction => IOStrategy.ConcurrentIO(cacheOnAccess = ioAction.isCompressed),
          compression = _ => Seq.empty
        ),
      binarySearchIndex =
        BinarySearchIndex.FullIndex(
          minimumNumberOfKeys = 5,
          searchSortedIndexDirectly = true,
          ioStrategy = ioAction => IOStrategy.ConcurrentIO(cacheOnAccess = ioAction.isCompressed),
          compression = _ => Seq.empty
        ),
      bloomFilter =
        MightContainIndex.Disable,
      values =
        ValuesConfig(
          compressDuplicateValues = true,
          compressDuplicateRangeValues = true,
          ioStrategy = ioAction => IOStrategy.ConcurrentIO(cacheOnAccess = ioAction.isCompressed),
          compression = _ => Seq.empty
        ),
      applyGroupingOnCopy = false,
      groupIO = ioAction => IOStrategy.ConcurrentIO(cacheOnAccess = ioAction.isCompressed),
      groupCompressions = _ => Seq.empty,
      groupGroupBy = None
    )
}