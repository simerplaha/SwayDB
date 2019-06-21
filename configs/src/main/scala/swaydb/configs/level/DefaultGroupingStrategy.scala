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

import swaydb.data.api.grouping.{Compression, GroupGroupingStrategy, KeyValueGroupingStrategy}
import swaydb.data.compression.{LZ4Compressor, LZ4Decompressor, LZ4Instance}
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
      //specifies the number of key-values in a Segment before group. Grouping will be applies after every 1000th key-value.
      indexCompressions =
        //try index compression with LZ4 first with 10% compression requirement then Snappy and finally if both LZ4 & Snappy compression fails
        //simply add the current keys as uncompressed Group.
        Seq(
          Compression.LZ4(
            compressor = (LZ4Instance.FastestInstance, LZ4Compressor.FastCompressor(minCompressionSavingsPercent = minCompressionPercentage)),
            decompressor = (LZ4Instance.FastestInstance, LZ4Decompressor.FastDecompressor)
          ),
          Compression.UnCompressedGroup
        ),
      //try values compression with LZ4 first with 10% compression requirement then Snappy and finally if both LZ4 & Snappy compression fails
      //simply add the current values as uncompressed Group.
      valueCompressions =
        Seq(
          Compression.LZ4(
            compressor = (LZ4Instance.FastestInstance, LZ4Compressor.FastCompressor(minCompressionSavingsPercent = minCompressionPercentage)),
            decompressor = (LZ4Instance.FastestInstance, LZ4Decompressor.FastDecompressor)
          ),
          Compression.UnCompressedGroup
        ),
      //this groups existing Groups into a parent Group.
      groupGroupingStrategy = None
      //Example Grouping groups into nested group strategy.
      //      Some(
      //        GroupGroupingStrategy.Count(
      //          count = groupGroupsAtCount,
      //          indexCompression =
      //            Seq(
      //              Compression.LZ4(
      //                compressor = (LZ4Instance.FastestJavaInstance, LZ4Compressor.FastCompressor(minCompressionPercentage = minCompressionPercentage)),
      //                decompressor = (LZ4Instance.FastestJavaInstance, LZ4Decompressor.FastDecompressor)
      //              ),
      //              Compression.Snappy(minCompressionPercentage = minCompressionPercentage),
      //              Compression.UnCompressedGroup
      //            ),
      //          valueCompression =
      //            Seq(
      //              Compression.LZ4(
      //                compressor = (LZ4Instance.FastestJavaInstance, LZ4Compressor.FastCompressor(minCompressionPercentage = minCompressionPercentage)),
      //                decompressor = (LZ4Instance.FastestJavaInstance, LZ4Decompressor.FastDecompressor)
      //              ),
      //              Compression.Snappy(minCompressionPercentage = minCompressionPercentage),
      //              Compression.UnCompressedGroup
      //            )
      //        )
      //      )
    )
}