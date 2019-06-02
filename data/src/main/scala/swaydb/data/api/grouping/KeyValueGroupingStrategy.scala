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

package swaydb.data.api.grouping

sealed trait KeyValueGroupingStrategy
object KeyValueGroupingStrategy {

  object Count {
    def apply(count: Int,
              indexCompression: Compression,
              valueCompression: Compression,
              groupGroupingStrategy: Option[GroupGroupingStrategy]): Count =
      new Count(
        count = count,
        indexCompressions = Seq(indexCompression),
        valueCompressions = Seq(valueCompression),
        groupGroupingStrategy = groupGroupingStrategy
      )
  }

  case class Count(count: Int,
                   indexCompressions: Seq[Compression],
                   valueCompressions: Seq[Compression],
                   groupGroupingStrategy: Option[GroupGroupingStrategy]) extends KeyValueGroupingStrategy

  object Size {
    def apply(size: Int,
              indexCompression: Compression,
              valueCompression: Compression,
              groupGroupingStrategy: Option[GroupGroupingStrategy]): Size =
      new Size(
        size = size,
        indexCompressions = Seq(indexCompression),
        valueCompressions = Seq(valueCompression),
        groupGroupingStrategy = groupGroupingStrategy
      )
  }

  case class Size(size: Int,
                  indexCompressions: Seq[Compression],
                  valueCompressions: Seq[Compression],
                  groupGroupingStrategy: Option[GroupGroupingStrategy]) extends KeyValueGroupingStrategy
}
