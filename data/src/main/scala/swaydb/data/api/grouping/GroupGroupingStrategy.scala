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

sealed trait GroupGroupingStrategy
object GroupGroupingStrategy {

  object Count {
    def apply(count: Int,
              indexCompression: Compression,
              valueCompression: Compression): Count =
      new Count(
        count = count,
        indexCompression = Seq(indexCompression),
        valueCompression = Seq(valueCompression)
      )
  }

  case class Count(count: Int,
                   indexCompression: Seq[Compression],
                   valueCompression: Seq[Compression]) extends GroupGroupingStrategy

  object Size {
    def apply(size: Int,
              indexCompression: Compression,
              valueCompression: Compression): Size =
      new Size(
        size = size,
        indexCompression = Seq(indexCompression),
        valueCompression = Seq(valueCompression)
      )
  }

  case class Size(size: Int,
                  indexCompression: Seq[Compression],
                  valueCompression: Seq[Compression]) extends GroupGroupingStrategy
}