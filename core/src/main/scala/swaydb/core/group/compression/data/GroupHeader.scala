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

package swaydb.core.group.compression.data

import swaydb.compression.DecompressorInternal

private[core] case class ValueInfo(valuesDecompressor: DecompressorInternal,
                                   valuesDecompressedLength: Int,
                                   valuesCompressedLength: Int)

private[core] case class GroupHeader(headerSize: Int,
                                     indexDecompressor: DecompressorInternal,
                                     indexCompressedLength: Int,
                                     indexDecompressedLength: Int,
                                     compressedStartIndexOffset: Int,
                                     compressedEndIndexOffset: Int,
                                     decompressedStartIndexOffset: Int,
                                     decompressedEndIndexOffset: Int,
                                     valueInfo: Option[ValueInfo])