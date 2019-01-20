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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.group.compression

private[core] object GroupCompressorFailure {
  case class FailedToCompressKeys(keys: Int) extends Exception(s"Failed to compress keys: $keys.bytes")
  case class FailedToCompressValues(values: Int) extends Exception(s"Failed to compress values: $values.bytes")
  case class InvalidGroupCompressorFormatId(formatId: Int) extends Exception(s"Invalid GroupCompressor formatId: $formatId")
  case class InvalidGroupKeyValuesHeadPosition(position: Int) extends Exception(s"Group key-values have invalid position $position. Expected 1")
  case object GroupKeyIsEmpty extends Exception(s"Group key is empty")
}