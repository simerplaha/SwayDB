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

package swaydb.core.segment

import java.nio.file.Path

object SegmentException {
  case class FailedToWriteAllBytes(written: Int, expected: Int, bytesSize: Int) extends Exception(s"Failed to write all bytes written: $written, expected : $expected, bytesSize: $bytesSize")
  case class FailedToOpenFile(file: Path) extends Exception(s"Failed to open file $file")
  case object BusyDecompressingIndex extends Exception(s"Failed to decompress index")
  case object BusyDecompressionValues extends Exception(s"Failed to decompress values")
  case object BusyReadingHeader extends Exception(s"Busy reading header")
  case object BusyFetchingValue extends Exception(s"Failed to lazily fetch value")
  case class CannotCopyInMemoryFiles(file: Path) extends Exception(s"Cannot copy in-memory files $file")
  case class SegmentCorruptionException(message: String, cause: Exception) extends Exception(message, cause)
  case class SegmentFileMissing(path: Path) extends Exception(s"$path: Segment file missing.")
  case class InvalidEntryId(id: Int) extends Exception(s"Invalid entryId: $id.")
}