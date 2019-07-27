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

package swaydb

import java.nio.channels.OverlappingFileLockException
import java.nio.file.Path

import swaydb.data.Reserve
import swaydb.data.slice.Slice


/**
 * Exception types for all known [[Error]]s that can occur. Each [[Error]] can be converted to
 * Exception which which can then be converted back to [[Error]].
 *
 * SwayDB's code itself does not use these exception it uses [[Error]] type. These types are handy when
 * converting an [[IO]] type to [[scala.util.Try]] by the client using [[toTry]].
 */
sealed trait Exception
object Exception {
  case class Busy(error: Error.ReservedIO) extends Throwable("Is busy") with Exception
  case class OpeningFile(file: Path, reserve: Reserve[Unit]) extends Throwable(s"Failed to open file $file") with Exception

  case class ReservedResource(reserve: Reserve[Unit]) extends Throwable("Failed to fetch value")  with Exception
  case class NullMappedByteBuffer(exception: scala.Exception, reserve: Reserve[Unit]) extends Throwable(exception)  with Exception

  case object OverlappingPushSegment extends Throwable("Contains overlapping busy Segments")  with Exception
  case object NoSegmentsRemoved extends Throwable("No Segments Removed")  with Exception
  case object NotSentToNextLevel extends Throwable("Not sent to next Level")  with Exception
  case class MergeKeyValuesWithoutTargetSegment(keyValueCount: Int) extends Throwable(s"Received key-values to merge without target Segment - keyValueCount: $keyValueCount")  with Exception

  /**
   * [[functionID]] itself is not logged or printed to console since it may contain sensitive data but instead this Exception
   * with the [[functionID]] is returned to the client for reads and the exception's string message is only logged.
   *
   * @param functionID the id of the missing function.
   */
  case class FunctionNotFound(functionID: Slice[Byte]) extends Throwable("Function not found for ID.")  with Exception
  case class OverlappingFileLock(exception: OverlappingFileLockException) extends Throwable("Failed to get directory lock.")  with Exception
  case class FailedToWriteAllBytes(written: Int, expected: Int, bytesSize: Int) extends Throwable(s"Failed to write all bytes written: $written, expected : $expected, bytesSize: $bytesSize")  with Exception
  case class CannotCopyInMemoryFiles(file: Path) extends Throwable(s"Cannot copy in-memory files $file")  with Exception
  case class SegmentFileMissing(path: Path) extends Throwable(s"$path: Segment file missing.")  with Exception
  case class InvalidKeyValueId(id: Int) extends Throwable(s"Invalid keyValueId: $id.")  with Exception

  case class InvalidDecompressorId(id: Int) extends Throwable(s"Invalid decompressor id: $id")  with Exception

  case class NotAnIntFile(path: Path) extends Throwable  with Exception
  case class UnknownExtension(path: Path) extends Throwable  with Exception

}
