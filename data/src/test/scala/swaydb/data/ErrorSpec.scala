/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.data

import java.io.FileNotFoundException
import java.nio.ReadOnlyBufferException
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException, OverlappingFileLockException}
import java.nio.file.{NoSuchFileException, Paths}

import org.scalatest.{FlatSpec, Matchers}
import swaydb.Error.DataAccess
import swaydb.Exception.NullMappedByteBuffer
import swaydb.data.slice.Slice
import swaydb.{Error, _}

class ErrorSpec extends FlatSpec with Matchers {

  it should "convert known Exception to typed Errors and vice versa" in {
    val reserve = Reserve.free[Unit](name = "ErrorSpec")

    Error(Exception.OpeningFile(Paths.get("/some/path"), reserve)) shouldBe
      Error.OpeningFile(Paths.get("/some/path"), reserve)

    val noSuchFileException = new NoSuchFileException("/some/path")
    Error(noSuchFileException) shouldBe Error.NoSuchFile(None, Some(noSuchFileException))

    val fileNotFoundException = new FileNotFoundException("some_file.sh")
    Error(fileNotFoundException) shouldBe Error.FileNotFound(fileNotFoundException)

    val asynchronousCloseException = new AsynchronousCloseException()
    Error(asynchronousCloseException) shouldBe Error.AsynchronousClose(asynchronousCloseException)

    val closedChannelException = new ClosedChannelException()
    Error(closedChannelException) shouldBe Error.ClosedChannel(closedChannelException)

    val nullPointerException = new NullPointerException()
    Error(Exception.NullMappedByteBuffer(nullPointerException, reserve)) shouldBe
      Error.NullMappedByteBuffer(NullMappedByteBuffer(nullPointerException, reserve))

    val reserveError = Error.ReservedResource(reserve)
    val reserveErrorFromException = Error(Exception.ReservedResource(reserve))
    reserveError shouldBe reserveErrorFromException
    reserveError.reserve.hashCode() shouldBe reserveErrorFromException.asInstanceOf[Error.ReservedResource].reserve.hashCode() //same reserve

    Error(Exception.NotAnIntFile(Paths.get("some/path"))) shouldBe
      Error.NotAnIntFile(Exception.NotAnIntFile(Paths.get("some/path")))

    Error(Exception.UnknownExtension(Paths.get("some/path"))) shouldBe
      Error.UnknownExtension(Exception.UnknownExtension(Paths.get("some/path")))

    Error(Exception.OverlappingPushSegment) shouldBe
      Error.OverlappingPushSegment

    Error(Exception.NoSegmentsRemoved) shouldBe
      Error.NoSegmentsRemoved

    Error(Exception.NotSentToNextLevel) shouldBe
      Error.NotSentToNextLevel

    Error(Exception.MergeKeyValuesWithoutTargetSegment(1)) shouldBe
      Error.MergeKeyValuesWithoutTargetSegment(1)

    val readOnlyBufferException = new ReadOnlyBufferException
    Error(readOnlyBufferException) shouldBe Error.ReadOnlyBuffer(readOnlyBufferException)

    Error(Exception.FunctionNotFound(Slice.writeInt(1))) shouldBe
      Error.FunctionNotFound(Slice.writeInt(1))

    val overlappingFileLockException = swaydb.Exception.OverlappingFileLock(new OverlappingFileLockException)
    Error(overlappingFileLockException) shouldBe Error.UnableToLockDirectory(overlappingFileLockException)

    val arrayIndexOutOfBoundsException = new ArrayIndexOutOfBoundsException
    Error(arrayIndexOutOfBoundsException) shouldBe Error.DataAccess(DataAccess.message, arrayIndexOutOfBoundsException)

    val indexOutOfBoundsException = new IndexOutOfBoundsException
    Error(indexOutOfBoundsException) shouldBe Error.DataAccess(DataAccess.message, indexOutOfBoundsException)

    val illegalArgumentException = new IllegalArgumentException
    Error(illegalArgumentException) shouldBe Error.DataAccess(DataAccess.message, illegalArgumentException)

    val negativeArraySizeException = new NegativeArraySizeException
    Error(negativeArraySizeException) shouldBe Error.DataAccess(DataAccess.message, negativeArraySizeException)

    val segmentFileMissing = Exception.SegmentFileMissing(Paths.get("some/path"))
    Error(segmentFileMissing) shouldBe Error.SegmentFileMissing(segmentFileMissing)

    val failedToWriteAllBytes = Exception.FailedToWriteAllBytes(0, 10, 100)
    Error(failedToWriteAllBytes) shouldBe Error.FailedToWriteAllBytes(failedToWriteAllBytes)

    val cannotCopyInMemoryFiles = Exception.CannotCopyInMemoryFiles(Paths.get("111"))
    Error(cannotCopyInMemoryFiles) shouldBe Error.CannotCopyInMemoryFiles(cannotCopyInMemoryFiles)

    val invalidKeyValueId = Exception.InvalidBaseId(Int.MaxValue)
    Error(invalidKeyValueId) shouldBe Error.InvalidKeyValueId(invalidKeyValueId)

    val exception = new Throwable("Some unknown exception")
    Error(exception) shouldBe Error.Fatal(exception)
  }
}
