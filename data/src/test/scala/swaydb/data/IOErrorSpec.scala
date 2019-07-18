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

package swaydb.data

import java.io.FileNotFoundException
import java.nio.ReadOnlyBufferException
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException}
import java.nio.file.{NoSuchFileException, Paths}

import org.scalatest.{FlatSpec, Matchers}
import swaydb.data.IO.Exception.NullMappedByteBuffer

class IOErrorSpec extends FlatSpec with Matchers {

  it should "convert known Exception to known Error types and vice versa" in {

    var error: IO.Error = IO.Error.OpeningFile(Paths.get("/some/path"), Reserve())
    var exception: Throwable = IO.Exception.OpeningFile(Paths.get("/some/path"), Reserve())
    error.exception.getMessage shouldBe exception.getMessage
    IO.Error(exception) shouldBe a[IO.Error.OpeningFile]

    //BUSY Errors
    error = IO.Error.NoSuchFile(Some(Paths.get("/some/path")), None)
    exception = new NoSuchFileException("/some/path")
    error.exception.getMessage shouldBe exception.getMessage
    IO.Error(exception) shouldBe a[IO.Error.NoSuchFile]

    error = IO.Error.FileNotFound(new FileNotFoundException("some_file.sh"))
    exception = new FileNotFoundException("some_file.sh")
    error.exception.getMessage shouldBe exception.getMessage
    IO.Error(exception) shouldBe a[IO.Error.FileNotFound]

    error = IO.Error.AsynchronousClose(new AsynchronousCloseException())
    exception = new AsynchronousCloseException()
    error.exception.getMessage shouldBe exception.getMessage
    IO.Error(exception) shouldBe a[IO.Error.AsynchronousClose]

    error = IO.Error.ClosedChannel(new ClosedChannelException())
    exception = new ClosedChannelException()
    error.exception.getMessage shouldBe exception.getMessage
    IO.Error(exception) shouldBe a[IO.Error.ClosedChannel]

    error = IO.Error.NullMappedByteBuffer(NullMappedByteBuffer(new NullPointerException(), Reserve()))
    exception = IO.Exception.NullMappedByteBuffer(new NullPointerException(), Reserve())
    IO.Error(exception) shouldBe a[IO.Error.NullMappedByteBuffer]

    error = IO.Error.DecompressingIndex(Reserve())
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    IO.Error(exception) shouldBe a[IO.Error.DecompressingIndex]

    error = IO.Error.DecompressingValues(Reserve())
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    IO.Error(exception) shouldBe a[IO.Error.DecompressingValues]

    error = IO.Error.ReadingHeader(Reserve())
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    IO.Error(exception) shouldBe a[IO.Error.ReadingHeader]

    error = IO.Error.ReservedValue(Reserve())
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    IO.Error(exception) shouldBe a[IO.Error.ReservedValue]

    //OTHER Errors
    error = IO.Error.OverlappingPushSegment
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    IO.Error(exception) shouldBe IO.Error.OverlappingPushSegment

    error = IO.Error.NoSegmentsRemoved
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    IO.Error(exception) shouldBe IO.Error.NoSegmentsRemoved

    error = IO.Error.NotSentToNextLevel
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    IO.Error(exception) shouldBe IO.Error.NotSentToNextLevel

    error = IO.Error.ReceivedKeyValuesToMergeWithoutTargetSegment(1)
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    IO.Error(exception) shouldBe IO.Error.ReceivedKeyValuesToMergeWithoutTargetSegment(1)

    error = IO.Error.ReadOnlyBuffer(new ReadOnlyBufferException)
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    IO.Error(exception) shouldBe a[IO.Error.ReadOnlyBuffer]

    error = IO.Error.Fatal(new Exception("Something bad happened"))
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    IO.Error(exception) shouldBe a[IO.Error.Fatal]
  }
}
