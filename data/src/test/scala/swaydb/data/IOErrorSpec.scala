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
import swaydb.data.io.Core
import swaydb.data.io.Core.IO.Exception.NullMappedByteBuffer

class IOErrorSpec extends FlatSpec with Matchers {

  it should "convert known Exception to known Error types and vice versa" in {

    var error: Core.IO.Error = Core.IO.Error.OpeningFile(Paths.get("/some/path"), Reserve())
    var exception: Throwable = Core.IO.Exception.OpeningFile(Paths.get("/some/path"), Reserve())
    error.exception.getMessage shouldBe exception.getMessage
    Core.IO.Error(exception) shouldBe a[Core.IO.Error.OpeningFile]

    //BUSY Errors
    error = Core.IO.Error.NoSuchFile(Some(Paths.get("/some/path")), None)
    exception = new NoSuchFileException("/some/path")
    error.exception.getMessage shouldBe exception.getMessage
    Core.IO.Error(exception) shouldBe a[Core.IO.Error.NoSuchFile]

    error = Core.IO.Error.FileNotFound(new FileNotFoundException("some_file.sh"))
    exception = new FileNotFoundException("some_file.sh")
    error.exception.getMessage shouldBe exception.getMessage
    Core.IO.Error(exception) shouldBe a[Core.IO.Error.FileNotFound]

    error = Core.IO.Error.AsynchronousClose(new AsynchronousCloseException())
    exception = new AsynchronousCloseException()
    error.exception.getMessage shouldBe exception.getMessage
    Core.IO.Error(exception) shouldBe a[Core.IO.Error.AsynchronousClose]

    error = Core.IO.Error.ClosedChannel(new ClosedChannelException())
    exception = new ClosedChannelException()
    error.exception.getMessage shouldBe exception.getMessage
    Core.IO.Error(exception) shouldBe a[Core.IO.Error.ClosedChannel]

    error = Core.IO.Error.NullMappedByteBuffer(NullMappedByteBuffer(new NullPointerException(), Reserve()))
    exception = Core.IO.Exception.NullMappedByteBuffer(new NullPointerException(), Reserve())
    Core.IO.Error(exception) shouldBe a[Core.IO.Error.NullMappedByteBuffer]

    error = Core.IO.Error.DecompressingIndex(Reserve())
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.IO.Error(exception) shouldBe a[Core.IO.Error.DecompressingIndex]

    error = Core.IO.Error.DecompressingValues(Reserve())
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.IO.Error(exception) shouldBe a[Core.IO.Error.DecompressingValues]

    error = Core.IO.Error.ReadingHeader(Reserve())
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.IO.Error(exception) shouldBe a[Core.IO.Error.ReadingHeader]

    error = Core.IO.Error.ReservedValue(Reserve())
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.IO.Error(exception) shouldBe a[Core.IO.Error.ReservedValue]

    //OTHER Errors
    error = Core.IO.Error.OverlappingPushSegment
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.IO.Error(exception) shouldBe Core.IO.Error.OverlappingPushSegment

    error = Core.IO.Error.NoSegmentsRemoved
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.IO.Error(exception) shouldBe Core.IO.Error.NoSegmentsRemoved

    error = Core.IO.Error.NotSentToNextLevel
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.IO.Error(exception) shouldBe Core.IO.Error.NotSentToNextLevel

    error = Core.IO.Error.ReceivedKeyValuesToMergeWithoutTargetSegment(1)
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.IO.Error(exception) shouldBe Core.IO.Error.ReceivedKeyValuesToMergeWithoutTargetSegment(1)

    error = Core.IO.Error.ReadOnlyBuffer(new ReadOnlyBufferException)
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.IO.Error(exception) shouldBe a[Core.IO.Error.ReadOnlyBuffer]

    error = Core.IO.Error.Fatal(new Exception("Something bad happened"))
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.IO.Error(exception) shouldBe a[Core.IO.Error.Fatal]
  }
}
