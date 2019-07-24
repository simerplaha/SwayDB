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
import swaydb.data.io.Core.Exception.NullMappedByteBuffer

class IOErrorSpec extends FlatSpec with Matchers {

  it should "convert known Exception to known Error types and vice versa" in {

    var error: Core.Error = Core.Error.OpeningFile(Paths.get("/some/path"), Reserve())
    var exception: Throwable = Core.Exception.OpeningFile(Paths.get("/some/path"), Reserve())
    error.exception.getMessage shouldBe exception.getMessage
    Core.Error(exception) shouldBe a[Core.Error.OpeningFile]

    //BUSY Errors
    error = Core.Error.NoSuchFile(Some(Paths.get("/some/path")), None)
    exception = new NoSuchFileException("/some/path")
    error.exception.getMessage shouldBe exception.getMessage
    Core.Error(exception) shouldBe a[Core.Error.NoSuchFile]

    error = Core.Error.FileNotFound(new FileNotFoundException("some_file.sh"))
    exception = new FileNotFoundException("some_file.sh")
    error.exception.getMessage shouldBe exception.getMessage
    Core.Error(exception) shouldBe a[Core.Error.FileNotFound]

    error = Core.Error.AsynchronousClose(new AsynchronousCloseException())
    exception = new AsynchronousCloseException()
    error.exception.getMessage shouldBe exception.getMessage
    Core.Error(exception) shouldBe a[Core.Error.AsynchronousClose]

    error = Core.Error.ClosedChannel(new ClosedChannelException())
    exception = new ClosedChannelException()
    error.exception.getMessage shouldBe exception.getMessage
    Core.Error(exception) shouldBe a[Core.Error.ClosedChannel]

    error = Core.Error.NullMappedByteBuffer(NullMappedByteBuffer(new NullPointerException(), Reserve()))
    exception = Core.Exception.NullMappedByteBuffer(new NullPointerException(), Reserve())
    Core.Error(exception) shouldBe a[Core.Error.NullMappedByteBuffer]

    error = Core.Error.DecompressingIndex(Reserve())
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.Error(exception) shouldBe a[Core.Error.DecompressingIndex]

    error = Core.Error.DecompressingValues(Reserve())
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.Error(exception) shouldBe a[Core.Error.DecompressingValues]

    error = Core.Error.ReadingHeader(Reserve())
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.Error(exception) shouldBe a[Core.Error.ReadingHeader]

    error = Core.Error.ReservedValue(Reserve())
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.Error(exception) shouldBe a[Core.Error.ReservedValue]

    //OTHER Errors
    error = Core.Error.OverlappingPushSegment
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.Error(exception) shouldBe Core.Error.OverlappingPushSegment

    error = Core.Error.NoSegmentsRemoved
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.Error(exception) shouldBe Core.Error.NoSegmentsRemoved

    error = Core.Error.NotSentToNextLevel
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.Error(exception) shouldBe Core.Error.NotSentToNextLevel

    error = Core.Error.ReceivedKeyValuesToMergeWithoutTargetSegment(1)
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.Error(exception) shouldBe Core.Error.ReceivedKeyValuesToMergeWithoutTargetSegment(1)

    error = Core.Error.ReadOnlyBuffer(new ReadOnlyBufferException)
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.Error(exception) shouldBe a[Core.Error.ReadOnlyBuffer]

    error = Core.Error.Fatal(new Exception("Something bad happened"))
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    Core.Error(exception) shouldBe a[Core.Error.Fatal]
  }
}
