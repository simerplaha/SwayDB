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
import swaydb.Exception.NullMappedByteBuffer

class IOErrorSpec extends FlatSpec with Matchers {

  it should "convert known Exception to known Error types and vice versa" in {

    var error: swaydb.Error = swaydb.Error.OpeningFile(Paths.get("/some/path"), Reserve())
    var exception: Throwable = swaydb.Exception.OpeningFile(Paths.get("/some/path"), Reserve())
    error.exception.getMessage shouldBe exception.getMessage
    swaydb.Error(exception) shouldBe a[swaydb.Error.OpeningFile]

    //BUSY Errors
    error = swaydb.Error.NoSuchFile(Some(Paths.get("/some/path")), None)
    exception = new NoSuchFileException("/some/path")
    error.exception.getMessage shouldBe exception.getMessage
    swaydb.Error(exception) shouldBe a[swaydb.Error.NoSuchFile]

    error = swaydb.Error.FileNotFound(new FileNotFoundException("some_file.sh"))
    exception = new FileNotFoundException("some_file.sh")
    error.exception.getMessage shouldBe exception.getMessage
    swaydb.Error(exception) shouldBe a[swaydb.Error.FileNotFound]

    error = swaydb.Error.AsynchronousClose(new AsynchronousCloseException())
    exception = new AsynchronousCloseException()
    error.exception.getMessage shouldBe exception.getMessage
    swaydb.Error(exception) shouldBe a[swaydb.Error.AsynchronousClose]

    error = swaydb.Error.ClosedChannel(new ClosedChannelException())
    exception = new ClosedChannelException()
    error.exception.getMessage shouldBe exception.getMessage
    swaydb.Error(exception) shouldBe a[swaydb.Error.ClosedChannel]

    error = swaydb.Error.NullMappedByteBuffer(NullMappedByteBuffer(new NullPointerException(), Reserve()))
    exception = swaydb.Exception.NullMappedByteBuffer(new NullPointerException(), Reserve())
    swaydb.Error(exception) shouldBe a[swaydb.Error.NullMappedByteBuffer]

    error = swaydb.Error.DecompressingIndex(Reserve())
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    swaydb.Error(exception) shouldBe a[swaydb.Error.DecompressingIndex]

    error = swaydb.Error.DecompressingValues(Reserve())
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    swaydb.Error(exception) shouldBe a[swaydb.Error.DecompressingValues]

    error = swaydb.Error.ReadingHeader(Reserve())
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    swaydb.Error(exception) shouldBe a[swaydb.Error.ReadingHeader]

    error = swaydb.Error.ReservedValue(Reserve())
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    swaydb.Error(exception) shouldBe a[swaydb.Error.ReservedValue]

    //OTHER Errors
    error = swaydb.Error.OverlappingPushSegment
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    swaydb.Error(exception) shouldBe swaydb.Error.OverlappingPushSegment

    error = swaydb.Error.NoSegmentsRemoved
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    swaydb.Error(exception) shouldBe swaydb.Error.NoSegmentsRemoved

    error = swaydb.Error.NotSentToNextLevel
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    swaydb.Error(exception) shouldBe swaydb.Error.NotSentToNextLevel

    error = swaydb.Error.ReceivedKeyValuesToMergeWithoutTargetSegment(1)
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    swaydb.Error(exception) shouldBe swaydb.Error.ReceivedKeyValuesToMergeWithoutTargetSegment(1)

    error = swaydb.Error.ReadOnlyBuffer(new ReadOnlyBufferException)
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    swaydb.Error(exception) shouldBe a[swaydb.Error.ReadOnlyBuffer]

    error = swaydb.Error.Fatal(new Exception("Something bad happened"))
    exception = error.exception
    error.exception.getMessage shouldBe exception.getMessage
    swaydb.Error(exception) shouldBe a[swaydb.Error.Fatal]
  }
}
