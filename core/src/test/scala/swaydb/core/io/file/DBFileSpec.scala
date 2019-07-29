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

package swaydb.core.io.file

import java.nio.ReadOnlyBufferException
import java.nio.channels.{NonReadableChannelException, NonWritableChannelException}
import java.nio.file.{FileAlreadyExistsException, NoSuchFileException}

import org.scalamock.scalatest.MockFactory
import swaydb.Error.Segment.ErrorHandler
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.queue.{FileLimiter, FileLimiterItem}
import swaydb.core.util.Benchmark
import swaydb.core.util.PipeOps._
import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.data.slice.Slice

class DBFileSpec extends TestBase with Benchmark with MockFactory {

  implicit val fileOpenLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter

  "DBFile.write" should {
    "write bytes to a File" in {
      val testFile = randomFilePath
      val bytes = Slice(randomBytes())

      DBFile.write(testFile, bytes).valueIOGet
      DBFile.mmapRead(testFile, autoClose = false).valueIOGet ==> {
        file =>
          file.readAll.valueIOGet shouldBe bytes
          file.close.valueIOGet
      }
      DBFile.channelRead(testFile, autoClose = false).valueIOGet ==> {
        file =>
          file.readAll.valueIOGet shouldBe bytes
          file.close.valueIOGet
      }
    }

    "write empty bytes to a File" in {
      val testFile = randomFilePath
      val bytes = Slice.emptyBytes

      DBFile.write(testFile, bytes).valueIOGet
      DBFile.mmapRead(testFile, autoClose = false).valueIOGet ==> {
        file =>
          file.readAll.valueIOGet shouldBe empty
          file.close.valueIOGet
      }
      IOEffect.exists(testFile) shouldBe true
    }

    "fail to write bytes if the Slice contains empty bytes" in {
      val testFile = randomFilePath
      val bytes = Slice.create[Byte](10)
      bytes.addIntUnsigned(1)
      bytes.addIntUnsigned(2)

      bytes.size shouldBe 2

      DBFile.write(testFile, bytes).failed.valueIOGet.exception shouldBe swaydb.Exception.FailedToWriteAllBytes(10, 2, bytes.size)
    }

    "fail to write if the file already exists" in {
      val testFile = randomFilePath
      val bytes = randomBytesSlice()

      DBFile.write(testFile, bytes).valueIOGet
      DBFile.write(testFile, bytes).failed.valueIOGet.exception shouldBe a[FileAlreadyExistsException] //creating the same file again should fail
      //file remains unchanged
      DBFile.channelRead(testFile, autoClose = false).valueIOGet ==> {
        file =>
          file.readAll.valueIOGet shouldBe bytes
          file.close.valueIOGet
      }
    }
  }

  "DBFile.channelWrite" should {
    "initialise a FileChannel for writing and not reading and invoke the onOpen function on open" in {
      val testFile = randomFilePath
      val bytes = randomBytesSlice()

      //opening a file should trigger the onOpen function
      implicit val fileOpenLimiter = mock[FileLimiter]

      fileOpenLimiter.close _ expects * onCall {
        dbFile: FileLimiterItem =>
          dbFile.path shouldBe testFile
          dbFile.isOpen shouldBe true
          ()
      } repeat 3.times

      val file = DBFile.channelWrite(testFile, autoClose = true).valueIOGet
      //above onOpen is also invoked
      file.isFileDefined shouldBe true //file is set
      file.isOpen shouldBe true
      file.append(bytes).valueIOGet

      file.readAll.failed.valueIOGet.exception shouldBe a[NonReadableChannelException]
      file.read(0, 1).failed.valueIOGet.exception shouldBe a[NonReadableChannelException]
      file.get(0).failed.valueIOGet.exception shouldBe a[NonReadableChannelException]

      //closing the channel and reopening it will open it in read only mode
      file.close.valueIOGet
      file.isFileDefined shouldBe false
      file.isOpen shouldBe false
      file.readAll.valueIOGet shouldBe bytes //read
      //above onOpen is also invoked
      file.isFileDefined shouldBe true
      file.isOpen shouldBe true
      //cannot write to a reopened file channel. Ones closed! It cannot be reopened for writing.
      file.append(bytes).failed.valueIOGet.exception shouldBe a[NonWritableChannelException]

      file.close.valueIOGet

      DBFile.channelRead(testFile, autoClose = true).valueIOGet ==> {
        file =>
          file.readAll.valueIOGet shouldBe bytes
          file.close.valueIOGet
      }
      //above onOpen is also invoked
    }

    "fail write if the slice is partially written" in {
      val testFile = randomFilePath
      val bytes = Slice.create[Byte](10)
      bytes.addIntUnsigned(1)
      bytes.addIntUnsigned(2)

      bytes.size shouldBe 2

      val channelFile = DBFile.channelWrite(testFile, autoClose = true).valueIOGet
      channelFile.append(bytes).failed.valueIOGet.exception shouldBe swaydb.Exception.FailedToWriteAllBytes(10, 2, bytes.size)
      channelFile.close.valueIOGet
    }

    "fail initialisation if the file already exists" in {
      val testFile = randomFilePath

      DBFile.channelWrite(testFile, autoClose = true).valueIOGet ==> {
        file =>
          file.existsOnDisk shouldBe true
          file.close.valueIOGet
      }
      //creating the same file again should fail
      DBFile.channelWrite(testFile, autoClose = true).failed.valueIOGet.exception.toString shouldBe new FileAlreadyExistsException(testFile.toString).toString
      //file remains unchanged
      DBFile.channelRead(testFile, autoClose = true).valueIOGet ==> {
        file =>
          file.readAll.valueIOGet shouldBe empty
          file.close.valueIOGet
      }
    }
  }

  "DBFile.channelRead" should {
    "initialise a FileChannel for reading only" in {
      val testFile = randomFilePath
      val bytes = randomBytesSlice()

      //opening a file should trigger the onOpen function
      implicit val fileOpenLimiter = mock[FileLimiter]
      fileOpenLimiter.close _ expects * onCall {
        dbFile: FileLimiterItem =>
          dbFile.path shouldBe testFile
          dbFile.isOpen shouldBe true
          ()
      } repeat 3.times

      IOEffect.write(testFile, bytes).valueIOGet

      val readFile = DBFile.channelRead(testFile, autoClose = true).valueIOGet
      //reading a file should load the file lazily
      readFile.isFileDefined shouldBe false
      readFile.isOpen shouldBe false
      //reading the opens the file
      readFile.readAll.valueIOGet shouldBe bytes
      //file is now opened
      readFile.isFileDefined shouldBe true
      readFile.isOpen shouldBe true

      //writing fails since the file is readonly
      readFile.append(bytes).failed.valueIOGet.exception shouldBe a[NonWritableChannelException]
      //data remain unchanged
      DBFile.channelRead(testFile, autoClose = true).valueIOGet.readAll.valueIOGet shouldBe bytes

      readFile.close.valueIOGet
      readFile.isOpen shouldBe false
      readFile.isFileDefined shouldBe false
      //read bytes one by one
      (0 until bytes.size) foreach {
        index =>
          readFile.get(index).valueIOGet shouldBe bytes(index)
      }
      readFile.isOpen shouldBe true

      readFile.close.valueIOGet
    }

    "fail initialisation if the file does not exists" in {
      DBFile.channelRead(randomFilePath, autoClose = true).failed.valueIOGet.exception shouldBe a[NoSuchFileException]
    }
  }

  "DBFile.mmapWriteAndRead" should {
    "write bytes to a File, extend the buffer on overflow and reopen it for reading via mmapRead" in {
      val testFile = randomFilePath
      val bytes = Slice("bytes one".getBytes())

      //opening a file should trigger the onOpen function
      implicit val fileOpenLimiter = mock[FileLimiter]
      fileOpenLimiter.close _ expects * onCall {
        dbFile: FileLimiterItem =>
          dbFile.path shouldBe testFile
          dbFile.isOpen shouldBe true
          ()
      } repeat 3.times

      val file = DBFile.mmapWriteAndRead(testFile, autoClose = true, bytes).valueIOGet
      file.readAll.valueIOGet shouldBe bytes
      file.isFull.valueIOGet shouldBe true

      //overflow bytes
      val bytes2 = Slice("bytes two".getBytes())
      file.append(bytes2).valueIOGet
      file.isFull.valueIOGet shouldBe true //complete fit - no extra bytes

      //overflow bytes
      val bytes3 = Slice("bytes three".getBytes())
      file.append(bytes3).valueIOGet
      file.isFull.valueIOGet shouldBe true //complete fit - no extra bytes

      val expectedBytes = bytes ++ bytes2 ++ bytes3

      file.readAll.valueIOGet shouldBe expectedBytes

      //close buffer
      file.close.valueIOGet
      file.isFileDefined shouldBe false
      file.isOpen shouldBe false
      file.readAll.valueIOGet shouldBe expectedBytes
      file.isFileDefined shouldBe true
      file.isOpen shouldBe true

      //writing fails since the file is now readonly
      file.append(bytes).failed.valueIOGet.exception shouldBe a[ReadOnlyBufferException]
      file.close.valueIOGet

      //open read only buffer
      DBFile.mmapRead(testFile, autoClose = true).valueIOGet ==> {
        file =>
          file.readAll.valueIOGet shouldBe expectedBytes
          file.close.valueIOGet
      }
    }

    "fail write if the slice is partially written" in {
      val testFile = randomFilePath
      val bytes = Slice.create[Byte](10)
      bytes.addIntUnsigned(1)
      bytes.addIntUnsigned(2)

      bytes.size shouldBe 2

      DBFile.mmapWriteAndRead(testFile, autoClose = true, bytes).failed.valueIOGet.exception shouldBe swaydb.Exception.FailedToWriteAllBytes(0, 2, bytes.size)
    }

    "fail to write if the file already exists" in {
      val testFile = randomFilePath
      val bytes = randomBytesSlice()

      DBFile.mmapWriteAndRead(testFile, autoClose = true, bytes).valueIOGet.close.valueIOGet
      DBFile.mmapWriteAndRead(testFile, autoClose = true, bytes).failed.valueIOGet.exception shouldBe a[FileAlreadyExistsException] //creating the same file again should fail
      //file remains unchanged
      DBFile.mmapRead(testFile, autoClose = true).valueIOGet ==> {
        file =>
          file.readAll.valueIOGet shouldBe bytes
          file.close.valueIOGet
      }
    }
  }

  "DBFile.mmapRead" should {
    "open an existing file for reading" in {
      val testFile = randomFilePath
      val bytes = Slice("bytes one".getBytes())

      DBFile.write(testFile, bytes).valueIOGet

      val readFile = DBFile.mmapRead(testFile, autoClose = true).valueIOGet

      def doRead = {
        readFile.isFileDefined shouldBe false //reading a file should load the file lazily
        readFile.isOpen shouldBe false
        readFile.readAll.valueIOGet shouldBe bytes
        readFile.isFileDefined shouldBe true
        readFile.isOpen shouldBe true
      }

      doRead

      //close and read again
      readFile.close.valueIOGet
      doRead

      DBFile.write(testFile, bytes).failed.valueIOGet.exception shouldBe a[FileAlreadyExistsException] //creating the same file again should fail

      readFile.close.valueIOGet
    }

    "fail to read if the file does not exists" in {
      DBFile.mmapRead(randomFilePath, autoClose = true).failed.valueIOGet.exception shouldBe a[NoSuchFileException]
    }
  }

  "DBFile.mmapInit" should {
    "open a file for writing" in {
      val testFile = randomFilePath
      val bytes1 = Slice("bytes one".getBytes())
      val bytes2 = Slice("bytes two".getBytes())
      val bytes3 = Slice("bytes three".getBytes())
      val bytes4 = Slice("bytes four".getBytes())

      val file = DBFile.mmapInit(testFile, bytes1.size + bytes2.size + bytes3.size, autoClose = true).valueIOGet
      file.append(bytes1).valueIOGet
      file.isFull.valueIOGet shouldBe false
      file.append(bytes2).valueIOGet
      file.isFull.valueIOGet shouldBe false
      file.append(bytes3).valueIOGet
      file.isFull.valueIOGet shouldBe true
      file.append(bytes4).valueIOGet //overflow write, buffer gets extended
      file.isFull.valueIOGet shouldBe true

      file.readAll.valueIOGet shouldBe (bytes1 ++ bytes2 ++ bytes3 ++ bytes4)

      file.close.valueIOGet
    }

    "fail to initialise if it already exists" in {
      val testFile = randomFilePath
      DBFile.write(testFile, Slice(randomBytes())).valueIOGet

      DBFile.mmapInit(testFile, 10, autoClose = true).failed.valueIOGet.exception shouldBe a[FileAlreadyExistsException]
    }
  }

  "DBFile.close" should {
    "close a file channel and reopen on read" in {
      val testFile = randomFilePath
      val bytes = randomBytesSlice()

      //opening a file should trigger the onOpen function
      implicit val fileOpenLimiter = mock[FileLimiter]
      fileOpenLimiter.close _ expects * onCall {
        (dbFile: FileLimiterItem) =>
          dbFile.path shouldBe testFile
          dbFile.isOpen shouldBe true
          ()
      } repeat 4.times

      val file = DBFile.channelWrite(testFile, autoClose = true).valueIOGet
      file.append(bytes).valueIOGet

      def close = {
        file.close.valueIOGet
        file.isOpen shouldBe false
        file.isFileDefined shouldBe false
        file.existsOnDisk shouldBe true
      }

      def open = {
        file.read(0, bytes.size).valueIOGet shouldBe bytes
        file.isOpen shouldBe true
        file.isFileDefined shouldBe true
      }

      close
      //closing an already closed channel should not fail
      close
      open

      close
      open

      close
      open

      close
    }

    "close a memory mapped file and reopen on read" in {
      val testFile = randomFilePath
      val bytes = randomBytesSlice()

      val file = DBFile.mmapInit(testFile, bytes.size, autoClose = true).valueIOGet
      file.append(bytes).valueIOGet

      def close = {
        file.close.valueIOGet
        file.isOpen shouldBe false
        file.isFileDefined shouldBe false
        file.existsOnDisk shouldBe true
      }

      def open = {
        file.read(0, bytes.size).valueIOGet shouldBe bytes
        file.isOpen shouldBe true
        file.isFileDefined shouldBe true
      }

      //closing multiple times should not fail
      close
      close
      close

      open

      close
      open

      close
      open

      close
    }

    "close a FileChannel and then reopening the file should open it in read only mode" in {
      val testFile = randomFilePath
      val bytes = randomBytesSlice()

      val file = DBFile.channelWrite(testFile, autoClose = true).valueIOGet
      file.append(bytes).valueIOGet
      file.close.valueIOGet

      file.append(bytes).failed.valueIOGet.exception shouldBe a[NonWritableChannelException]
      file.readAll.valueIOGet shouldBe bytes

      file.close.valueIOGet
    }

    "close that MMAPFile and reopening the file should open it in read only mode" in {
      val testFile = randomFilePath
      val bytes = randomBytesSlice()

      val file = DBFile.mmapInit(testFile, bytes.size, autoClose = true).valueIOGet
      file.append(bytes).valueIOGet
      file.close.valueIOGet

      file.append(bytes).failed.valueIOGet.exception shouldBe a[ReadOnlyBufferException]
      file.readAll.valueIOGet shouldBe bytes

      file.close.valueIOGet
    }
  }

  "DBFile.append" should {
    "append bytes to the end of the ChannelFile" in {
      val testFile = randomFilePath
      val bytes = List(randomBytesSlice(), randomBytesSlice(), randomBytesSlice())

      val file = DBFile.channelWrite(testFile, autoClose = true).valueIOGet
      file.append(bytes(0)).valueIOGet
      file.append(bytes(1)).valueIOGet
      file.append(bytes(2)).valueIOGet
      file.read(0, 1).isFailure shouldBe true //not open for read

      file.close.valueIOGet

      val expectedAllBytes = bytes.foldLeft(List.empty[Byte])(_ ++ _).toSlice

      DBFile.channelRead(testFile, autoClose = true).valueIOGet ==> {
        file =>
          file.readAll.valueIOGet shouldBe expectedAllBytes
          file.close.valueIOGet
      }
      DBFile.mmapRead(testFile, autoClose = true).valueIOGet ==> {
        file =>
          file.readAll.valueIOGet shouldBe expectedAllBytes
          file.close.valueIOGet
      }

      file.close.valueIOGet
    }

    "append bytes to the end of the MMAP file" in {
      val testFile = randomFilePath
      val bytes = List(randomBytesSlice(), randomBytesSlice(), randomBytesSlice())

      val allBytesSize = bytes.foldLeft(0)(_ + _.size)
      val file = DBFile.mmapInit(testFile, allBytesSize, autoClose = true).valueIOGet
      file.append(bytes(0)).valueIOGet
      file.append(bytes(1)).valueIOGet
      file.append(bytes(2)).valueIOGet
      file.get(0).valueIOGet shouldBe bytes.head.head
      file.get(allBytesSize - 1).valueIOGet shouldBe bytes.last.last

      val expectedAllBytes = bytes.foldLeft(List.empty[Byte])(_ ++ _).toSlice

      file.readAll.valueIOGet shouldBe expectedAllBytes
      file.close.valueIOGet //close

      //reopen
      DBFile.mmapRead(testFile, autoClose = true).valueIOGet ==> {
        file =>
          file.readAll.valueIOGet shouldBe expectedAllBytes
          file.close.valueIOGet
      }
      DBFile.channelRead(testFile, autoClose = true).valueIOGet ==> {
        file =>
          file.readAll.valueIOGet shouldBe expectedAllBytes
          file.close.valueIOGet
      }
    }

    "append bytes by extending an overflown buffer of MMAP file" in {
      val testFile = randomFilePath
      val bytes = List(randomBytesSlice(), randomBytesSlice(), randomBytesSlice(), randomBytesSlice(), randomBytesSlice())
      val allBytesSize = bytes.foldLeft(0)(_ + _.size)

      val file = DBFile.mmapInit(testFile, bytes.head.size, autoClose = true).valueIOGet
      file.append(bytes(0)).valueIOGet
      file.append(bytes(1)).valueIOGet
      file.append(bytes(2)).valueIOGet
      file.append(bytes(3)).valueIOGet
      file.append(bytes(4)).valueIOGet
      file.get(0).valueIOGet shouldBe bytes.head.head
      file.get(allBytesSize - 1).valueIOGet shouldBe bytes.last.last

      val expectedAllBytes = bytes.foldLeft(List.empty[Byte])(_ ++ _).toSlice

      file.readAll.valueIOGet shouldBe expectedAllBytes
      file.close.valueIOGet //close

      //reopen
      DBFile.mmapRead(testFile, autoClose = true).valueIOGet ==> {
        file =>
          file.readAll.valueIOGet shouldBe expectedAllBytes
          file.close.valueIOGet
      }
      DBFile.channelRead(testFile, autoClose = true).valueIOGet ==> {
        file =>
          file.readAll.valueIOGet shouldBe expectedAllBytes
          file.close.valueIOGet
      }
    }

    "not fail when appending empty bytes to ChannelFile" in {
      val file = DBFile.channelWrite(randomFilePath, autoClose = true).valueIOGet
      file.append(Slice.emptyBytes).valueIOGet
      DBFile.channelRead(file.path, autoClose = true).valueIOGet ==> {
        file =>
          file.readAll.valueIOGet shouldBe empty
          file.close.valueIOGet
      }
      file.close.valueIOGet
    }

    "not fail when appending empty bytes to MMAPFile" in {
      val file = DBFile.mmapInit(randomFilePath, 100, autoClose = true).valueIOGet
      file.append(Slice.emptyBytes).valueIOGet
      file.readAll.valueIOGet shouldBe Slice.fill(file.fileSize.get.toInt)(0)
      file.close.valueIOGet

      DBFile.mmapRead(file.path, autoClose = true).valueIOGet ==> {
        file2 =>
          file2.readAll.valueIOGet shouldBe Slice.fill(file.fileSize.get.toInt)(0)
          file2.close.valueIOGet
      }
    }
  }

  "DBFile.read and find" should {
    "read and find bytes at a position from a ChannelFile" in {
      val testFile = randomFilePath
      val bytes = randomBytesSlice(100)

      val file = DBFile.channelWrite(testFile, autoClose = true).valueIOGet
      file.append(bytes).valueIOGet
      file.read(0, 1).isFailure shouldBe true //not open for read

      file.close.valueIOGet

      val readFile = DBFile.channelRead(testFile, autoClose = true).valueIOGet

      (0 until bytes.size) foreach {
        index =>
          readFile.read(index, 1).valueIOGet should contain only bytes(index)
      }

      readFile.read(0, bytes.size / 2).valueIOGet.toList should contain theSameElementsInOrderAs bytes.dropRight(bytes.size / 2).toList
      readFile.read(bytes.size / 2, bytes.size / 2).valueIOGet.toList should contain theSameElementsInOrderAs bytes.drop(bytes.size / 2).toList
      readFile.get(1000).valueIOGet shouldBe 0

      readFile.close.valueIOGet
    }
  }

  "DBFile.memory" should {
    "create an immutable DBFile" in {
      val path = randomFilePath
      val bytes = randomBytesSlice(100)

      val file = DBFile.memory(path, bytes, autoClose = true).valueIOGet
      //cannot write to a memory file as it's immutable
      file.append(bytes).failed.valueIOGet.exception shouldBe a[UnsupportedOperationException]
      file.isFull.valueIOGet shouldBe true
      file.isOpen shouldBe true
      file.existsOnDisk shouldBe false

      file.readAll.valueIOGet shouldBe bytes

      (0 until bytes.size) foreach {
        index =>
          val readBytes = file.read(index, 1).valueIOGet
          readBytes.underlyingArraySize shouldBe bytes.size
          readBytes.head shouldBe bytes(index)
          file.get(index).valueIOGet shouldBe bytes(index)
      }

      file.close.valueIOGet
    }

    "exist in memory after being closed" in {
      val path = randomFilePath
      val bytes = randomBytesSlice(100)

      val file = DBFile.memory(path, bytes, autoClose = true).valueIOGet
      file.isFull.valueIOGet shouldBe true
      file.isOpen shouldBe true
      file.existsOnDisk shouldBe false
      file.isFileDefined shouldBe true
      file.fileSize.valueIOGet shouldBe bytes.size

      file.close.valueIOGet

      file.isFull.valueIOGet shouldBe true
      //in memory files are never closed
      file.isOpen shouldBe true
      file.existsOnDisk shouldBe false
      //memory files are not remove from DBFile's reference when they closed.
      file.isFileDefined shouldBe true
      file.fileSize.valueIOGet shouldBe bytes.size

      //reading an in-memory file
      file.readAll.valueIOGet shouldBe bytes
      //      file.isInitialised shouldBe true

      file.close.valueIOGet
    }
  }

  "DBFile.delete" should {
    "delete a ChannelFile" in {
      val bytes = randomBytesSlice(100)

      val file = DBFile.channelWrite(randomFilePath, autoClose = true).valueIOGet
      file.append(bytes).valueIOGet

      file.delete().valueIOGet
      file.existsOnDisk shouldBe false
      file.isOpen shouldBe false
      file.isFileDefined shouldBe false
    }

    "delete a MMAPFile" in {
      val file = DBFile.mmapWriteAndRead(randomFilePath, autoClose = true, randomBytesSlice()).valueIOGet
      file.close.valueIOGet

      file.delete().valueIOGet
      file.existsOnDisk shouldBe false
      file.isOpen shouldBe false
      file.isFileDefined shouldBe false
    }

    "delete a MemoryFile" in {
      val file = DBFile.memory(randomFilePath, randomBytesSlice(), autoClose = true).valueIOGet
      file.close.valueIOGet

      file.delete().valueIOGet
      file.existsOnDisk shouldBe false
      file.isOpen shouldBe false
      file.isFileDefined shouldBe false
      //bytes are nulled to be garbage collected
      file.get(0).failed.valueIOGet.exception shouldBe a[NoSuchFileException]
      file.isOpen shouldBe false
    }
  }

  "DBFile.copy" should {
    "copy a ChannelFile" in {
      val bytes = randomBytesSlice(100)

      val file = DBFile.channelWrite(randomFilePath, autoClose = true).valueIOGet
      file.append(bytes).valueIOGet

      val targetFile = randomFilePath
      file.copyTo(targetFile).valueIOGet shouldBe targetFile

      DBFile.channelRead(targetFile, autoClose = true).valueIOGet ==> {
        file =>
          file.readAll.valueIOGet shouldBe bytes
          file.close.valueIOGet
      }

      file.close.valueIOGet
    }

    "copy a MMAPFile" in {
      val bytes = randomBytesSlice(100)

      val file = DBFile.mmapInit(randomFilePath, bytes.size, autoClose = true).valueIOGet
      file.append(bytes).valueIOGet
      file.isFull.valueIOGet shouldBe true
      file.close.valueIOGet

      val targetFile = randomFilePath
      file.copyTo(targetFile).valueIOGet shouldBe targetFile

      DBFile.channelRead(targetFile, autoClose = true).valueIOGet ==> {
        file =>
          file.readAll.valueIOGet shouldBe bytes
          file.close.valueIOGet
      }
    }

    "fail when copying a MemoryFile" in {
      val bytes = randomBytesSlice(100)
      val file = DBFile.memory(randomFilePath, bytes, autoClose = true).valueIOGet

      file.copyTo(randomFilePath).failed.valueIOGet.exception shouldBe swaydb.Exception.CannotCopyInMemoryFiles(file.path)
    }
  }

  //  "Concurrently opening files" should {
  //    "result in Busy exception" in {
  //      //create a file
  //      val bytes = Slice(randomBytes())
  //      val file = DBFile.mmapInit(randomFilePath, bytes.size, autoClose = true).runIO
  //      file.append(bytes).runIO
  //
  //      //concurrently close and read the same file.
  //      val ios =
  //        (1 to 500).par map {
  //          _ =>
  //            if (randomBoolean) Future(file.close)
  //            file.readAll
  //        }
  //
  //      //convert all failures to Async
  //      val result: List[IO.Later[_]] =
  //        ios.toList collect {
  //          case io: IO.Failure[_] =>
  //            io.recoverToAsync(IO.Async((), swaydb.Error.None)).asInstanceOf[IO.Later[_]]
  //        }
  //
  //      result.size should be >= 1
  //
  //      //eventually all IO.Later instances will value busy set to false.
  //      eventual {
  //        result foreach {
  //          result =>
  //            result.error.busy.isBusy shouldBe false
  //        }
  //      }
  //    }
  //
  //  }
}
