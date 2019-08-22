///*
// * Copyright (c) 2019 Simer Plaha (@simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.core.io.file
//
//import java.nio.ReadOnlyBufferException
//import java.nio.channels.{NonReadableChannelException, NonWritableChannelException}
//import java.nio.file.{FileAlreadyExistsException, NoSuchFileException}
//
//import org.scalamock.scalatest.MockFactory
//import swaydb.Error.Segment.ErrorHandler
//import swaydb.IOValues._
//import swaydb.core.CommonAssertions.{randomBlockSize, randomIOStrategy}
//import swaydb.core.RunThis._
//import swaydb.core.TestData._
//import swaydb.core.queue.{FileSweeper, FileSweeperItem}
//import swaydb.core.util.PipeOps._
//import swaydb.core.{TestBase, TestLimitQueues}
//import swaydb.data.slice.Slice
//
//class DBFileSpec extends TestBase with MockFactory {
//
//  implicit val fileSweeper: FileSweeper.Enabled = TestLimitQueues.fileSweeper
//  implicit val memorySweeper = TestLimitQueues.memorySweeper
//  implicit def blockCache: Option[BlockCache.State] = TestLimitQueues.randomBlockCache
//
//  "DBFile.write" should {
//    "write bytes to a File" in {
//      val testFile = randomFilePath
//      val bytes = Slice(randomBytes())
//
//      DBFile.write(testFile, bytes).runRandomIO.value
//      DBFile.mmapRead(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = false
//      ).runRandomIO.value ==> {
//        file =>
//          file.readAll.runRandomIO.value shouldBe bytes
//          file.close.runRandomIO.value
//      }
//      DBFile.channelRead(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = false
//      ).runRandomIO.value ==> {
//        file =>
//          file.readAll.runRandomIO.value shouldBe bytes
//          file.close.runRandomIO.value
//      }
//    }
//
//    "write empty bytes to a File" in {
//      val testFile = randomFilePath
//      val bytes = Slice.emptyBytes
//
//      DBFile.write(testFile, bytes).runRandomIO.value
//      DBFile.mmapRead(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = false
//      ).runRandomIO.value ==> {
//        file =>
//          file.readAll.runRandomIO.value shouldBe empty
//          file.close.runRandomIO.value
//      }
//      IOEffect.exists(testFile) shouldBe true
//    }
//
//    "write only the bytes written" in {
//      val testFile = randomFilePath
//      val bytes = Slice.create[Byte](10)
//      bytes.addIntUnsigned(1)
//      bytes.addIntUnsigned(2)
//
//      bytes.size shouldBe 2
//
//      DBFile.write(testFile, bytes).runRandomIO.get shouldBe testFile
//      IOEffect.readAll(testFile).get shouldBe bytes
//    }
//
//    "fail to write if the file already exists" in {
//      val testFile = randomFilePath
//      val bytes = randomBytesSlice()
//
//      DBFile.write(testFile, bytes).runRandomIO.value
//      DBFile.write(testFile, bytes).failed.runRandomIO.value.exception shouldBe a[FileAlreadyExistsException] //creating the same file again should fail
//      //file remains unchanged
//      DBFile.channelRead(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = false
//      ).runRandomIO.value ==> {
//        file =>
//          file.readAll.runRandomIO.value shouldBe bytes
//          file.close.runRandomIO.value
//      }
//    }
//  }
//
//  "DBFile.channelWrite" should {
//    "initialise a FileChannel for writing and not reading and invoke the onOpen function on open" in {
//      val testFile = randomFilePath
//      val bytes = randomBytesSlice()
//
//      //opening a file should trigger the onOpen function
//      implicit val fileSweeper = mock[FileSweeper]
//
//      fileSweeper.close _ expects * onCall {
//        dbFile: FileSweeperItem =>
//          dbFile.path shouldBe testFile
//          dbFile.isOpen shouldBe true
//          ()
//      } repeat 3.times
//
//      val file =
//        DBFile.channelWrite(
//          path = testFile,
//          ioStrategy = randomIOStrategy(cacheOnAccess = true),
//          autoClose = true
//        ).runRandomIO.value
//
//      //above onOpen is also invoked
//      file.isFileDefined shouldBe true //file is set
//      file.isOpen shouldBe true
//      file.append(bytes).runRandomIO.value
//
//      file.readAll.failed.runRandomIO.value.exception shouldBe a[NonReadableChannelException]
//      file.read(0, 1).failed.runRandomIO.value.exception shouldBe a[NonReadableChannelException]
//      file.get(0).failed.runRandomIO.value.exception shouldBe a[NonReadableChannelException]
//
//      //closing the channel and reopening it will open it in read only mode
//      file.close.runRandomIO.value
//      file.isFileDefined shouldBe false
//      file.isOpen shouldBe false
//      file.readAll.runRandomIO.value shouldBe bytes //read
//      //above onOpen is also invoked
//      file.isFileDefined shouldBe true
//      file.isOpen shouldBe true
//      //cannot write to a reopened file channel. Ones closed! It cannot be reopened for writing.
//      file.append(bytes).failed.runRandomIO.value.exception shouldBe a[NonWritableChannelException]
//
//      file.close.runRandomIO.value
//
//      DBFile.channelRead(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = true
//      ).runRandomIO.value ==> {
//        file =>
//          file.readAll.runRandomIO.value shouldBe bytes
//          file.close.runRandomIO.value
//      }
//      //above onOpen is also invoked
//    }
//
//    "append if the slice is partially written" in {
//      val testFile = randomFilePath
//      val bytes = Slice.create[Byte](10)
//      bytes.addIntUnsigned(1)
//      bytes.addIntUnsigned(2)
//
//      bytes.size shouldBe 2
//
//      val channelFile =
//        DBFile.channelWrite(
//          path = testFile,
//          ioStrategy = randomIOStrategy(cacheOnAccess = true),
//          autoClose = true
//        ).runRandomIO.value
//
//      channelFile.append(bytes).runRandomIO.get
//      IOEffect.readAll(testFile).value shouldBe bytes
//      channelFile.close.runRandomIO.value
//    }
//
//    "fail initialisation if the file already exists" in {
//      val testFile = randomFilePath
//
//      DBFile.channelWrite(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = true
//      ).runRandomIO.value ==> {
//        file =>
//          file.existsOnDisk shouldBe true
//          file.close.runRandomIO.value
//      }
//      //creating the same file again should fail
//      DBFile.channelWrite(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = true
//      ).failed.runRandomIO.value.exception.toString shouldBe new FileAlreadyExistsException(testFile.toString).toString
//      //file remains unchanged
//      DBFile.channelRead(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = true
//      ).runRandomIO.value ==> {
//        file =>
//          file.readAll.runRandomIO.value shouldBe empty
//          file.close.runRandomIO.value
//      }
//    }
//  }
//
//  "DBFile.channelRead" should {
//    "initialise a FileChannel for reading only" in {
//      val testFile = randomFilePath
//      val bytes = randomBytesSlice()
//
//      //opening a file should trigger the onOpen function
//      implicit val fileSweeper = mock[FileSweeper]
//      fileSweeper.close _ expects * onCall {
//        dbFile: FileSweeperItem =>
//          dbFile.path shouldBe testFile
//          dbFile.isOpen shouldBe true
//          ()
//      } repeat 3.times
//
//      IOEffect.write(testFile, bytes).runRandomIO.value
//
//      val readFile = DBFile.channelRead(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = true
//      ).runRandomIO.value
//
//      //reading a file should load the file lazily
//      readFile.isFileDefined shouldBe false
//      readFile.isOpen shouldBe false
//      //reading the opens the file
//      readFile.readAll.runRandomIO.value shouldBe bytes
//      //file is now opened
//      readFile.isFileDefined shouldBe true
//      readFile.isOpen shouldBe true
//
//      //writing fails since the file is readonly
//      readFile.append(bytes).failed.runRandomIO.value.exception shouldBe a[NonWritableChannelException]
//      //data remain unchanged
//      DBFile.channelRead(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = true
//      ).runRandomIO.value.readAll.runRandomIO.value shouldBe bytes
//
//      readFile.close.runRandomIO.value
//      readFile.isOpen shouldBe false
//      readFile.isFileDefined shouldBe false
//      //read bytes one by one
//      (0 until bytes.size) foreach {
//        index =>
//          readFile.get(index).runRandomIO.value shouldBe bytes(index)
//      }
//      readFile.isOpen shouldBe true
//
//      readFile.close.runRandomIO.value
//    }
//
//    "fail initialisation if the file does not exists" in {
//      DBFile.channelRead(
//        path = randomFilePath,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = true
//      ).failed.runRandomIO.value.exception shouldBe a[NoSuchFileException]
//    }
//  }
//
//  "DBFile.mmapWriteAndRead" should {
//    "write bytes to a File, extend the buffer on overflow and reopen it for reading via mmapRead" in {
//      val testFile = randomFilePath
//      val bytes = Slice("bytes one".getBytes())
//
//      //opening a file should trigger the onOpen function
//      implicit val fileSweeper = mock[FileSweeper]
//      fileSweeper.close _ expects * onCall {
//        dbFile: FileSweeperItem =>
//          dbFile.path shouldBe testFile
//          dbFile.isOpen shouldBe true
//          ()
//      } repeat 3.times
//
//      val file =
//        DBFile.mmapWriteAndRead(
//          path = testFile,
//          ioStrategy = randomIOStrategy(cacheOnAccess = true),
//          autoClose = true,
//          bytes = bytes
//        ).runRandomIO.value
//
//      file.readAll.runRandomIO.value shouldBe bytes
//      file.isFull.runRandomIO.value shouldBe true
//
//      //overflow bytes
//      val bytes2 = Slice("bytes two".getBytes())
//      file.append(bytes2).runRandomIO.value
//      file.isFull.runRandomIO.value shouldBe true //complete fit - no extra bytes
//
//      //overflow bytes
//      val bytes3 = Slice("bytes three".getBytes())
//      file.append(bytes3).runRandomIO.value
//      file.isFull.runRandomIO.value shouldBe true //complete fit - no extra bytes
//
//      val expectedBytes = bytes ++ bytes2 ++ bytes3
//
//      file.readAll.runRandomIO.value shouldBe expectedBytes
//
//      //close buffer
//      file.close.runRandomIO.value
//      file.isFileDefined shouldBe false
//      file.isOpen shouldBe false
//      file.readAll.runRandomIO.value shouldBe expectedBytes
//      file.isFileDefined shouldBe true
//      file.isOpen shouldBe true
//
//      //writing fails since the file is now readonly
//      file.append(bytes).failed.runRandomIO.value.exception shouldBe a[ReadOnlyBufferException]
//      file.close.runRandomIO.value
//
//      //open read only buffer
//      DBFile.mmapRead(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = true
//      ).runRandomIO.value ==> {
//        file =>
//          file.readAll.runRandomIO.value shouldBe expectedBytes
//          file.close.runRandomIO.value
//      }
//    }
//
//    "fail write if the slice is partially written" in {
//      val testFile = randomFilePath
//      val bytes = Slice.create[Byte](10)
//      bytes.addIntUnsigned(1)
//      bytes.addIntUnsigned(2)
//
//      bytes.size shouldBe 2
//
//      DBFile.mmapWriteAndRead(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = true,
//        bytes = bytes
//      ).failed.runRandomIO.value.exception shouldBe swaydb.Exception.FailedToWriteAllBytes(0, 2, bytes.size)
//    }
//
//    "fail to write if the file already exists" in {
//      val testFile = randomFilePath
//      val bytes = randomBytesSlice()
//
//      DBFile.mmapWriteAndRead(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = true,
//        bytes = bytes
//      ).runRandomIO.value.close.runRandomIO.value
//
//      DBFile.mmapWriteAndRead(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = true,
//        bytes = bytes
//      ).failed.runRandomIO.value.exception shouldBe a[FileAlreadyExistsException] //creating the same file again should fail
//
//      //file remains unchanged
//      DBFile.mmapRead(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = true
//      ).runRandomIO.value ==> {
//        file =>
//          file.readAll.runRandomIO.value shouldBe bytes
//          file.close.runRandomIO.value
//      }
//    }
//  }
//
//  "DBFile.mmapRead" should {
//    "open an existing file for reading" in {
//      val testFile = randomFilePath
//      val bytes = Slice("bytes one".getBytes())
//
//      DBFile.write(testFile, bytes).runRandomIO.value
//
//      val readFile =
//        DBFile.mmapRead(
//          path = testFile,
//          ioStrategy = randomIOStrategy(cacheOnAccess = true),
//          autoClose = true
//        ).runRandomIO.value
//
//      def doRead = {
//        readFile.isFileDefined shouldBe false //reading a file should load the file lazily
//        readFile.isOpen shouldBe false
//        readFile.readAll.runRandomIO.value shouldBe bytes
//        readFile.isFileDefined shouldBe true
//        readFile.isOpen shouldBe true
//      }
//
//      doRead
//
//      //close and read again
//      readFile.close.runRandomIO.value
//      doRead
//
//      DBFile.write(testFile, bytes).failed.runRandomIO.value.exception shouldBe a[FileAlreadyExistsException] //creating the same file again should fail
//
//      readFile.close.runRandomIO.value
//    }
//
//    "fail to read if the file does not exists" in {
//      DBFile.mmapRead(
//        path = randomFilePath,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = true
//      ).failed.runRandomIO.value.exception shouldBe a[NoSuchFileException]
//    }
//  }
//
//  "DBFile.mmapInit" should {
//    "open a file for writing" in {
//      val testFile = randomFilePath
//      val bytes1 = Slice("bytes one".getBytes())
//      val bytes2 = Slice("bytes two".getBytes())
//      val bytes3 = Slice("bytes three".getBytes())
//      val bytes4 = Slice("bytes four".getBytes())
//
//      val file =
//        DBFile.mmapInit(
//          path = testFile,
//          ioStrategy = randomIOStrategy(cacheOnAccess = true),
//          bufferSize = bytes1.size + bytes2.size + bytes3.size,
//          autoClose = true
//        ).runRandomIO.value
//
//      file.append(bytes1).runRandomIO.value
//      file.isFull.runRandomIO.value shouldBe false
//      file.append(bytes2).runRandomIO.value
//      file.isFull.runRandomIO.value shouldBe false
//      file.append(bytes3).runRandomIO.value
//      file.isFull.runRandomIO.value shouldBe true
//      file.append(bytes4).runRandomIO.value //overflow write, buffer gets extended
//      file.isFull.runRandomIO.value shouldBe true
//
//      file.readAll.runRandomIO.value shouldBe (bytes1 ++ bytes2 ++ bytes3 ++ bytes4)
//
//      file.close.runRandomIO.value
//    }
//
//    "fail to initialise if it already exists" in {
//      val testFile = randomFilePath
//      DBFile.write(path = testFile, bytes = Slice(randomBytes())).runRandomIO.value
//
//      DBFile.mmapInit(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        bufferSize = 10,
//        autoClose = true
//      ).failed.runRandomIO.value.exception shouldBe a[FileAlreadyExistsException]
//    }
//  }
//
//  "DBFile.close" should {
//    "close a file channel and reopen on read" in {
//      val testFile = randomFilePath
//      val bytes = randomBytesSlice()
//
//      //opening a file should trigger the onOpen function
//      implicit val fileSweeper = mock[FileSweeper]
//      fileSweeper.close _ expects * onCall {
//        (dbFile: FileSweeperItem) =>
//          dbFile.path shouldBe testFile
//          dbFile.isOpen shouldBe true
//          ()
//      } repeat 5.times
//
//      val file = DBFile.channelWrite(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = true
//      ).runRandomIO.value
//
//      file.append(bytes).runRandomIO.value
//      file.isFileDefined shouldBe true
//
//      def close = {
//        file.close.runRandomIO.value
//        file.isOpen shouldBe false
//        file.isFileDefined shouldBe false
//        file.existsOnDisk shouldBe true
//      }
//
//      def open = {
//        file.read(0, bytes.size).runRandomIO.value shouldBe bytes
//        file.isOpen shouldBe true
//        file.isFileDefined shouldBe true
//      }
//
//      close
//      //closing an already closed channel should not fail
//      close
//      open
//
//      close
//      open
//
//      close
//      open
//
//      close
//    }
//
//    "close a memory mapped file and reopen on read" in {
//      val testFile = randomFilePath
//      val bytes = randomBytesSlice()
//
//      val file =
//        DBFile.mmapInit(
//          path = testFile,
//          ioStrategy = randomIOStrategy(cacheOnAccess = true),
//          bufferSize = bytes.size,
//          autoClose = true
//        ).runRandomIO.value
//
//      file.append(bytes).runRandomIO.value
//
//      def close = {
//        file.close.runRandomIO.value
//        file.isOpen shouldBe false
//        file.isFileDefined shouldBe false
//        file.existsOnDisk shouldBe true
//      }
//
//      def open = {
//        file.read(0, bytes.size).runRandomIO.value shouldBe bytes
//        file.isOpen shouldBe true
//        file.isFileDefined shouldBe true
//      }
//
//      //closing multiple times should not fail
//      close
//      close
//      close
//
//      open
//
//      close
//      open
//
//      close
//      open
//
//      close
//    }
//
//    "close a FileChannel and then reopening the file should open it in read only mode" in {
//      val testFile = randomFilePath
//      val bytes = randomBytesSlice()
//
//      val file =
//        DBFile.channelWrite(
//          path = testFile,
//          ioStrategy = randomIOStrategy(cacheOnAccess = true),
//          autoClose = true
//        ).runRandomIO.value
//
//      file.append(bytes).runRandomIO.value
//      file.close.runRandomIO.value
//
//      file.append(bytes).failed.runRandomIO.value.exception shouldBe a[NonWritableChannelException]
//      file.readAll.runRandomIO.value shouldBe bytes
//
//      file.close.runRandomIO.value
//    }
//
//    "close that MMAPFile and reopening the file should open it in read only mode" in {
//      val testFile = randomFilePath
//      val bytes = randomBytesSlice()
//
//      val file =
//        DBFile.mmapInit(
//          path = testFile,
//          ioStrategy = randomIOStrategy(cacheOnAccess = true),
//          bufferSize = bytes.size,
//          autoClose = true
//        ).runRandomIO.value
//
//      file.append(bytes).runRandomIO.value
//      file.close.runRandomIO.value
//
//      file.append(bytes).failed.runRandomIO.value.exception shouldBe a[ReadOnlyBufferException]
//      file.readAll.runRandomIO.value shouldBe bytes
//
//      file.close.runRandomIO.value
//    }
//  }
//
//  "DBFile.append" should {
//    "append bytes to the end of the ChannelFile" in {
//      val testFile = randomFilePath
//      val bytes = List(randomBytesSlice(), randomBytesSlice(), randomBytesSlice())
//
//      val file =
//        DBFile.channelWrite(
//          path = testFile,
//          ioStrategy = randomIOStrategy(cacheOnAccess = true),
//          autoClose = true
//        ).runRandomIO.value
//
//      file.append(bytes(0)).runRandomIO.value
//      file.append(bytes(1)).runRandomIO.value
//      file.append(bytes(2)).runRandomIO.value
//      file.read(0, 1).isFailure shouldBe true //not open for read
//
//      file.close.runRandomIO.value
//
//      val expectedAllBytes = bytes.foldLeft(List.empty[Byte])(_ ++ _).toSlice
//
//      DBFile.channelRead(
//        path = testFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = true
//      ).runRandomIO.value ==> {
//        file =>
//          file.readAll.runRandomIO.value shouldBe expectedAllBytes
//          file.close.runRandomIO.value
//      }
//      DBFile.mmapRead(testFile, randomIOStrategy(cacheOnAccess = true), autoClose = true).runRandomIO.value ==> {
//        file =>
//          file.readAll.runRandomIO.value shouldBe expectedAllBytes
//          file.close.runRandomIO.value
//      }
//
//      file.close.runRandomIO.value
//    }
//
//    "append bytes to the end of the MMAP file" in {
//      val testFile = randomFilePath
//      val bytes = List(randomBytesSlice(), randomBytesSlice(), randomBytesSlice())
//
//      val allBytesSize = bytes.foldLeft(0)(_ + _.size)
//      val file =
//        DBFile.mmapInit(
//          path = testFile,
//          ioStrategy = randomIOStrategy(cacheOnAccess = true),
//          bufferSize = allBytesSize,
//          autoClose = true
//        ).runRandomIO.value
//
//      file.append(bytes(0)).runRandomIO.value
//      file.append(bytes(1)).runRandomIO.value
//      file.append(bytes(2)).runRandomIO.value
//      file.get(0).runRandomIO.value shouldBe bytes.head.head
//      file.get(allBytesSize - 1).runRandomIO.value shouldBe bytes.last.last
//
//      val expectedAllBytes = bytes.foldLeft(List.empty[Byte])(_ ++ _).toSlice
//
//      file.readAll.runRandomIO.value shouldBe expectedAllBytes
//      file.close.runRandomIO.value //close
//
//      //reopen
//      DBFile.mmapRead(path = testFile, ioStrategy = randomIOStrategy(cacheOnAccess = true), autoClose = true).runRandomIO.value ==> {
//        file =>
//          file.readAll.runRandomIO.value shouldBe expectedAllBytes
//          file.close.runRandomIO.value
//      }
//      DBFile.channelRead(path = testFile, ioStrategy = randomIOStrategy(cacheOnAccess = true), autoClose = true).runRandomIO.value ==> {
//        file =>
//          file.readAll.runRandomIO.value shouldBe expectedAllBytes
//          file.close.runRandomIO.value
//      }
//    }
//
//    "append bytes by extending an overflown buffer of MMAP file" in {
//      val testFile = randomFilePath
//      val bytes = List(randomBytesSlice(), randomBytesSlice(), randomBytesSlice(), randomBytesSlice(), randomBytesSlice())
//      val allBytesSize = bytes.foldLeft(0)(_ + _.size)
//
//      val file = DBFile.mmapInit(testFile, randomIOStrategy(cacheOnAccess = true), bytes.head.size, autoClose = true).runRandomIO.value
//      file.append(bytes(0)).runRandomIO.value
//      file.append(bytes(1)).runRandomIO.value
//      file.append(bytes(2)).runRandomIO.value
//      file.append(bytes(3)).runRandomIO.value
//      file.append(bytes(4)).runRandomIO.value
//      file.get(0).runRandomIO.value shouldBe bytes.head.head
//      file.get(allBytesSize - 1).runRandomIO.value shouldBe bytes.last.last
//
//      val expectedAllBytes = bytes.foldLeft(List.empty[Byte])(_ ++ _).toSlice
//
//      file.readAll.runRandomIO.value shouldBe expectedAllBytes
//      file.close.runRandomIO.value //close
//
//      //reopen
//      DBFile.mmapRead(path = testFile, ioStrategy = randomIOStrategy(cacheOnAccess = true), autoClose = true).runRandomIO.value ==> {
//        file =>
//          file.readAll.runRandomIO.value shouldBe expectedAllBytes
//          file.close.runRandomIO.value
//      }
//      DBFile.channelRead(path = testFile, ioStrategy = randomIOStrategy(cacheOnAccess = true), autoClose = true).runRandomIO.value ==> {
//        file =>
//          file.readAll.runRandomIO.value shouldBe expectedAllBytes
//          file.close.runRandomIO.value
//      }
//    }
//
//    "not fail when appending empty bytes to ChannelFile" in {
//      val file = DBFile.channelWrite(path = randomFilePath, ioStrategy = randomIOStrategy(cacheOnAccess = true), autoClose = true).runRandomIO.value
//      file.append(Slice.emptyBytes).runRandomIO.value
//      DBFile.channelRead(path = file.path, ioStrategy = randomIOStrategy(cacheOnAccess = true), autoClose = true).runRandomIO.value ==> {
//        file =>
//          file.readAll.runRandomIO.value shouldBe empty
//          file.close.runRandomIO.value
//      }
//      file.close.runRandomIO.value
//    }
//
//    "not fail when appending empty bytes to MMAPFile" in {
//      val file = DBFile.mmapInit(path = randomFilePath, ioStrategy = randomIOStrategy(cacheOnAccess = true), bufferSize = 100, autoClose = true).runRandomIO.value
//      file.append(Slice.emptyBytes).runRandomIO.value
//      file.readAll.runRandomIO.value shouldBe Slice.fill(file.fileSize.get.toInt)(0)
//      file.close.runRandomIO.value
//
//      DBFile.mmapRead(file.path, randomIOStrategy(cacheOnAccess = true), autoClose = true).runRandomIO.value ==> {
//        file2 =>
//          file2.readAll.runRandomIO.value shouldBe Slice.fill(file.fileSize.get.toInt)(0)
//          file2.close.runRandomIO.value
//      }
//    }
//  }
//
//  "DBFile.read and find" should {
//    "read and find bytes at a position from a ChannelFile" in {
//      val testFile = randomFilePath
//      val bytes = randomBytesSlice(100)
//
//      val file = DBFile.channelWrite(path = testFile, ioStrategy = randomIOStrategy(cacheOnAccess = true), autoClose = true).runRandomIO.value
//      file.append(bytes).runRandomIO.value
//      file.read(0, 1).isFailure shouldBe true //not open for read
//
//      file.close.runRandomIO.value
//
//      val readFile = DBFile.channelRead(path = testFile, ioStrategy = randomIOStrategy(cacheOnAccess = true), autoClose = true).runRandomIO.value
//
//      (0 until bytes.size) foreach {
//        index =>
//          readFile.read(index, 1).runRandomIO.value should contain only bytes(index)
//      }
//
//      readFile.read(0, bytes.size / 2).runRandomIO.value.toList should contain theSameElementsInOrderAs bytes.dropRight(bytes.size / 2).toList
//      readFile.read(bytes.size / 2, bytes.size / 2).runRandomIO.value.toList should contain theSameElementsInOrderAs bytes.drop(bytes.size / 2).toList
//      readFile.get(1000).runRandomIO.value shouldBe 0
//
//      readFile.close.runRandomIO.value
//    }
//  }
//
//  "DBFile.delete" should {
//    "delete a ChannelFile" in {
//      val bytes = randomBytesSlice(100)
//
//      val file = DBFile.channelWrite(
//        path = randomFilePath,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = true
//      ).runRandomIO.value
//
//      file.append(bytes).runRandomIO.value
//
//      file.delete().runRandomIO.value
//      file.existsOnDisk shouldBe false
//      file.isOpen shouldBe false
//      file.isFileDefined shouldBe false
//    }
//
//    "delete a MMAPFile" in {
//      val file =
//        DBFile.mmapWriteAndRead(
//          path = randomFilePath,
//          ioStrategy = randomIOStrategy(cacheOnAccess = true),
//          autoClose = true,
//          bytes = randomBytesSlice()
//        ).runRandomIO.value
//
//      file.close.runRandomIO.value
//
//      file.delete().runRandomIO.value
//      file.existsOnDisk shouldBe false
//      file.isOpen shouldBe false
//      file.isFileDefined shouldBe false
//    }
//  }
//
//  "DBFile.copy" should {
//    "copy a ChannelFile" in {
//      val bytes = randomBytesSlice(100)
//
//      val file =
//        DBFile.channelWrite(
//          path = randomFilePath,
//          ioStrategy = randomIOStrategy(cacheOnAccess = true),
//          autoClose = true
//        ).runRandomIO.value
//
//      file.append(bytes).runRandomIO.value
//
//      val targetFile = randomFilePath
//      file.copyTo(targetFile).runRandomIO.value shouldBe targetFile
//
//      DBFile.channelRead(targetFile, randomIOStrategy(cacheOnAccess = true), autoClose = true).runRandomIO.value ==> {
//        file =>
//          file.readAll.runRandomIO.value shouldBe bytes
//          file.close.runRandomIO.value
//      }
//
//      file.close.runRandomIO.value
//    }
//
//    "copy a MMAPFile" in {
//      val bytes = randomBytesSlice(100)
//
//      val file =
//        DBFile.mmapInit(
//          path = randomFilePath,
//          ioStrategy = randomIOStrategy(cacheOnAccess = true),
//          bufferSize = bytes.size,
//          autoClose = true
//        ).runRandomIO.value
//
//      file.append(bytes).runRandomIO.value
//      file.isFull.runRandomIO.value shouldBe true
//      file.close.runRandomIO.value
//
//      val targetFile = randomFilePath
//      file.copyTo(targetFile).runRandomIO.value shouldBe targetFile
//
//      DBFile.channelRead(
//        path = targetFile,
//        ioStrategy = randomIOStrategy(cacheOnAccess = true),
//        autoClose = true
//      ).runRandomIO.value ==> {
//        file =>
//          file.readAll.runRandomIO.value shouldBe bytes
//          file.close.runRandomIO.value
//      }
//    }
//  }
//
//  //  "Concurrently opening files" should {
//  //    "result in Busy exception" in {
//  //      //create a file
//  //      val bytes = Slice(randomBytes())
//  //      val file = DBFile.mmapInit(randomFilePath, bytes.size, autoClose = true).runIO
//  //      file.append(bytes).runIO
//  //
//  //      //concurrently close and read the same file.
//  //      val ios =
//  //        (1 to 500).par map {
//  //          _ =>
//  //            if (randomBoolean) Future(file.close)
//  //            file.readAll
//  //        }
//  //
//  //      //convert all failures to Async
//  //      val result: List[IO.Later[_]] =
//  //        ios.toList collect {
//  //          case io: IO.Failure[_] =>
//  //            io.recoverToAsync(IO.Async((), swaydb.Error.None)).asInstanceOf[IO.Later[_]]
//  //        }
//  //
//  //      result.size should be >= 1
//  //
//  //      //eventually all IO.Later instances will value busy set to false.
//  //      eventual {
//  //        result foreach {
//  //          result =>
//  //            result.error.busy.isBusy shouldBe false
//  //        }
//  //      }
//  //    }
//  //
//  //  }
//}
