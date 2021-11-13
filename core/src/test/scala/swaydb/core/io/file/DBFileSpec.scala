/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.io.file

import org.scalamock.scalatest.MockFactory
import swaydb.IO
import swaydb.IOValues._
import swaydb.core.CommonAssertions.randomThreadSafeIOStrategy
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.core.util.PipeOps._
import swaydb.core.{TestBase, TestCaseSweeper, TestForceSave}
import swaydb.data.slice.Slice
import swaydb.effect.Effect
import swaydb.testkit.RunThis._
import swaydb.utils.OperatingSystem

import java.nio.ReadOnlyBufferException
import java.nio.channels.{NonReadableChannelException, NonWritableChannelException}
import java.nio.file.FileAlreadyExistsException

class DBFileSpec extends TestBase with MockFactory {

  "write" should {
    "write bytes to a File" in {
      TestCaseSweeper {
        implicit sweeper =>
          val bytes1 = Slice(randomBytes(100))

          val bytes2 = Slice(randomBytes(100))

          val files = createDBFiles(bytes1, bytes2)
          files should have size 2

          createAllFilesReaders(files.head.path) foreach {
            reader =>
              reader.file.readAll shouldBe bytes1
          }

          createAllFilesReaders(files.last.path) foreach {
            reader =>
              reader.file.readAll shouldBe bytes2
          }
      }
    }

    "write empty bytes to a File" in {
      TestCaseSweeper {
        implicit sweeper =>
          //This test might log NullPointerException because cleaner is being
          //invoked a MappedByteBuffer which is empty. This is a valid test
          //but does not occur in reality. If a file (Segment or Map) are empty
          //then they are not created which would only occur if all key-values
          //from that file were removed.
          val files = createDBFiles(Slice.emptyBytes, Slice.emptyBytes)
          files.foreach(_.existsOnDisk shouldBe true)

          files foreach {
            file =>
              createAllFilesReaders(file.path) foreach {
                reader =>
                  reader.file.readAll shouldBe Slice.emptyBytes
                  reader.file.close()
              }
          }

          //ensure that file exists
          files.exists(file => Effect.notExists(file.path)) shouldBe false
      }
    }

    "write partially written bytes" in {
      TestCaseSweeper {
        implicit sweeper =>
          //size is 10 but only 2 bytes were written
          val incompleteBytes = Slice.of[Byte](10)
          incompleteBytes.addUnsignedInt(1)
          incompleteBytes.addUnsignedInt(2)
          incompleteBytes.size shouldBe 2

          val bytes = incompleteBytes.close()

          val files = createDBFiles(bytes, bytes)

          files should have size 2
          files foreach {
            file =>
              Slice(Effect.readAllBytes(file.path)) shouldBe bytes
              file.readAll shouldBe bytes
          }
      }
    }

    "fail to write if the file already exists" in {
      TestCaseSweeper {
        implicit sweeper =>
          val bytes = randomBytesSlice()

          val mmap = createMMAPWriteAndRead(randomFilePath, bytes)
          val standard = createStandardWriteAndRead(randomFilePath, bytes)

          val files = List(mmap, standard)

          files foreach {
            file =>
              //creating the same file again should fail
              IO(createMMAPWriteAndRead(file.path, randomBytesSlice())).left.value shouldBe a[FileAlreadyExistsException]
              IO(createStandardWriteAndRead(file.path, randomBytesSlice())).left.value shouldBe a[FileAlreadyExistsException]
          }

          //flush
          files.foreach(_.close())

          files foreach {
            file =>
              file.readAll shouldBe bytes
          }
      }
    }
  }

  "standardWrite" should {
    "initialise a StandardFile for writing and not reading and invoke the onOpen function on open" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val testFile = randomFilePath
          val bytes = randomBytesSlice()

          //      //opening a file should trigger the onOpen function
          //      implicit val fileSweeper = mock[FileSweeper]
          //
          //      fileSweeper.close _ expects * onCall {
          //        dbFile: FileSweeperItem =>
          //          dbFile.path shouldBe testFile
          //          dbFile.isOpen shouldBe true
          //          ()
          //      } repeat 3.times

          val file =
            DBFile.standardWrite(
              path = testFile,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              forceSave = TestForceSave.channel()
            )

          //above onOpen is also invoked
          file.isFileDefined shouldBe true //file is set
          file.isOpen shouldBe true
          file.append(bytes)

          IO(file.readAll).left.value shouldBe a[NonReadableChannelException]
          IO(file.read(position = 0, size = 1)).left.value shouldBe a[NonReadableChannelException]
          IO(file.getSkipCache(position = 0)).left.value shouldBe a[NonReadableChannelException]

          //closing the channel and reopening it will open it in read only mode
          file.close()
          file.isFileDefined shouldBe false
          file.isOpen shouldBe false
          file.readAll shouldBe bytes //read
          //above onOpen is also invoked
          file.isFileDefined shouldBe true
          file.isOpen shouldBe true
          //cannot write to a reopened file channel. Ones closed! It cannot be reopened for writing.
          IO(file.append(bytes)).left.value shouldBe a[NonWritableChannelException]

          file.close()

          DBFile.standardRead(
            path = testFile,
            fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          ) ==> {
            file =>
              file.readAll shouldBe bytes
              file.close()
          }
        //above onOpen is also invoked
      }
    }
  }

  "standardRead" should {
    "initialise a StandardFile for reading only" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val testFile = randomFilePath
          val bytes = randomBytesSlice()

          //      //opening a file should trigger the onOpen function
          //      implicit val fileSweeper = mock[FileSweeper]
          //      fileSweeper.close _ expects * onCall {
          //        dbFile: FileSweeperItem =>
          //          dbFile.path shouldBe testFile
          //          dbFile.isOpen shouldBe true
          //          ()
          //      } repeat 3.times

          Effect.write(testFile, bytes.toByteBufferWrap)

          val readFile = DBFile.standardRead(
            path = testFile,
            fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          )

          //reading a file should load the file lazily
          readFile.isFileDefined shouldBe false
          readFile.isOpen shouldBe false
          //reading the opens the file
          readFile.readAll shouldBe bytes
          //file is now opened
          readFile.isFileDefined shouldBe true
          readFile.isOpen shouldBe true

          //writing fails since the file is readonly
          IO(readFile.append(bytes)).left.value shouldBe a[NonWritableChannelException]
          //data remain unchanged
          DBFile.standardRead(
            path = testFile,
            fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          ).readAll shouldBe bytes

          readFile.close()
          readFile.isOpen shouldBe false
          readFile.isFileDefined shouldBe false
          //read bytes one by one
          (0 until bytes.size) foreach {
            index =>
              readFile.getSkipCache(position = index) shouldBe bytes(index)
          }
          readFile.isOpen shouldBe true
      }
    }

    "fail initialisation if the file does not exists" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          IO {
            DBFile.standardRead(
              path = randomFilePath,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true
            )
          }.left.value shouldBe a[swaydb.Exception.NoSuchFile]
      }
    }
  }

  "mmapWriteAndRead" should {
    "write bytes to a File, extend the buffer on overflow and reopen it for reading via mmapRead" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = randomFilePath
          val bytes = Slice("bytes one".getBytes())

          //      //opening a file should trigger the onOpen function
          //      implicit val fileSweeper = mock[FileSweeper]
          //      fileSweeper.close _ expects * onCall {
          //        dbFile: FileSweeperItem =>
          //          dbFile.path shouldBe testFile
          //          dbFile.isOpen shouldBe true
          //          ()
          //      } repeat 3.times

          val file =
            DBFile.mmapWriteAndRead(
              path = testFile,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows,
              forceSave = TestForceSave.mmap(),
              bytes = bytes
            )

          file.readAll shouldBe bytes
          file.isFull shouldBe true

          //overflow bytes
          val bytes2 = Slice("bytes two".getBytes())
          file.append(bytes2)
          file.isFull shouldBe true //complete fit - no extra bytes

          //overflow bytes
          val bytes3 = Slice("bytes three".getBytes())
          file.append(bytes3)
          file.isFull shouldBe true //complete fit - no extra bytes

          val expectedBytes = bytes ++ bytes2 ++ bytes3

          file.readAll shouldBe expectedBytes

          //close buffer
          file.close()
          file.isFileDefined shouldBe false
          file.isOpen shouldBe false
          file.readAll shouldBe expectedBytes
          file.isFileDefined shouldBe true
          file.isOpen shouldBe true

          //writing fails since the file is now readonly
          IO(file.append(bytes)).left.value shouldBe a[ReadOnlyBufferException]
          file.close()

          //open read only buffer
          DBFile.mmapRead(
            path = testFile,
            fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true,
            deleteAfterClean = OperatingSystem.isWindows
          ) ==> {
            file =>
              file.readAll shouldBe expectedBytes
              file.close()
          }
      }
    }

    "fail write if the slice is partially written" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = randomFilePath
          val bytes = Slice.of[Byte](10)
          bytes.addUnsignedInt(1)
          bytes.addUnsignedInt(2)

          bytes.size shouldBe 2

          IO {
            DBFile.mmapWriteAndRead(
              path = testFile,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows,
              forceSave = TestForceSave.mmap(),
              bytes = bytes
            )
          }.left.value shouldBe swaydb.Exception.FailedToWriteAllBytes(0, 2, bytes.size)
      }
    }

    "fail to write if the file already exists" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = randomFilePath
          val bytes = randomBytesSlice()

          DBFile.mmapWriteAndRead(
            path = testFile,
            fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true,
            deleteAfterClean = OperatingSystem.isWindows,
            forceSave = TestForceSave.mmap(),
            bytes = bytes
          ).close()

          IO {
            DBFile.mmapWriteAndRead(
              path = testFile,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows,
              forceSave = TestForceSave.mmap(),
              bytes = bytes
            )
          }.left.value shouldBe a[FileAlreadyExistsException] //creating the same file again should fail

          //file remains unchanged
          DBFile.mmapRead(
            path = testFile,
            fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true,
            deleteAfterClean = OperatingSystem.isWindows
          ) ==> {
            file =>
              file.readAll shouldBe bytes
              file.close()
          }
      }
    }
  }

  "mmapRead" should {
    "open an existing file for reading" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = randomFilePath
          val bytes = Slice("bytes one".getBytes())

          Effect.write(testFile, bytes.toByteBufferWrap)

          val readFile =
            DBFile.mmapRead(
              path = testFile,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows
            )

          def doRead = {
            readFile.isFileDefined shouldBe false //reading a file should load the file lazily
            readFile.isOpen shouldBe false
            readFile.readAll shouldBe bytes
            readFile.isFileDefined shouldBe true
            readFile.isOpen shouldBe true
          }

          doRead

          //close and read again
          readFile.close()
          doRead

          IO(Effect.write(testFile, bytes.toByteBufferWrap)).left.value shouldBe a[FileAlreadyExistsException] //creating the same file again should fail

          readFile.close()
      }
    }

    "fail to read if the file does not exists" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          IO {
            DBFile.mmapRead(
              path = randomFilePath,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows
            )
          }.left.value shouldBe a[swaydb.Exception.NoSuchFile]
      }
    }
  }

  "mmapInit" should {
    "open a file for writing" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._
            val testFile = randomFilePath
            val bytes1 = Slice("bytes one".getBytes())
            val bytes2 = Slice("bytes two".getBytes())
            val bytes3 = Slice("bytes three".getBytes())
            val bytes4 = Slice("bytes four".getBytes())

            val bufferSize = {
              //also randomly add partial or full byte size of byte4 to assert BufferOverflow is extended
              //even only partially written buffer.
              bytes1.size + bytes2.size + bytes3.size + (bytes4.size / (randomIntMax(3) max 1))
            }

            val file =
              DBFile.mmapInit(
                path = testFile,
                fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
                bufferSize = bufferSize,
                autoClose = true,
                deleteAfterClean = OperatingSystem.isWindows,
                forceSave = TestForceSave.mmap()
              )

            file.append(bytes1)
            file.isFull shouldBe false
            file.append(bytes2)
            file.isFull shouldBe false
            file.append(bytes3)
            //          file.isFull shouldBe true
            file.append(bytes4) //overflow write, buffer gets extended
            file.isFull shouldBe true

            file.readAll shouldBe (bytes1 ++ bytes2 ++ bytes3 ++ bytes4)
        }
      }
    }

    "fail to initialise if it already exists" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = randomFilePath
          Effect.write(to = testFile, bytes = Slice(randomBytes()).toByteBufferWrap)

          IO {
            DBFile.mmapInit(
              path = testFile,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              bufferSize = 10,
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows,
              forceSave = TestForceSave.mmap()
            )
          }.left.value shouldBe a[FileAlreadyExistsException]
      }
    }
  }

  "close" should {
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
    //      val file = DBFile.standardWrite(
    //        path = testFile,
    //        ioStrategy = randomIOStrategy(cacheOnAccess = true),
    //        blockCacheFileId = idGenerator.nextID,
    //        autoClose = true
    //      )
    //
    //      file.append(bytes)
    //      file.isFileDefined shouldBe true
    //
    //      def close = {
    //        file.close()
    //        file.isOpen shouldBe false
    //        file.isFileDefined shouldBe false
    //        file.existsOnDisk shouldBe true
    //      }
    //
    //      def open = {
    //        file.read(0, bytes.size) shouldBe bytes
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

    "close a memory mapped file and reopen on read" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = randomFilePath
          val bytes = randomBytesSlice()

          val file =
            DBFile.mmapInit(
              path = testFile,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              bufferSize = bytes.size,
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows,
              forceSave = TestForceSave.mmap()
            )

          file.append(bytes)

          def close = {
            file.close()
            file.isOpen shouldBe false
            file.isFileDefined shouldBe false
            file.existsOnDisk shouldBe true
          }

          def open = {
            file.read(position = 0, size = bytes.size) shouldBe bytes
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
    }

    "close a StandardFile and then reopening the file should open it in read only mode" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = randomFilePath
          val bytes = randomBytesSlice()

          val file =
            DBFile.standardWrite(
              path = testFile,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              forceSave = TestForceSave.channel()
            )

          file.append(bytes)
          file.close()

          IO(file.append(bytes)).left.value shouldBe a[NonWritableChannelException]
          file.readAll shouldBe bytes

          file.close()
      }
    }

    "close that MMAPFile and reopening the file should open it in read only mode" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = randomFilePath
          val bytes = randomBytesSlice()

          val file =
            DBFile.mmapInit(
              path = testFile,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              bufferSize = bytes.size,
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows,
              forceSave = TestForceSave.mmap()
            )

          file.append(bytes)
          file.close()

          IO(file.append(bytes)).left.value shouldBe a[ReadOnlyBufferException]
          file.readAll shouldBe bytes

          file.close()
      }
    }
  }

  "append" should {
    "append bytes to the end of the StandardFile" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = randomFilePath
          val bytes = List(randomBytesSlice(), randomBytesSlice(), randomBytesSlice())

          val file =
            DBFile.standardWrite(
              path = testFile,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              forceSave = TestForceSave.channel()
            )

          file.append(bytes(0))
          file.append(bytes(1))
          file.append(bytes(2))
          IO(file.read(position = 0, size = 1)).isLeft shouldBe true //not open for read

          file.close()

          val expectedAllBytes = bytes.foldLeft(List.empty[Byte])(_ ++ _).toSlice

          DBFile.standardRead(
            path = testFile,
            fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          ) ==> {
            file =>
              file.readAll shouldBe expectedAllBytes
              file.close()
          }
          DBFile.mmapRead(
            path = testFile,
            fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true,
            deleteAfterClean = OperatingSystem.isWindows
          ) ==> {
            file =>
              file.readAll shouldBe expectedAllBytes
              file.close()
          }

          file.close()
      }
    }

    "append bytes to the end of the MMAP file" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = randomFilePath
          val bytes = List(randomBytesSlice(), randomBytesSlice(), randomBytesSlice())

          val allBytesSize = bytes.foldLeft(0)(_ + _.size)
          val file =
            DBFile.mmapInit(
              path = testFile,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              bufferSize = allBytesSize,
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows,
              forceSave = TestForceSave.mmap()
            )

          file.append(bytes(0))
          file.append(bytes(1))
          file.append(bytes(2))
          file.get(position = 0) shouldBe bytes.head.head
          file.get(position = allBytesSize - 1) shouldBe bytes.last.last

          val expectedAllBytes = bytes.foldLeft(List.empty[Byte])(_ ++ _).toSlice

          file.readAll shouldBe expectedAllBytes
          file.close() //close

          //reopen
          DBFile.mmapRead(
            path = testFile,
            fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true,
            deleteAfterClean = OperatingSystem.isWindows
          ) ==> {
            file =>
              file.readAll shouldBe expectedAllBytes
              file.close()
          }

          DBFile.standardRead(
            path = testFile,
            fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          ) ==> {
            file =>
              file.readAll shouldBe expectedAllBytes
              file.close()
          }
      }
    }

    "append bytes by extending an overflown buffer of MMAP file" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = randomFilePath
          val bytes = List(randomBytesSlice(), randomBytesSlice(), randomBytesSlice(), randomBytesSlice(), randomBytesSlice())
          val allBytesSize = bytes.foldLeft(0)(_ + _.size)

          val file =
            DBFile.mmapInit(
              path = testFile,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              bufferSize = bytes.head.size,
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows,
              forceSave = TestForceSave.mmap()
            )

          file.append(bytes(0))
          file.append(bytes(1))
          file.append(bytes(2))
          file.append(bytes(3))
          file.append(bytes(4))
          file.get(position = 0) shouldBe bytes.head.head
          file.get(position = allBytesSize - 1) shouldBe bytes.last.last

          val expectedAllBytes = bytes.foldLeft(List.empty[Byte])(_ ++ _).toSlice

          file.readAll shouldBe expectedAllBytes
          file.close() //close

          //reopen
          DBFile.mmapRead(
            path = testFile,
            fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true,
            deleteAfterClean = OperatingSystem.isWindows
          ) ==> {
            file =>
              file.readAll shouldBe expectedAllBytes
              file.close()
          }

          DBFile.standardRead(
            path = testFile,
            fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          ) ==> {
            file =>
              file.readAll shouldBe expectedAllBytes
              file.close()
          }
      }
    }

    "not fail when appending empty bytes to StandardFile" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val file =
            DBFile.standardWrite(
              path = randomFilePath,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              forceSave = TestForceSave.channel()
            )

          file.append(Slice.emptyBytes)

          DBFile.standardRead(
            path = file.path,
            fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          ) ==> {
            file =>
              file.readAll shouldBe empty
              file.close()
          }
          file.close()
      }
    }

    "not fail when appending empty bytes to MMAPFile" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val file =
            DBFile.mmapInit(
              path = randomFilePath,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              bufferSize = 100,
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows,
              forceSave = TestForceSave.mmap()
            )

          file.append(Slice.emptyBytes)
          file.readAll shouldBe Slice.fill(file.fileSize)(0)
          file.close()

          DBFile.mmapRead(
            path = file.path,
            fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true,
            deleteAfterClean = OperatingSystem.isWindows
          ) ==> {
            file2 =>
              file2.readAll shouldBe Slice.fill(file.fileSize)(0)
              file2.close()
          }
      }
    }
  }

  "read and find" should {
    "read and find bytes at a position from a StandardFile" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = randomFilePath
          val bytes = randomBytesSlice(100)

          val file =
            DBFile.standardWrite(
              path = testFile,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              forceSave = TestForceSave.channel()
            )

          file.append(bytes)
          IO(file.read(position = 0, size = 1)).isLeft shouldBe true //not open for read

          file.close()

          val readFile = DBFile.standardRead(
            path = testFile,
            fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          )

          (0 until bytes.size) foreach {
            index =>
              readFile.read(position = index, size = 1) should contain only bytes(index)
          }

          readFile.read(position = 0, size = bytes.size / 2).toList should contain theSameElementsInOrderAs bytes.dropRight(bytes.size / 2).toList
          readFile.read(position = bytes.size / 2, size = bytes.size / 2).toList should contain theSameElementsInOrderAs bytes.drop(bytes.size / 2).toList
          //      readFile.get(1000) shouldBe 0

          readFile.close()
      }
    }
  }

  "delete" should {
    "delete a StandardFile" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val bytes = randomBytesSlice(100)

          val file =
            DBFile.standardWrite(
              path = randomFilePath,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              forceSave = TestForceSave.channel()
            )

          file.append(bytes)

          file.delete()
          file.existsOnDisk shouldBe false
          file.isOpen shouldBe false
          file.isFileDefined shouldBe false
      }
    }

    "delete a MMAPFile" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val file =
            DBFile.mmapWriteAndRead(
              path = randomFilePath,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows,
              forceSave = TestForceSave.mmap(),
              bytes = randomBytesSlice()
            ).sweep()

          file.close()

          file.delete()

          if (OperatingSystem.isWindows)
            sweeper.receiveAll()

          file.existsOnDisk shouldBe false
          file.isOpen shouldBe false
          file.isFileDefined shouldBe false
      }
    }
  }

  "copy" should {
    "copy a StandardFile" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val bytes = randomBytesSlice(100)

          val file =
            DBFile.standardWrite(
              path = randomFilePath,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              forceSave = TestForceSave.channel()
            )

          file.append(bytes)

          val targetFile = randomFilePath
          file.copyTo(targetFile) shouldBe targetFile

          DBFile.standardRead(
            path = targetFile,
            fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          ) ==> {
            file =>
              file.readAll shouldBe bytes
              file.close()
          }

          file.close()
      }
    }

    "copy a MMAPFile" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val bytes = randomBytesSlice(100)

          val file =
            DBFile.mmapInit(
              path = randomFilePath,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              bufferSize = bytes.size,
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows,
              forceSave = TestForceSave.mmap()
            )

          file.append(bytes)
          file.isFull shouldBe true
          file.close()

          val targetFile = randomFilePath
          file.copyTo(targetFile) shouldBe targetFile

          DBFile.standardRead(
            path = targetFile,
            fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          ) ==> {
            file =>
              file.readAll shouldBe bytes
              file.close()
          }
      }
    }
  }

  //  "copying large number of memory-mapped files" in {
  //    TestCaseSweeper {
  //      implicit sweeper =>
  //        import swaydb.testkit.RunThis._
  //
  //        runThis(1000.times, log = true) {
  //          import sweeper._
  //          val testFile = randomFilePath
  //          import swaydb.data.util.StorageUnits._
  //          val bytes = randomBytesSlice(1.kb)
  //          val file =
  //            DBFile.mmapWriteAndRead(
  //              path = testFile,
  //              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
  //              autoClose = true,
  //              deleteAfterClean = OperatingSystem.isWindows,
  //              blockCacheFileId = idGenerator.nextID,
  //              bytes = bytes
  //            )
  //
  //          file.readAll
  //
  //          Effect.copy(testFile, randomFilePath)
  //        }
  //    }
  //  }

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
  //          case io: IO.Left[_] =>
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
