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

package swaydb.core.file

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.{CoreTestSweeper, RandomForceSave}
import swaydb.core.file.CoreFileTestKit._
import swaydb.core.CoreTestSweeper._
import swaydb.effect.Effect
import swaydb.effect.EffectTestKit._
import swaydb.slice.{Slice, Slices}
import swaydb.slice.SliceTestKit._
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._
import swaydb.utils.OperatingSystem
import swaydb.utils.PipeOps._

import java.nio.channels.{NonReadableChannelException, NonWritableChannelException}
import java.nio.file.FileAlreadyExistsException
import java.nio.ReadOnlyBufferException

class CoreFileSpec extends AnyWordSpec {

  "standardWritable" should {
    "initialise a StandardFile for write only" in {
      CoreTestSweeper.repeat(3.times) {
        implicit sweeper =>
          import sweeper._

          val testFile = genFilePath()
          val bytes = randomBytesSlice()

          val file =
            CoreFile.standardWritable(
              path = testFile,
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              forceSave = RandomForceSave.standard()
            )

          //the file is open
          file.isFileDefined shouldBe true //file is set
          file.isOpen shouldBe true
          file.append(bytes)

          //cannot read the file
          anyOrder(
            assertThrows[NonReadableChannelException](file.readAll()),
            assertThrows[NonReadableChannelException](file.read(position = 0, size = 1)),
            assertThrows[NonReadableChannelException](file.get(position = 0))
          )

          //closing the channel and reopening it will open it in read only mode
          file.close()
          file.isFileDefined shouldBe false
          file.isOpen shouldBe false
          file.readAll() shouldBe bytes //read

          //above onOpen is also invoked
          file.isFileDefined shouldBe true
          file.isOpen shouldBe true

          //cannot write to a reopened file channel. Ones closed! It cannot be reopened for writing.
          anyOrder(
            assertThrows[NonWritableChannelException](file.append(bytes)),
            assertThrows[FileAlreadyExistsException] {
              CoreFile.standardWritable(
                path = testFile,
                fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
                autoClose = true,
                forceSave = RandomForceSave.standard()
              )
            }
          )

          file.close()

          CoreFile.standardReadable(
            path = testFile,
            fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          ) ==> {
            file =>
              file.readAll() shouldBe bytes
              file.close()
          }
        //above onOpen is also invoked
      }
    }

    "fail if file already exists" in {
      CoreTestSweeper.repeat(3.times) {
        implicit sweeper =>
          import sweeper._

          val testFile = genFilePath()

          def genFile() =
            CoreFile.standardWritable(
              path = testFile,
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              forceSave = RandomForceSave.standard()
            )

          val file = genFile()
          file.isOpen shouldBe true

          assertThrows[NonReadableChannelException](file.readAll()) //cannot read
          assertThrows[FileAlreadyExistsException](genFile()) //create new file fails
      }
    }
  }

  "standardReadable" should {
    "initialise a StandardFile for read only" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._

          val testFile = genFilePath()
          val bytes = randomBytesSlice()

          Effect.write(testFile, bytes.toByteBufferWrap())

          val readFile =
            CoreFile.standardReadable(
              path = testFile,
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true
            )

          //reading a file should load the file lazily
          readFile.isFileDefined shouldBe false
          readFile.isOpen shouldBe false
          //reading the opens the file
          readFile.readAll() shouldBe bytes
          //file is now opened
          readFile.isFileDefined shouldBe true
          readFile.isOpen shouldBe true

          //writing fails since the file is readonly
          assertThrows[NonWritableChannelException](readFile.append(bytes))
          //data remain unchanged
          CoreFile.standardReadable(
            path = testFile,
            fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          ).readAll() shouldBe bytes

          readFile.close()
          readFile.isOpen shouldBe false
          readFile.isFileDefined shouldBe false
          //read bytes one by one
          (0 until bytes.size) foreach {
            index =>
              readFile.get(position = index) shouldBe bytes(index)
          }
          readFile.isOpen shouldBe true
      }
    }

    "fail initialisation if the file does not exists" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._
          assertThrows[swaydb.Exception.NoSuchFile] {
            CoreFile.standardReadable(
              path = genFilePath(),
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true
            )
          }
      }
    }
  }

  "mmapWriteableReadable" should {
    "write bytes to a File, extend the buffer on overflow and reopen it for reading via mmapRead" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = genFilePath()
          val bytes = Slice.writeString("bytes one")

          val file =
            CoreFile.mmapWriteableReadable(
              path = testFile,
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows(),
              forceSave = RandomForceSave.mmap(),
              bytes = bytes
            )

          file.readAll() shouldBe bytes
          file.isFull() shouldBe true

          //overflow bytes
          val bytes2 = Slice.writeString("bytes two")
          file.append(bytes2)
          file.isFull() shouldBe true //complete fit - no extra bytes

          //overflow bytes
          val bytes3 = Slice.writeString("bytes three")
          file.append(bytes3)
          file.isFull() shouldBe true //complete fit - no extra bytes

          val expectedBytes = bytes ++ bytes2 ++ bytes3

          file.readAll() shouldBe expectedBytes

          //close buffer
          file.close()
          file.isFileDefined shouldBe false
          file.isOpen shouldBe false
          file.readAll() shouldBe expectedBytes
          file.isFileDefined shouldBe true
          file.isOpen shouldBe true

          //writing fails since the file is now readonly
          assertThrows[ReadOnlyBufferException](file.append(bytes))
          file.close()

          //open read only buffer
          CoreFile.mmapReadable(
            path = testFile,
            fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true,
            deleteAfterClean = OperatingSystem.isWindows()
          ) ==> {
            file =>
              file.readAll() shouldBe expectedBytes
              file.close()
          }
      }
    }

    "fail write if the slice is partially written" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = genFilePath()

          val bytes = Slice.allocate[Byte](10)
          bytes.addUnsignedInt(1)
          bytes.addUnsignedInt(2)
          bytes.size shouldBe 2
          bytes.isOriginalFullSlice shouldBe false

          intercept[swaydb.Exception.FailedToWriteAllBytes] {
            CoreFile.mmapWriteableReadable(
              path = testFile,
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows(),
              forceSave = RandomForceSave.mmap(),
              bytes = bytes
            )
          } shouldBe swaydb.Exception.FailedToWriteAllBytes(0, 10, 2)
      }
    }

    "fail to write if the file already exists" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = genFilePath()
          val bytes = randomBytesSlice()

          CoreFile.mmapWriteableReadable(
            path = testFile,
            fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true,
            deleteAfterClean = OperatingSystem.isWindows(),
            forceSave = RandomForceSave.mmap(),
            bytes = bytes
          ).close()

          //creating the same file again should fail
          assertThrows[FileAlreadyExistsException] {
            CoreFile.mmapWriteableReadable(
              path = testFile,
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows(),
              forceSave = RandomForceSave.mmap(),
              bytes = bytes
            )
          }

          //file remains unchanged
          CoreFile.mmapReadable(
            path = testFile,
            fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true,
            deleteAfterClean = OperatingSystem.isWindows()
          ) ==> {
            file =>
              file.readAll() shouldBe bytes
              file.close()
          }
      }
    }
  }

  "mmapReadable" should {
    "open an existing file for reading" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = genFilePath()
          val bytes = Slice.writeString(randomString())

          Effect.write(testFile, bytes.toByteBufferWrap())

          val readFile =
            CoreFile.mmapReadable(
              path = testFile,
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows()
            )

          def doRead() = {
            readFile.isFileDefined shouldBe false //reading a file should load the file lazily
            readFile.isOpen shouldBe false
            readFile.readAll() shouldBe bytes
            readFile.isFileDefined shouldBe true
            readFile.isOpen shouldBe true
          }

          doRead()

          //close and read again
          readFile.close()
          doRead()

          assertThrows[FileAlreadyExistsException](Effect.write(testFile, bytes.toByteBufferWrap())) //creating the same file again should fail

          readFile.close()
      }
    }

    "fail to read if the file does not exists" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._
          assertThrows[swaydb.Exception.NoSuchFile] {
            CoreFile.mmapReadable(
              path = genFilePath(),
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows()
            )
          }
      }
    }
  }

  "mmapEmptyWriteableReadable" should {
    "fail to initialise if it already exists" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = genFilePath()
          Effect.write(to = testFile, bytes = Slice.wrap(randomBytes()).toByteBufferWrap())

          assertThrows[FileAlreadyExistsException] {
            CoreFile.mmapEmptyWriteableReadable(
              path = testFile,
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              bufferSize = 10,
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows(),
              forceSave = RandomForceSave.mmap()
            )
          }
      }
    }
  }

  "closing" should {
    "close a StandardFile and then reopening the file should open it in read only mode" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = genFilePath()
          val bytes = randomBytesSlice()

          val file =
            CoreFile.standardWritable(
              path = testFile,
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              forceSave = RandomForceSave.standard()
            )

          file.append(bytes)
          file.close()

          assertThrows[NonWritableChannelException](file.append(bytes))
          file.readAll() shouldBe bytes

          file.close()
      }
    }

    "close a memory mapped file and reopen on read" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = genFilePath()
          val bytes = randomBytesSlice()

          val file =
            CoreFile.mmapEmptyWriteableReadable(
              path = testFile,
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              bufferSize = bytes.size,
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows(),
              forceSave = RandomForceSave.mmap()
            )

          file.append(bytes)

          def close() = {
            file.close()
            file.isOpen shouldBe false
            file.isFileDefined shouldBe false
            file.existsOnDisk() shouldBe true
          }

          def open() = {
            file.read(position = 0, size = bytes.size) shouldBe bytes
            file.isOpen shouldBe true
            file.isFileDefined shouldBe true
          }

          //closing multiple times should not fail
          close()
          close()
          close()

          open()

          close()
          open()

          close()
          open()

          close()
      }
    }

    "close a MMAPFile and reopening the file should open it in read only mode" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = genFilePath()
          val bytes = randomBytesSlice()

          val file =
            CoreFile.mmapEmptyWriteableReadable(
              path = testFile,
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              bufferSize = bytes.size,
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows(),
              forceSave = RandomForceSave.mmap()
            )

          file.append(bytes)
          file.close()

          assertThrows[ReadOnlyBufferException](file.append(bytes))
          file.readAll shouldBe bytes

          file.close()
      }
    }
  }

  "handle byte overflow" when {
    "open a file for writing and handle BufferOverflow" in {
      runThisNumbered(10.times, log = true) {
        testNumber =>
          CoreTestSweeper {
            implicit sweeper =>
              import sweeper._
              val bytes1 = Slice.writeString("bytes one")
              val bytes2 = Slice.writeString("bytes two")
              val bytes3 = Slice.writeString("bytes three")
              val bytes4 = Slice.writeString("bytes four")

              val bufferSize = {
                //also randomly add partial or full byte size of byte4 to assert BufferOverflow is extended
                //even only partially written buffer.
                bytes1.size + bytes2.size + bytes3.size + (bytes4.size / (randomIntMax(3) max 1))
              }

              val mmapFile =
                CoreFile.mmapEmptyWriteableReadable(
                  path = genFilePath(),
                  fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
                  bufferSize = bufferSize,
                  autoClose = true,
                  deleteAfterClean = OperatingSystem.isWindows(),
                  forceSave = RandomForceSave.mmap()
                ).sweep()

              val standardFile =
                CoreFile.standardWritable(
                  path = genFilePath(),
                  fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
                  autoClose = true,
                  forceSave = RandomForceSave.standard()
                ).sweep()

              Seq(mmapFile, standardFile) foreach {
                file =>
                  //Either use batch append or singular appends.
                  if (testNumber % 2 == 0) {
                    //bytes4 will cause buffer overflow.
                    file.appendBatch(Array(bytes1, bytes2, bytes3, bytes4))
                    if (file.memoryMapped) file.isFull() shouldBe true
                  } else {
                    file.append(bytes1)
                    file.isFull() shouldBe false
                    file.append(bytes2)
                    file.isFull() shouldBe false
                    file.append(bytes3)
                    //          file.isFull() shouldBe true
                    file.append(bytes4) //overflow write, buffer gets extended
                    if (file.memoryMapped) file.isFull() shouldBe true
                  }

                  val allBytes = bytes1 ++ bytes2 ++ bytes3 ++ bytes4

                  if (file.memoryMapped)
                    file.readAll() shouldBe allBytes
                  else
                    Effect.readAllBytes(standardFile.path) shouldBe allBytes.toArray
              }
          }
      }
    }
  }

  "writing" should {
    "write bytes to a File" in {
      CoreTestSweeper.repeat(10.times) {
        implicit sweeper =>
          val bytes1 = Slice.wrap(randomBytes(100))
          val bytes2 = Slice.wrap(randomBytes(100))

          val filesAndBytes = createFiles(bytes1, bytes2) zip Seq(bytes1, bytes2)
          filesAndBytes should have size 2

          filesAndBytes foreach {
            case (file, expectedBytes) =>
              createFileReaders(file.path) foreach {
                reader =>
                  reader.readRemaining() shouldBe expectedBytes
                  invokePrivate_file(reader).readAll() shouldBe expectedBytes
              }
          }
      }
    }

    "write empty bytes to a File" in {
      CoreTestSweeper {
        implicit sweeper =>
          //This test might log NullPointerException because cleaner is being
          //invoked a MappedByteBuffer which is empty. This is a valid test
          //but does not occur in reality. If a file (Segment or Map) are empty
          //then they are not created which would only occur if all key-values
          //from that file were removed.
          val files = createFiles(Slice.emptyBytes, Slice.emptyBytes)
          files should have size 2
          files.foreach(_.existsOnDisk() shouldBe true)

          files foreach {
            file =>
              createFileReaders(file.path) foreach {
                reader =>
                  invokePrivate_file(reader).readAll() shouldBe Slice.emptyBytes
                  invokePrivate_file(reader).close()
              }
          }

          //ensure that file exists
          files foreach {
            file =>
              Effect.exists(file.path) shouldBe true
          }
      }
    }

    "write partially written bytes" in {
      CoreTestSweeper {
        implicit sweeper =>
          //size is 10 but only 2 bytes were written
          val incompleteBytes = Slice.allocate[Byte](10)
          incompleteBytes.addUnsignedInt(1)
          incompleteBytes.addUnsignedInt(2)
          incompleteBytes.size shouldBe 2

          val bytes = incompleteBytes.close()

          val files = createFiles(bytes, bytes)
          files should have size 2

          files foreach {
            file =>
              bytes shouldBe Slice.wrap(Effect.readAllBytes(file.path))
              bytes shouldBe file.readAll()
          }
      }
    }

    "fail to write if the file already exists" in {
      CoreTestSweeper {
        implicit sweeper =>
          val bytes = randomBytesSlice()

          val files = createFiles(bytes)
          files should have size 2

          files foreach {
            file =>
              //creating the same file again should fail
              anyOrder(
                assertThrows[FileAlreadyExistsException](createMMAPWriteableReadable(file.path, randomBytesSlice())),
                assertThrows[FileAlreadyExistsException](createStandardWriteableReadable(file.path, randomBytesSlice()))
              )
          }

          //flush
          files.foreach(_.close())

          files foreach {
            file =>
              file.readAll() shouldBe bytes
          }
      }
    }
  }

  "appending" should {
    "append bytes to the end of the StandardFile" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = genFilePath()
          val bytes = List(randomBytesSlice(), randomBytesSlice(), randomBytesSlice())

          val file =
            CoreFile.standardWritable(
              path = testFile,
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              forceSave = RandomForceSave.standard()
            )

          file.append(bytes(0))
          file.append(bytes(1))
          file.append(bytes(2))
          assertThrows[Throwable](file.read(position = 0, size = 1)) //not open for read

          file.close()

          val expectedAllBytes = bytes.foldLeft(List.empty[Byte])(_ ++ _).toSlice

          CoreFile.standardReadable(
            path = testFile,
            fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          ) ==> {
            file =>
              file.readAll() shouldBe expectedAllBytes
              file.close()
          }

          CoreFile.mmapReadable(
            path = testFile,
            fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true,
            deleteAfterClean = OperatingSystem.isWindows()
          ) ==> {
            file =>
              file.readAll() shouldBe expectedAllBytes
              file.close()
          }

          file.close()
      }
    }

    "append bytes to the end of the MMAP file" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = genFilePath()
          val bytes = List(randomBytesSlice(), randomBytesSlice(), randomBytesSlice())

          val allBytesSize = bytes.foldLeft(0)(_ + _.size)
          val file =
            CoreFile.mmapEmptyWriteableReadable(
              path = testFile,
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              bufferSize = allBytesSize,
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows(),
              forceSave = RandomForceSave.mmap()
            )

          file.append(bytes(0))
          file.append(bytes(1))
          file.append(bytes(2))
          file.get(position = 0) shouldBe bytes.head.head
          file.get(position = allBytesSize - 1) shouldBe bytes.last.last

          val expectedAllBytes = bytes.foldLeft(List.empty[Byte])(_ ++ _).toSlice

          file.readAll() shouldBe expectedAllBytes
          file.close() //close

          //reopen
          CoreFile.mmapReadable(
            path = testFile,
            fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true,
            deleteAfterClean = OperatingSystem.isWindows()
          ) ==> {
            file =>
              file.readAll() shouldBe expectedAllBytes
              file.close()
          }

          CoreFile.standardReadable(
            path = testFile,
            fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          ) ==> {
            file =>
              file.readAll() shouldBe expectedAllBytes
              file.close()
          }
      }
    }

    "append bytes by extending an overflown buffer of MMAP file" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = genFilePath()
          val bytes = List(randomBytesSlice(), randomBytesSlice(), randomBytesSlice(), randomBytesSlice(), randomBytesSlice())
          val allBytesSize = bytes.foldLeft(0)(_ + _.size)

          val file =
            CoreFile.mmapEmptyWriteableReadable(
              path = testFile,
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              bufferSize = bytes.head.size,
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows(),
              forceSave = RandomForceSave.mmap()
            )

          file.append(bytes(0))
          file.append(bytes(1))
          file.append(bytes(2))
          file.append(bytes(3))
          file.append(bytes(4))
          file.get(position = 0) shouldBe bytes.head.head
          file.get(position = allBytesSize - 1) shouldBe bytes.last.last

          val expectedAllBytes = bytes.foldLeft(List.empty[Byte])(_ ++ _).toSlice

          file.readAll() shouldBe expectedAllBytes
          file.close() //close

          //reopen
          CoreFile.mmapReadable(
            path = testFile,
            fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true,
            deleteAfterClean = OperatingSystem.isWindows()
          ) ==> {
            file =>
              file.readAll() shouldBe expectedAllBytes
              file.close()
          }

          CoreFile.standardReadable(
            path = testFile,
            fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          ) ==> {
            file =>
              file.readAll() shouldBe expectedAllBytes
              file.close()
          }
      }
    }

    "not fail when appending empty bytes to StandardFile" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._

          val file =
            CoreFile.standardWritable(
              path = genFilePath(),
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              forceSave = RandomForceSave.standard()
            )

          file.append(Slice.emptyBytes)

          CoreFile.standardReadable(
            path = file.path,
            fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          ) ==> {
            file =>
              file.readAll() shouldBe empty
              file.close()
          }
          file.close()
      }
    }

    "not fail when appending empty bytes to MMAPFile" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._

          val file =
            CoreFile.mmapEmptyWriteableReadable(
              path = genFilePath(),
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              bufferSize = 100,
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows(),
              forceSave = RandomForceSave.mmap()
            )

          file.append(Slice.emptyBytes)
          file.readAll() shouldBe Slice.fill(file.fileSize())(0)
          file.close()

          CoreFile.mmapReadable(
            path = file.path,
            fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true,
            deleteAfterClean = OperatingSystem.isWindows()
          ) ==> {
            file2 =>
              file2.readAll() shouldBe Slice.fill(file.fileSize())(0)
              file2.close()
          }
      }
    }
  }

  "read and find" should {
    "read and find bytes at a position from a StandardFile" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._
          val testFile = genFilePath()
          val bytes = randomBytesSlice(100)

          val file =
            CoreFile.standardWritable(
              path = testFile,
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              forceSave = RandomForceSave.standard()
            )

          file.append(bytes)
          assertThrows[Throwable](file.read(position = 0, size = 1)) //not open for read

          file.close()

          val readFile = CoreFile.standardReadable(
            path = testFile,
            fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
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
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._
          val bytes = randomBytesSlice(100)

          val file =
            CoreFile.standardWritable(
              path = genFilePath(),
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              forceSave = RandomForceSave.standard()
            )

          file.append(bytes)

          file.delete()
          file.existsOnDisk() shouldBe false
          file.isOpen shouldBe false
          file.isFileDefined shouldBe false
      }
    }

    "delete a MMAPFile" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._

          val file =
            CoreFile.mmapWriteableReadable(
              path = genFilePath(),
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows(),
              forceSave = RandomForceSave.mmap(),
              bytes = randomBytesSlice()
            ).sweep()

          file.close()

          file.delete()

          if (OperatingSystem.isWindows())
            sweeper.receiveAll()

          file.existsOnDisk() shouldBe false
          file.isOpen shouldBe false
          file.isFileDefined shouldBe false
      }
    }
  }

  "copy" should {
    "copy a StandardFile" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._
          val bytes = randomBytesSlice(100)

          val file =
            CoreFile.standardWritable(
              path = genFilePath(),
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              forceSave = RandomForceSave.standard()
            )

          file.append(bytes)

          val targetFile = genFilePath()
          file.copyTo(targetFile) shouldBe targetFile

          CoreFile.standardReadable(
            path = targetFile,
            fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          ) ==> {
            file =>
              file.readAll() shouldBe bytes
              file.close()
          }

          file.close()
      }
    }

    "copy a MMAPFile" in {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._
          val bytes = randomBytesSlice(100)

          val file =
            CoreFile.mmapEmptyWriteableReadable(
              path = genFilePath(),
              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
              bufferSize = bytes.size,
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows(),
              forceSave = RandomForceSave.mmap()
            )

          file.append(bytes)
          file.isFull() shouldBe true
          file.close()

          val targetFile = genFilePath()
          file.copyTo(targetFile) shouldBe targetFile

          CoreFile.standardReadable(
            path = targetFile,
            fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
            autoClose = true
          ) ==> {
            file =>
              file.readAll() shouldBe bytes
              file.close()
          }
      }
    }
  }

  //  "copying large number of memory-mapped files" in {
  //    CoreTestSweeper {
  //      implicit sweeper =>
  //        import swaydb.testkit.RunThis._
  //
  //        runThis(1000.times, log = true) {
  //          import sweeper._
  //          val testFile = genFilePath()
  //          import swaydb.config.util.StorageUnits._
  //          val bytes = randomBytesSlice(1.kb)
  //          val file =
  //            CoreFile.mmapWriteAndRead(
  //              path = testFile,
  //              fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
  //              autoClose = true,
  //              deleteAfterClean = OperatingSystem.isWindows(),
  //              blockCacheFileId = idGenerator.nextID,
  //              bytes = bytes
  //            )
  //
  //          file.readAll()
  //
  //          Effect.copy(testFile, genFilePath())
  //        }
  //    }
  //  }

  //  "Concurrently opening files" should {
  //    "result in Busy exception" in {
  //      //create a file
  //      val bytes = Slice(randomBytes())
  //      val file = CoreFile.mmapInit(genFilePath(), bytes.size, autoClose = true).runIO
  //      file.append(bytes).runIO
  //
  //      //concurrently close and read the same file.
  //      val ios =
  //        (1 to 500).par map {
  //          _ =>
  //            if (randomBoolean) Future(file.close())
  //            file.readAll()
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

  "transfer" in {
    CoreTestSweeper.repeat(1.times) {
      implicit sweeper =>

        val bytes = randomBytesSlice(size = 100)
        //test when files are both channel and mmap
        val files = createFiles(mmapBytes = bytes, standardBytes = bytes)
        files should have size 2

        files foreach {
          file =>
            //transfer bytes to both mmap and channel files
            val targetMMAPFile = createWriteableMMAPFile(genFilePath(), 100)
            val targetStandardFile = createWriteableStandardFile(genFilePath())

            Seq(targetMMAPFile, targetStandardFile) foreach {
              targetFile =>
                file.transfer(position = 0, count = 10, transferTo = targetFile)
                targetFile.close()

                val fileReaders = createFileReaders(targetFile.path)
                fileReaders should have size 2
                fileReaders foreach {
                  reader =>
                    reader.read(10) shouldBe bytes.take(10)
                }
            }
        }
    }
  }

  "read blockSize" when {
    "size is empty" in {
      CoreTestSweeper {
        implicit sweeper =>
          createFiles(randomBytesSlice(100), randomBytesSlice(100)) foreach {
            file =>
              file.read(0, 0, 10) shouldBe empty
          }
      }
    }

    "blockSize == size" in {
      CoreTestSweeper {
        implicit sweeper =>
          val bytes = randomBytesSlice(100)

          createFiles(bytes, bytes) foreach {
            file =>
              file.read(0, bytes.size, bytes.size) shouldBe bytes
          }
      }
    }

    "blockSize > size" in {
      CoreTestSweeper {
        implicit sweeper =>
          val bytes = randomBytesSlice(100)

          createFiles(bytes, bytes) foreach {
            file =>
              file.read(0, bytes.size, bytes.size + 1).asInstanceOf[Slice[Byte]] shouldBe bytes
          }
      }
    }

    "blockSize < size" when {
      "size is multiple of blockSize" in {
        CoreTestSweeper {
          implicit sweeper =>
            val bytes = randomBytesSlice(100)

            createFiles(bytes, bytes) foreach {
              file =>
                val slices = file.read(0, bytes.size, 10).asInstanceOf[Slices[Byte]].slices
                slices should have size 10

                val zip = slices.zip(bytes.grouped(10).toList) //.toList for scala 2.12

                zip should have size 10

                zip foreach {
                  case (readSlice, expectedSlice) =>
                    readSlice shouldBe expectedSlice
                }

                slices.flatten.toList shouldBe bytes.toList
            }
        }
      }

      "size is not a multiple of blockSize" in {
        CoreTestSweeper {
          implicit sweeper =>
            val bytes = randomBytesSlice(100)

            createFiles(bytes, bytes) foreach {
              file =>
                //blockSize is 9 so expect 12 slices to get created with the last slice being of size 1
                val slices = file.read(0, bytes.size, 9).shouldBeInstanceOf[Slices[Byte]].slices
                slices should have size 12

                slices.indices foreach {
                  index =>
                    //underlying array sizes should be exact. No unnecessary arrays should get created
                    if (index == slices.length - 1)
                      slices(index).underlyingArraySize shouldBe 1 //last slice is of size 1
                    else
                      slices(index).underlyingArraySize shouldBe 9

                    slices(index) shouldBe bytes.drop(index * 9).take(9)
                }
            }
        }
      }
    }
  }
}
