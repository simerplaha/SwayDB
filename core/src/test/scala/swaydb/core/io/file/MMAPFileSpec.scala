/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.io.file

import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicBoolean
import org.scalamock.scalatest.MockFactory
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.sweeper.ByteBufferCleaner
import swaydb.core.{TestBase, TestCaseSweeper, TestForceSave}
import swaydb.data.RunThis._
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

class MMAPFileSpec extends TestBase with MockFactory {

  "BEHAVIOUR TEST - cleared MappedByteBuffer without forceSave" should {
    "not fatal JVM terminate" when {
      "writing, reading & copying" in {
        TestCaseSweeper {
          implicit sweeper =>
            runThis(50.times, log = true) {
              //create random path and byte slice
              val path = randomFilePath
              val bytes = randomBytesSlice(10.mb)

              /**
               * BEHAVIOR 1 - Open a memory-mapped file in read & write mode.
               */
              {
                //create a readWriteChannel
                val readWriteChannel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
                val readWriteBuff = readWriteChannel.map(MapMode.READ_WRITE, 0, bytes.size)
                //save bytes to the channel randomly in groups of small slices or one large slice
                eitherOne(
                  readWriteBuff.put(bytes.toByteBufferWrap),
                  bytes.groupedSlice(randomIntMax(10000) max 1).foreach {
                    slice =>
                      readWriteBuff.put(slice.toByteBufferWrap)
                  },
                  bytes.groupedSlice(randomIntMax(1000) max 1).foreach {
                    slice =>
                      readWriteBuff.put(slice.toByteBufferWrap)
                  },
                  bytes.groupedSlice(randomIntMax(100) max 1).foreach {
                    slice =>
                      readWriteBuff.put(slice.toByteBufferWrap)
                  },
                  bytes.groupedSlice(randomIntMax(10) max 1).foreach {
                    slice =>
                      readWriteBuff.put(slice.toByteBufferWrap)
                  }
                )

                //forceSave configurations
                val forceSave = TestForceSave.mmap()
                val alreadyForced = randomBoolean()
                val forced = new AtomicBoolean(alreadyForced)

                implicit val applier: ForceSaveApplier =
                  if (alreadyForced)
                    mock[ForceSaveApplier] //mock it so it not get invoked
                  else
                    ForceSaveApplier.On

                //do not forceSave and clear the in-memory bytes.
                ByteBufferCleaner.initialiseCleaner(readWriteBuff, path, forced, forceSave).value

                if (alreadyForced)
                  forced.get() shouldBe true
                else if (forceSave.enabledBeforeClean)
                  forced.get() shouldBe true
                else
                  forced.get() shouldBe false

                //copy the file and read and aldo read the bytes form path in any order should succeed.
                Seq(
                  () => {
                    //copy file to another path
                    val path2 = Effect.copy(path, randomFilePath)
                    Slice(Effect.readAllBytes(path2)) shouldBe bytes
                  },
                  () =>
                    //read all the bytes from disk and they exist
                    Slice(Effect.readAllBytes(path)) shouldBe bytes
                ).runThisRandomly
              }

              /**
               * BEHAVIOR 2 - REOPEN the above memory-mapped file in read mode.
               */
              {
                //open the file as another memory-mapped file in readonly mode
                val readOnlyChannel = FileChannel.open(path, StandardOpenOption.READ)
                val readOnlyBuff = readOnlyChannel.map(MapMode.READ_ONLY, 0, bytes.size)
                //read all bytes from the readOnly buffer
                val array = new Array[Byte](bytes.size)
                readOnlyBuff.get(array)
                array shouldBe bytes.toArray

                val forceSaveAgain = TestForceSave.mmap()
                val alreadyForced2 = randomBoolean()
                val forcedAgain = new AtomicBoolean(alreadyForced2)

                implicit val applier: ForceSaveApplier =
                  if (alreadyForced2)
                    mock[ForceSaveApplier] //mock it so it not get invoked
                  else
                    ForceSaveApplier.On

                //clear the buffer again
                ByteBufferCleaner.initialiseCleaner(readOnlyBuff, path, forcedAgain, forceSaveAgain).value

                if (alreadyForced2)
                  forcedAgain.get() shouldBe true
                else if (forceSaveAgain.enabledBeforeClean)
                  forcedAgain.get() shouldBe true
                else
                  forcedAgain.get() shouldBe false

                //read bytes from disk and they should exist.
                Slice(Effect.readAllBytes(path)) shouldBe bytes
              }
            }
        }
      }
    }
  }
}
