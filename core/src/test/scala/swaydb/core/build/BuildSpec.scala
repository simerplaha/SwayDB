/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.build

import java.nio.file.FileAlreadyExistsException

import swaydb.IOValues._
import swaydb.core.TestData._
import swaydb.core.io.file.Effect
import swaydb.core.{TestBase, TestCaseSweeper}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

class BuildSpec extends TestBase {

  "write" should {
    "create a build.info file" in {
      TestCaseSweeper {
        implicit sweeper =>
          val buildInfo = Build.Info(major = randomIntMax(), minor = randomIntMax(), revision = randomIntMax())

          val folder = randomDir
          Build.write(folder, buildInfo).value shouldBe folder.resolve(Build.fileName)

          val readBuildInfo = Build.read(folder).value
          readBuildInfo shouldBe buildInfo
      }
    }

    "fail is build.info already exists" in {
      TestCaseSweeper {
        implicit sweeper =>
          val folder = createRandomDir
          val file = Effect.createFile(folder.resolve(Build.fileName))
          val fileContent = Effect.readAllBytes(file)

          Build.write(folder).left.value shouldBe a[FileAlreadyExistsException]

          //file content is unaffected
          Effect.readAllBytes(file) shouldBe fileContent
      }
    }
  }

  "read" should {
    "return fresh" when {
      "it's a new folder" in {
        TestCaseSweeper {
          implicit sweeper =>

            val folder = randomDir
            Effect.exists(folder) shouldBe false
            Build.read(folder).value shouldBe Build.Fresh
        }
      }
    }

    "return NoBuildInfo" when {
      "fold exists but there was no build.info file" in {
        TestCaseSweeper {
          implicit sweeper =>

            val folder = createRandomDir
            Effect.exists(folder) shouldBe true
            Build.read(folder).value shouldBe Build.NoBuildInfo
        }
      }
    }

    //full read is already tested in writes test-case

    "fail" when {
      "invalid crc" in {
        TestCaseSweeper {
          implicit sweeper =>
            val buildInfo = Build.Info(major = randomIntMax(), minor = randomIntMax(), revision = randomIntMax())

            val folder = randomDir
            val file = Build.write(folder, buildInfo).value

            //drop crc
            Effect.overwrite(file, Effect.readAllBytes(file).dropHead())

            Build.read(folder).left.value.getMessage should startWith(s"assertion failed: $file has invalid CRC.")
        }
      }

      "invalid formatId" in {
        TestCaseSweeper {
          implicit sweeper =>
            val buildInfo = Build.Info(major = randomIntMax(), minor = randomIntMax(), revision = randomIntMax())

            val folder = randomDir
            val file = Build.write(folder, buildInfo).value

            val existsBytes = Effect.readAllBytes(file)

            val bytesWithInvalidFormatId = Slice.create[Byte](existsBytes.size)
            bytesWithInvalidFormatId addAll existsBytes.take(ByteSizeOf.long) //keep CRC
            bytesWithInvalidFormatId add (Build.formatId + 1).toByte //change formatId
            bytesWithInvalidFormatId addAll existsBytes.drop(ByteSizeOf.long + 1) //keep the rest
            bytesWithInvalidFormatId.isFull shouldBe true

            //overwrite
            Effect.overwrite(file, bytesWithInvalidFormatId)

            //Invalid formatId will return invalid CRC.
            //            Build.read(folder).left.value.getMessage shouldBe s"assertion failed: $file has invalid formatId. ${Build.formatId + 1} != ${Build.formatId}"
            Build.read(folder).left.value.getMessage should startWith(s"assertion failed: $file has invalid CRC.")
        }
      }
    }
  }

  "validateOrCreate" should {
    "fail on an existing non-empty directories" when {
      "DisallowOlderVersions" in {
        TestCaseSweeper {
          implicit sweeper =>
            val folder = createRandomDir
            val file = folder.resolve("somefile.txt")

            Effect.createFile(file)
            Effect.exists(folder) shouldBe true
            Effect.exists(file) shouldBe true

            implicit val validator = BuildValidator.DisallowOlderVersions
            Build.validateOrCreate(folder).left.value.getMessage should startWith("This directory is not empty or is an older version of SwayDB which is incompatible with")
        }
      }
    }

    "pass on empty directory" when {
      "DisallowOlderVersions" in {
        TestCaseSweeper {
          implicit sweeper =>
            val folder = createRandomDir
            Effect.exists(folder) shouldBe true

            implicit val validator = BuildValidator.DisallowOlderVersions
            Build.validateOrCreate(folder).value

            Build.read(folder).value shouldBe a[Build.Info]
        }
      }
    }

    "pass on non existing directory" when {
      "DisallowOlderVersions" in {
        TestCaseSweeper {
          implicit sweeper =>
            val folder = randomDir
            Effect.exists(folder) shouldBe false

            implicit val validator = BuildValidator.DisallowOlderVersions
            Build.validateOrCreate(folder).value

            Build.read(folder).value shouldBe a[Build.Info]
        }
      }
    }
  }
}
