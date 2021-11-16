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

package swaydb.core.build

import swaydb.Error.IO
import swaydb.Exception.InvalidDirectoryType
import swaydb.IOValues._
import swaydb.core.TestData._
import swaydb.core.{TestBase, TestCaseSweeper}
import swaydb.config.DataType
import swaydb.slice.Slice
import swaydb.effect.{Effect, Extension}
import swaydb.utils.ByteSizeOf

import java.nio.file.FileAlreadyExistsException
import scala.util.Random

class BuildSpec extends TestBase {

  "write" should {
    "create a build.info file" in {
      TestCaseSweeper {
        implicit sweeper =>
          DataType.all foreach {
            dataType =>
              val version = Build.Version(major = randomIntMax(), minor = randomIntMax(), revision = randomIntMax())
              val buildInfo = Build.Info(version = version, dataType = dataType)

              val folder = randomDir
              Build.write(folder, buildInfo).value shouldBe folder.resolve(Build.fileName)

              val readBuildInfo = Build.read(folder).value
              readBuildInfo shouldBe buildInfo
          }
      }
    }

    "fail if build.info already exists" in {
      TestCaseSweeper {
        implicit sweeper =>
          val folder = createRandomDir
          val file = Effect.createFile(folder.resolve(Build.fileName))
          val fileContent = Effect.readAllBytes(file)

          Build.write(folder, DataType.Map).left.value shouldBe a[FileAlreadyExistsException]

          //file content is unaffected
          Effect.readAllBytes(file) shouldBe fileContent
      }
    }
  }

  "read" should {
    "return fresh" when {
      "the folder does not exist" in {
        TestCaseSweeper {
          implicit sweeper =>

            val folder = randomDir
            Effect.exists(folder) shouldBe false
            Build.read(folder).value shouldBe Build.Fresh
        }
      }

      "the folder exists but is empty" in {
        TestCaseSweeper {
          implicit sweeper =>

            val folder = createRandomDir
            Effect.exists(folder) shouldBe true
            Build.read(folder).value shouldBe Build.Fresh
        }
      }
    }

    "return NoBuildInfo" when {
      "non-empty folder exists without build.info file" in {
        TestCaseSweeper {
          implicit sweeper =>
            Extension.all foreach {
              extension =>
                val folder = createRandomDir
                val file = Effect.createFile(folder.resolve(s"somefile.$extension"))

                Effect.exists(folder) shouldBe true
                Effect.exists(file) shouldBe true

                Build.read(folder).value shouldBe Build.NoBuildInfo

                Build.read(file).value shouldBe Build.NoBuildInfo
            }
        }
      }
    }

    //full read is already tested in writes test-case

    "fail" when {
      "invalid crc" in {
        TestCaseSweeper {
          implicit sweeper =>
            DataType.all foreach {
              dataType =>
                val version = Build.Version(major = randomIntMax(), minor = randomIntMax(), revision = randomIntMax())
                val buildInfo = Build.Info(version = version, dataType = dataType)

                val folder = randomDir
                val file = Build.write(folder, buildInfo).value

                //drop crc
                Effect.overwrite(file, Effect.readAllBytes(file).drop(1))

                Build.read(folder).left.value.getMessage should startWith(s"assertion failed: Invalid CRC.")
            }
        }
      }

      "invalid formatId" in {
        TestCaseSweeper {
          implicit sweeper =>
            DataType.all foreach {
              dataType =>
                val version = Build.Version(major = randomIntMax(), minor = randomIntMax(), revision = randomIntMax())
                val buildInfo = Build.Info(version = version, dataType = dataType)

                val folder = randomDir
                val file = Build.write(folder, buildInfo).value

                val existsBytes = Effect.readAllBytes(file)

                val bytesWithInvalidFormatId = Slice.of[Byte](existsBytes.size)
                bytesWithInvalidFormatId addAll existsBytes.take(ByteSizeOf.long) //keep CRC
                bytesWithInvalidFormatId add (Build.formatId + 1).toByte //change formatId
                bytesWithInvalidFormatId addAll existsBytes.drop(ByteSizeOf.long + 1) //keep the rest
                bytesWithInvalidFormatId.isFull shouldBe true

                //overwrite
                Effect.overwrite(file, bytesWithInvalidFormatId.toArray)

                //Invalid formatId will return invalid CRC.
                //            Build.read(folder).left.value.getMessage shouldBe s"assertion failed: $file has invalid formatId. ${Build.formatId + 1} != ${Build.formatId}"
                Build.read(folder).left.value.getMessage should startWith(s"assertion failed: Invalid CRC.")
            }
        }
      }
    }
  }

  "validateOrCreate" should {
    "fail on an existing non-empty directories" when {
      "DisallowOlderVersions" in {
        TestCaseSweeper {
          implicit sweeper =>
            DataType.all foreach {
              dataType =>
                Extension.all foreach {
                  extension =>

                    val folder = createRandomDir
                    val file = folder.resolve(s"somefile.$extension")

                    Effect.createFile(file)
                    Effect.exists(folder) shouldBe true
                    Effect.exists(file) shouldBe true

                    implicit val validator = BuildValidator.DisallowOlderVersions(dataType)
                    Build.validateOrCreate(folder).left.value.getMessage should startWith("Missing build.info file. This directory might be an incompatible older version of SwayDB. Current version:")
                }
            }
        }
      }
    }

    "fail on an existing directory with different dataType" when {
      "DisallowOlderVersions" in {
        TestCaseSweeper {
          implicit sweeper =>
            DataType.all foreach {
              invalidDataType =>

                val dataType = Random.shuffle(DataType.all.toList).find(_ != invalidDataType).get

                implicit val validator = BuildValidator.DisallowOlderVersions(dataType)
                val folder = createRandomDir
                Build.validateOrCreate(folder)

                Effect.exists(folder) shouldBe true
                Effect.exists(folder.resolve(Build.fileName)) shouldBe true

                val error = Build.validateOrCreate(folder)(IO.ExceptionHandler, BuildValidator.DisallowOlderVersions(invalidDataType))
                error.left.value.exception shouldBe InvalidDirectoryType(invalidDataType.name, dataType.name)
            }
        }
      }
    }

    "pass on empty directory" when {
      "DisallowOlderVersions" in {
        TestCaseSweeper {
          implicit sweeper =>
            DataType.all foreach {
              dataType =>
                val folder = createRandomDir
                Effect.exists(folder) shouldBe true

                implicit val validator = BuildValidator.DisallowOlderVersions(dataType)
                Build.validateOrCreate(folder).value

                Build.read(folder).value shouldBe a[Build.Info]
            }
        }
      }
    }

    "pass on non existing directory" when {
      "DisallowOlderVersions" in {
        TestCaseSweeper {
          implicit sweeper =>
            DataType.all foreach {
              dataType =>
                val folder = randomDir
                Effect.exists(folder) shouldBe false

                implicit val validator = BuildValidator.DisallowOlderVersions(dataType)
                Build.validateOrCreate(folder).value

                Build.read(folder).value shouldBe a[Build.Info]
            }
        }
      }
    }
  }
}
