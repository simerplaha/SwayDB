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

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.{Benchmark, IO}
import swaydb.IOValues._
import swaydb.core.CoreTestData._
import swaydb.core.TestSweeper
import swaydb.core.file.CoreFileTestKit._
import swaydb.effect.Effect
import swaydb.slice.Slice
import swaydb.testkit.RunThis._
import swaydb.utils.Extension
import swaydb.utils.StorageUnits._

import java.nio.file.Paths

class EffectSpec extends AnyWordSpec with Matchers {

  "fileId" should {

    "value the file id" in {
      Effect.numberFileId(Paths.get("/one/1.log")).runRandomIO.right.value shouldBe(1, Extension.Log)
      Effect.numberFileId(Paths.get("/one/two/10.log")).runRandomIO.right.value shouldBe(10, Extension.Log)
      Effect.numberFileId(Paths.get("/one/two/three/1000.seg")).runRandomIO.right.value shouldBe(1000, Extension.Seg)
    }

    "fail if the file's name is not an integer" in {
      val path = Paths.get("/one/notInt.log")
      IO(Effect.numberFileId(path)).left.value shouldBe swaydb.Exception.NotAnIntFile(path)
    }

    "fail if the file has invalid extension" in {
      val path = Paths.get("/one/1.txt")
      IO(Effect.numberFileId(path)).left.runRandomIO.right.value shouldBe swaydb.Exception.UnknownExtension(path)
    }
  }

  "folderId" should {
    "value the folderId" in {
      Effect.folderId(Paths.get("/one/1")) shouldBe 1
      Effect.folderId(Paths.get("/one/two/10")) shouldBe 10
      Effect.folderId(Paths.get("/one/two/three/1000")) shouldBe 1000
    }
  }

  "incrementFileId" should {
    "return a new file path with incremented file id" in {
      Effect.incrementFileId(Paths.get("/one/1.log")).runRandomIO.right.value shouldBe Paths.get("/one/2.log")
      Effect.incrementFileId(Paths.get("/one/two/10.log")).runRandomIO.right.value shouldBe Paths.get("/one/two/11.log")
      Effect.incrementFileId(Paths.get("/one/two/three/1000.seg")).runRandomIO.right.value shouldBe Paths.get("/one/two/three/1001.seg")
    }
  }

  "incrementFolderId" should {
    "return a new file path with incremented folder id" in {
      Effect.incrementFolderId(Paths.get("/one/1")) shouldBe Paths.get("/one/2")
      Effect.incrementFolderId(Paths.get("/one/two/10")) shouldBe Paths.get("/one/two/11")
      Effect.incrementFolderId(Paths.get("/one/two/three/1000")) shouldBe Paths.get("/one/two/three/1001")
    }
  }

  "files" should {
    "fetch all the files in sorted order" in {
      TestSweeper {
        implicit sweeper =>

          val dir = createRandomIntDirectory()
          val actual =
            Seq(
              dir.resolve(s"1.${Extension.Log}"),
              dir.resolve(s"4.${Extension.Log}"),
              dir.resolve(s"99.${Extension.Log}"),
              dir.resolve(s"2.${Extension.Log}"),
              dir.resolve(s"299.${Extension.Log}"),
              dir.resolve(s"3.${Extension.Log}"),
              dir.resolve(s"10.${Extension.Log}"),
              dir.resolve(s"33.${Extension.Log}")
            )

          actual.foreach {
            path =>
              Effect.createFile(path).runRandomIO.right.value
          }

          val expect =
            Seq(
              dir.resolve(s"1.${Extension.Log}"),
              dir.resolve(s"2.${Extension.Log}"),
              dir.resolve(s"3.${Extension.Log}"),
              dir.resolve(s"4.${Extension.Log}"),
              dir.resolve(s"10.${Extension.Log}"),
              dir.resolve(s"33.${Extension.Log}"),
              dir.resolve(s"99.${Extension.Log}"),
              dir.resolve(s"299.${Extension.Log}")
            )

          Effect.files(dir, Extension.Log) shouldBe expect
      }
    }
  }

  "folders" should {
    "fetch all the folders in sorted order" in {
      TestSweeper {
        implicit sweeper =>

          val dir = createRandomIntDirectory()
          val actual =
            Seq(
              dir.resolve("1"),
              dir.resolve("10"),
              dir.resolve("7"),
              dir.resolve("15"),
              dir.resolve("7676"),
              dir.resolve("123"),
              dir.resolve("0"),
              dir.resolve("5454")
            )

          actual.foreach {
            path =>
              Effect.createDirectoryIfAbsent(path)
          }

          val expect =
            Seq(
              dir.resolve("0"),
              dir.resolve("1"),
              dir.resolve("7"),
              dir.resolve("10"),
              dir.resolve("15"),
              dir.resolve("123"),
              dir.resolve("5454"),
              dir.resolve("7676")
            )

          Effect.folders(dir) shouldBe expect
      }
    }
  }

  "segmentFilesOnDisk" should {
    "fetch all segment files in order" in {
      TestSweeper {
        implicit sweeper =>

          val dir1 = createRandomIntDirectory()
          val dir2 = createRandomIntDirectory()
          val dir3 = createRandomIntDirectory()
          val dirs = Seq(dir1, dir2, dir3)

          dirs foreach {
            dir =>
              val actual =
                Seq(
                  dir.resolve("1.seg"),
                  dir.resolve("10.seg"),
                  dir.resolve("7.seg"),
                  dir.resolve("15.seg"),
                  dir.resolve("7676.seg"),
                  dir.resolve("123.seg"),
                  dir.resolve("0.seg"),
                  dir.resolve("5454.seg")
                )
              actual.foreach {
                path =>
                  Effect.createFileIfAbsent(path)
              }
          }

          val expect =
            Seq(
              dir1.resolve("0.seg"),
              dir2.resolve("0.seg"),
              dir3.resolve("0.seg"),
              dir1.resolve("1.seg"),
              dir2.resolve("1.seg"),
              dir3.resolve("1.seg"),
              dir1.resolve("7.seg"),
              dir2.resolve("7.seg"),
              dir3.resolve("7.seg"),
              dir1.resolve("10.seg"),
              dir2.resolve("10.seg"),
              dir3.resolve("10.seg"),
              dir1.resolve("15.seg"),
              dir2.resolve("15.seg"),
              dir3.resolve("15.seg"),
              dir1.resolve("123.seg"),
              dir2.resolve("123.seg"),
              dir3.resolve("123.seg"),
              dir1.resolve("5454.seg"),
              dir2.resolve("5454.seg"),
              dir3.resolve("5454.seg"),
              dir1.resolve("7676.seg"),
              dir2.resolve("7676.seg"),
              dir3.resolve("7676.seg")
            )

          Effect.segmentFilesOnDisk(dirs) shouldBe expect
      }
    }
  }

  "walkDelete" in {
    TestSweeper {
      implicit sweeper =>
        //this iterators counts the number of nested directories to create.
        (0 to 10) foreach {
          maxNestedDirectories =>
            //initialise the root test directory
            val testDirectory = sweeper.testDirPath

            //but path for nested directory hierarchy.
            val testCaseDirectory =
              (0 to maxNestedDirectories).foldLeft(testDirectory) {
                case (dir, i) =>
                  dir.resolve(s"dir$maxNestedDirectories$i")
              }

            Effect.createDirectoriesIfAbsent(testCaseDirectory)

            val testFile = Effect.createFile(testCaseDirectory.resolve("file.txt"))

            println(s"Creating file = $testFile")

            //the testDirectory, testCaseDirectory and the testFile should exist
            Effect.exists(testDirectory) shouldBe true
            Effect.exists(testCaseDirectory) shouldBe true
            Effect.exists(testFile) shouldBe true

            //walk delete all in any order and they should all get deleted.
            Seq(
              () => {
                Effect.walkDelete(testFile)
                Effect.exists(testFile) shouldBe false
              },

              () => {
                Effect.walkDelete(testCaseDirectory)
                Effect.exists(testCaseDirectory) shouldBe false
              },

              () => {
                Effect.walkDelete(testDirectory)
                Effect.exists(testDirectory) shouldBe false
              }
            ).runThisRandomly
        }
    }
  }

  "transfer" in {
    TestSweeper {
      implicit sweeper =>
        val bytes = randomBytesSlice(size = 100)
        //test when files are both channel and mmap
        val files = createFiles(mmapBytes = bytes, standardBytes = bytes)
        files should have size 2

        files foreach {
          file =>
            //transfer bytes to both mmap and channel files
            val targetMMAPFile = createWriteableMMAPFile(randomFilePath(), 100)
            val targetStandardFile = createWriteableStandardFile(randomFilePath())

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

  "benchmark" in {
    TestSweeper {
      implicit sweeper =>

        val fileSize = 4.mb
        val flattenBytes = randomBytesSlice(fileSize)
        val groupBytes = flattenBytes.groupedSlice(8)

        //20.mb
        //0.067924621 seconds
        //4.mb
        //0.057647201 seconds & 0.047565694 seconds
        val groupedPath = Benchmark("groupBytes")(Effect.write(randomFilePath(), groupBytes.mapToSlice(_.toByteBufferWrap())))
        Slice.wrap(Effect.readAllBytes(groupedPath)) shouldBe flattenBytes

        //20.mb
        //0.077162871 seconds
        //4.mb
        //0.05330862 seconds & 0.045989919 seconds
        val flattenedPath = Benchmark("flattenBytes")(Effect.write(randomFilePath(), flattenBytes.toByteBufferWrap()))
        Slice.wrap(Effect.readAllBytes(flattenedPath)) shouldBe flattenBytes
    }
  }

  "isEmptyOrNotExists" when {
    "folder does not exist" in {
      TestSweeper {
        implicit sweeper =>
          val dir = randomDir()
          Effect.notExists(dir) shouldBe true

          Effect.isEmptyOrNotExists(dir).value shouldBe true
      }
    }

    "folder exists but is empty" in {
      TestSweeper {
        implicit sweeper =>
          val dir = createRandomDir()
          Effect.exists(dir) shouldBe true

          Effect.isEmptyOrNotExists(dir).value shouldBe true
      }
    }

    "folder exists and is non-empty" in {
      TestSweeper {
        implicit sweeper =>
          Extension.all foreach {
            extension =>
              val dir = createRandomDir()
              Effect.exists(dir) shouldBe true

              Effect.createFile(dir.resolve(s"somefile.$extension"))

              Effect.isEmptyOrNotExists(dir).value shouldBe false
          }
      }
    }

    "input is a file" in {
      TestSweeper {
        implicit sweeper =>
          Extension.all foreach {
            extension =>
              val file = createRandomDir().resolve(s"somefile.$extension")
              Effect.createFile(file)

              Effect.exists(file) shouldBe true

              Effect.isEmptyOrNotExists(file).value shouldBe false
          }
      }
    }
  }
}
