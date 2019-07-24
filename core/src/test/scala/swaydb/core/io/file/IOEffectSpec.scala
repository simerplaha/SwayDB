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

import java.nio.file.Paths

import swaydb.core.IOValues._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.util.{Benchmark, Extension}
import swaydb.data.util.StorageUnits._
import swaydb.data.io.Core.Error.Private.ErrorHandler

class IOEffectSpec extends TestBase {

  "fileId" should {
    "value the file id" in {
      IOEffect.fileId(Paths.get("/one/1.log")).runIO shouldBe(1, Extension.Log)
      IOEffect.fileId(Paths.get("/one/two/10.log")).runIO shouldBe(10, Extension.Log)
      IOEffect.fileId(Paths.get("/one/two/three/1000.seg")).runIO shouldBe(1000, Extension.Seg)
    }

    "fail if the file's name is not an integer" in {
      val path = Paths.get("/one/notInt.log")
      IOEffect.fileId(path).failed.runIO.exception shouldBe NotAnIntFile(path)
    }

    "fail if the file has invalid extension" in {
      val path = Paths.get("/one/1.txt")
      IOEffect.fileId(path).failed.runIO.exception shouldBe UnknownExtension(path)
    }
  }

  "folderId" should {
    "value the folderId" in {
      IOEffect.folderId(Paths.get("/one/1")) shouldBe 1
      IOEffect.folderId(Paths.get("/one/two/10")) shouldBe 10
      IOEffect.folderId(Paths.get("/one/two/three/1000")) shouldBe 1000
    }
  }

  "incrementFileId" should {
    "return a new file path with incremented file id" in {
      IOEffect.incrementFileId(Paths.get("/one/1.log")).runIO shouldBe Paths.get("/one/2.log")
      IOEffect.incrementFileId(Paths.get("/one/two/10.log")).runIO shouldBe Paths.get("/one/two/11.log")
      IOEffect.incrementFileId(Paths.get("/one/two/three/1000.seg")).runIO shouldBe Paths.get("/one/two/three/1001.seg")
    }
  }

  "incrementFolderId" should {
    "return a new file path with incremented folder id" in {
      IOEffect.incrementFolderId(Paths.get("/one/1")) shouldBe Paths.get("/one/2")
      IOEffect.incrementFolderId(Paths.get("/one/two/10")) shouldBe Paths.get("/one/two/11")
      IOEffect.incrementFolderId(Paths.get("/one/two/three/1000")) shouldBe Paths.get("/one/two/three/1001")
    }
  }

  "files" should {
    "fetch all the files in sorted order" in {
      val dir = createRandomIntDirectory
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
          IOEffect.createFile(path).runIO
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

      IOEffect.files(dir, Extension.Log) shouldBe expect
    }
  }

  "folders" should {
    "fetch all the folders in sorted order" in {
      val dir = createRandomIntDirectory
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
          IOEffect.createDirectoryIfAbsent(path)
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

      IOEffect.folders(dir) shouldBe expect
    }
  }

  "segmentFilesOnDisk" should {
    "fetch all segment files in order" in {
      val dir1 = createRandomIntDirectory
      val dir2 = createRandomIntDirectory
      val dir3 = createRandomIntDirectory
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
              IOEffect.createFileIfAbsent(path)
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

      IOEffect.segmentFilesOnDisk(dirs) shouldBe expect
    }
  }

  "benchmark" in {
    val fileSize = 4.mb
    val flattenBytes = randomBytesSlice(fileSize)
    val groupBytes = flattenBytes.groupedSlice(8)

    //20.mb
    //0.067924621 seconds
    //4.mb
    //0.057647201 seconds & 0.047565694 seconds
    val groupedPath = Benchmark("groupBytes")(IOEffect.write(randomFilePath, groupBytes)).get
    IOEffect.readAll(groupedPath).get shouldBe flattenBytes

    //20.mb
    //0.077162871 seconds
    //4.mb
    //0.05330862 seconds & 0.045989919 seconds
    val flattenedPath = Benchmark("flattenBytes")(IOEffect.write(randomFilePath, flattenBytes)).get
    IOEffect.readAll(flattenedPath).get shouldBe flattenBytes
  }
}
