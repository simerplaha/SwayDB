/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.benchmark

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import scala.io.StdIn._
import scala.util.Try

object Start {

  def main(args: Array[String]): Unit = {
    println(
      """***************************************************************
        |********************** SwayDB speed test **********************
        |***************************************************************
        |1. Memory database
        |2. Persistent memory-mapped database
        |3. Persistent memory-mapped disabled database (FileChannel)
        |""".stripMargin)

    print("Select database type (hit Enter for 1): ")
    val databaseType = Try(readInt()) getOrElse 1

    println(
      """
        |1. Sequential write & sequential read
        |2. Random write & random read
        |3. Sequential write & random read
        |4. Random write & sequential read
        |5. Forward iteration
        |6. Reverse iteration
        |""".stripMargin)

    print("Select test number (hit Enter for 1): ")
    val testNumber = Try(readInt()) getOrElse 1

    println(
      """
        |1. Map[Long, String]
        |2. Set[(Long, String)]
        |""".stripMargin)

    print("Select data type (hit Enter for 1): ")
    val dataTypeInt = Try(readInt()) getOrElse 1
    val map = if (dataTypeInt == 1) true else false
    println

    print("Enter test key-value count (hit Enter for 1 million): ")
    val keyValueCount = Try(readInt()) getOrElse 1000000

    println

    val dir = Paths.get(Start.getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath).getParent.resolve("speedDB")

    sys.addShutdownHook {
      //      deleteDir(dir)
    }

    val test: Test =
      if (databaseType == 1) {
        if (testNumber == 1)
          MemoryTest(keyValueCount = keyValueCount, randomWrite = false, randomRead = false, forwardIteration = false, reverseIteration = false, useMap = map)
        else if (testNumber == 2)
          MemoryTest(keyValueCount = keyValueCount, randomWrite = true, randomRead = true, forwardIteration = false, reverseIteration = false, useMap = map)
        else if (testNumber == 3)
          MemoryTest(keyValueCount = keyValueCount, randomWrite = false, randomRead = true, forwardIteration = false, reverseIteration = false, useMap = map)
        else if (testNumber == 4)
          MemoryTest(keyValueCount = keyValueCount, randomWrite = true, randomRead = false, forwardIteration = false, reverseIteration = false, useMap = map)
        else if (testNumber == 5)
          MemoryTest(keyValueCount = keyValueCount, randomWrite = false, randomRead = false, forwardIteration = true, reverseIteration = false, useMap = map)
        else if (testNumber == 6)
          MemoryTest(keyValueCount = keyValueCount, randomWrite = false, randomRead = false, forwardIteration = false, reverseIteration = true, useMap = map)
        else {
          throw new Exception(s"Invalid test number '$testNumber'.")
        }
      }

      else if (databaseType == 2) {
        if (testNumber == 1)
          PersistentTest(dir = dir, mmap = true, keyValueCount = keyValueCount, randomWrite = false, randomRead = false, forwardIteration = false, reverseIteration = false, useMap = map)
        else if (testNumber == 2)
          PersistentTest(dir = dir, mmap = true, keyValueCount = keyValueCount, randomWrite = true, randomRead = true, forwardIteration = false, reverseIteration = false, useMap = map)
        else if (testNumber == 3)
          PersistentTest(dir = dir, mmap = true, keyValueCount = keyValueCount, randomWrite = false, randomRead = true, forwardIteration = false, reverseIteration = false, useMap = map)
        else if (testNumber == 4)
          PersistentTest(dir = dir, mmap = true, keyValueCount = keyValueCount, randomWrite = true, randomRead = false, forwardIteration = false, reverseIteration = false, useMap = map)
        else if (testNumber == 5)
          PersistentTest(dir = dir, mmap = true, keyValueCount = keyValueCount, randomWrite = false, randomRead = false, forwardIteration = true, reverseIteration = false, useMap = map)
        else if (testNumber == 6)
          PersistentTest(dir = dir, mmap = true, keyValueCount = keyValueCount, randomWrite = false, randomRead = false, forwardIteration = false, reverseIteration = true, useMap = map)
        else
          throw new Exception(s"Invalid test number '$testNumber'.")
      }

      else if (databaseType == 3) {
        if (testNumber == 1)
          PersistentTest(dir = dir, mmap = false, keyValueCount = keyValueCount, randomWrite = false, randomRead = false, forwardIteration = false, reverseIteration = false, useMap = map)
        else if (testNumber == 2)
          PersistentTest(dir = dir, mmap = false, keyValueCount = keyValueCount, randomWrite = true, randomRead = true, forwardIteration = false, reverseIteration = false, useMap = map)
        else if (testNumber == 3)
          PersistentTest(dir = dir, mmap = false, keyValueCount = keyValueCount, randomWrite = false, randomRead = true, forwardIteration = false, reverseIteration = false, useMap = map)
        else if (testNumber == 4)
          PersistentTest(dir = dir, mmap = false, keyValueCount = keyValueCount, randomWrite = true, randomRead = false, forwardIteration = false, reverseIteration = false, useMap = map)
        else if (testNumber == 5)
          PersistentTest(dir = dir, mmap = false, keyValueCount = keyValueCount, randomWrite = false, randomRead = false, forwardIteration = true, reverseIteration = false, useMap = map)
        else if (testNumber == 6)
          PersistentTest(dir = dir, mmap = false, keyValueCount = keyValueCount, randomWrite = false, randomRead = false, forwardIteration = false, reverseIteration = true, useMap = map)
        else
          throw new Exception(s"Invalid test number '$testNumber'.")
      }
      else
        throw new Exception(s"Invalid database type '$databaseType'.")

    Runner(test).run
    deleteDir(dir)
  }

  def deleteDir(dir: Path): Unit =
    if (Files.exists(dir)) {
      println("Deleting test database files.")
      Files.walkFileTree(dir, new SimpleFileVisitor[Path]() {
        @throws[IOException]
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          if (exc != null) throw exc
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      })
    }
}