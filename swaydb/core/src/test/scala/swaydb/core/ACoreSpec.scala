///*
// * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package swaydb.core
//
//import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
//import org.scalatest.concurrent.Eventually
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AnyWordSpec
//import swaydb.config.MMAP
//import swaydb.core.TestSweeper._
//import swaydb.effect.Effect
//import swaydb.utils.OperatingSystem
//
//import java.nio.file._
//import java.util.concurrent.atomic.AtomicInteger
//
//trait ACoreSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with Eventually {
//
////  implicit val idGenerator = IDGenerator()
//
//  val reverseIdGenerator = new AtomicInteger(100000000)
//
//  def nextIntReversed = reverseIdGenerator.decrementAndGet()
//
//  private val projectTargetFolder = getClass.getClassLoader.getResource("").getPath
//
//  val projectDirectory =
//    if (OperatingSystem.isWindows())
//      Paths.get(projectTargetFolder.drop(1)).getParent.getParent
//    else
//      Paths.get(projectTargetFolder).getParent.getParent
//
//  val testFileDirectory = projectDirectory.resolve("TEST_FILES")
//
//  val testMemoryFileDirectory = projectDirectory.resolve("TEST_MEMORY_FILES")
//
//  val testClassDirPath = testFileDirectory.resolve(this.getClass.getSimpleName)
//
//  def appendixStorageMMAP: MMAP.Log = MMAP.On(OperatingSystem.isWindows(), TestForceSave.mmap())
//
//  def isMemorySpec = false
//
//  def isPersistentSpec = !isMemorySpec
//
//  def deleteFiles = true
//
//  def randomIntDirectory: Path =
//    testClassDir.resolve(nextIntReversed.toString)
//
//  def createRandomIntDirectory(implicit sweeper: TestSweeper): Path =
//    if (isPersistentSpec)
//      Effect.createDirectoriesIfAbsent(randomIntDirectory).sweep()
//    else
//      randomIntDirectory
//
//  def testClassDir: Path =
//    if (isMemorySpec)
//      testClassDirPath
//    else
//      Effect.createDirectoriesIfAbsent(testClassDirPath)
//
//  def memoryTestClassDir =
//    testFileDirectory.resolve(this.getClass.getSimpleName + "_MEMORY_DIR")
//
//  override protected def beforeAll(): Unit =
//    if (deleteFiles)
//      Effect.walkDelete(testClassDirPath)
//
//  override protected def afterEach(): Unit =
//    Effect.deleteIfExists(testClassDirPath)
//
//}
