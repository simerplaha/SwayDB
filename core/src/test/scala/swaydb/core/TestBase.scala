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

package swaydb.core

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import swaydb.core.TestLimitQueues._
import swaydb.core.actor.TestActor
import swaydb.core.data.{KeyValue, Memory, Persistent}
import swaydb.core.io.file.{DBFile, IO}
import swaydb.core.io.reader.FileReader
import swaydb.core.level.actor.LevelCommand.{PushSegments, PushSegmentsResponse}
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{Level, LevelRef}
import swaydb.core.map.{MapEntry, PersistentMap}
import swaydb.core.segment.Segment
import swaydb.core.util.IDGenerator
import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config.{Dir, RecoveryMode}
import swaydb.data.segment.MaxKey
import swaydb.data.slice.Slice
import swaydb.data.storage.{AppendixStorage, Level0Storage, LevelStorage}
import swaydb.data.util.StorageUnits._
import swaydb.core.util.FileUtil._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Random, Success, Try}

trait TestBase extends WordSpec with CommonAssertions with TestData with BeforeAndAfterAll with Eventually {

  implicit val idGenerator = IDGenerator(0)

  private val currentLevelId = new AtomicInteger(0)

  private def nextLevelId = currentLevelId.incrementAndGet()

  val testFileDirectory = Paths.get(getClass.getClassLoader.getResource("").getPath).getParent.getParent.resolve("TEST_FILES")

  val testMemoryFileDirectory = Paths.get(getClass.getClassLoader.getResource("").getPath).getParent.getParent.resolve("TEST_MEMORY_FILES")

  //default setting, these can be overridden to apply different settings for test cases.

  val levelZeroReadRetryLimit = 1000

  def segmentSize: Long = 2.mb

  def mapSize: Long = 4.mb


  def levelFoldersCount = 0

  def mmapSegmentsOnWrite = true

  def mmapSegmentsOnRead = true

  def level0MMAP = true

  def appendixStorageMMAP = true

  def inMemoryStorage = false

  def randomNextInt(max: Int) =
    Math.abs(Random.nextInt(max))

  def levelStorage: LevelStorage =
    if (inMemoryStorage)
      LevelStorage.Memory(dir = memoryTestDir.resolve(nextLevelId.toString))
    else
      LevelStorage.Persistent(
        mmapSegmentsOnWrite = mmapSegmentsOnWrite,
        mmapSegmentsOnRead = mmapSegmentsOnRead,
        dir = testDir.resolve(nextLevelId.toString),
        otherDirs =
          (0 until levelFoldersCount) map {
            _ =>
              Dir(testDir.resolve(nextLevelId.toString), 1)
          }
      )

  def level0Storage: Level0Storage =
    if (inMemoryStorage)
      Level0Storage.Memory
    else
      Level0Storage.Persistent(mmap = level0MMAP, randomIntDirectory, RecoveryMode.ReportFailure)

  def appendixStorage: AppendixStorage =
    if (inMemoryStorage)
      AppendixStorage.Memory
    else
      AppendixStorage.Persistent(mmap = appendixStorageMMAP, 4.mb)

  def persistent = levelStorage.persistent

  def memory = levelStorage.memory

  def randomDir = testDir.resolve(s"${randomCharacters()}")

  def createRandomDir = Files.createDirectory(randomDir)

  def randomFilePath = testDir.resolve(s"${randomCharacters()}.test")

  def nextSegmentId = idGenerator.nextSegmentID

  def nextId = idGenerator.nextID

  def deleteFiles = true

  def randomIntDirectory: Path =
    testDir.resolve(randomInt().toString)

  def createRandomIntDirectory: Path =
    if (persistent)
      IO.createDirectoriesIfAbsent(randomIntDirectory)
    else
      randomIntDirectory

  def createNextLevelPath: Path =
    Files.createDirectory(nextLevelPath)

  def nextLevelPath: Path =
    testDir.resolve(nextLevelId.toString)

  def testSegmentFile: Path =
    if (memory)
      randomIntDirectory.resolve(nextSegmentId)
    else
      IO.createDirectoriesIfAbsent(randomIntDirectory).resolve(nextSegmentId)

  def testMapFile: Path =
    if (memory)
      randomIntDirectory.resolve(nextId.toString + ".map")
    else
      IO.createDirectoriesIfAbsent(randomIntDirectory).resolve(nextId.toString + ".map")

  def farOut = new Exception("Far out! Something went wrong")

  def testDir = {
    val testDirPath = testFileDirectory.resolve(this.getClass.getSimpleName)
    if (inMemoryStorage)
      testDirPath
    else
      IO.createDirectoriesIfAbsent(testDirPath)
  }

  def memoryTestDir =
    testFileDirectory.resolve(this.getClass.getSimpleName + "_MEMORY_DIR")

  def walkDeleteFolder(folder: Path): Unit =
    if (deleteFiles && persistent)
      Files.walkFileTree(folder, new SimpleFileVisitor[Path]() {
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

  //
  //  sys.addShutdownHook {
  //    walkDeleteFolder(testDir)
  //  }

  override protected def afterAll(): Unit = {
    walkDeleteFolder(testDir)
  }

  implicit class RunThisImplicits[T, R](f: => R) {
    def runThis(times: Int): Unit =
      for (i <- 1 to times) f
  }

  implicit class FutureImplicits[T, R](f: => Future[T]) {
    def runThis(times: Int): Future[Seq[T]] = {
      println(s"runThis $times times")
      val futures =
        Range.inclusive(1, times).map {
          _ =>
            f
        }
      Future.sequence(futures)
    }
  }

  implicit class ReopenSegment(segment: Segment)(implicit ordering: Ordering[Slice[Byte]],
                                                 keyValueLimiter: (Persistent, Segment) => Unit = keyValueLimiter,
                                                 fileOpenLimited: DBFile => Unit = fileOpenLimiter) {

    def tryReopen: Try[Segment] =
      tryReopen(segment.path)

    def tryReopen(path: Path): Try[Segment] =
      Segment(
        path = path,
        mmapReads = Random.nextBoolean(),
        mmapWrites = Random.nextBoolean(),
        minKey = segment.minKey,
        maxKey = segment.maxKey,
        segmentSize = segment.segmentSize,
        removeDeletes = segment.removeDeletes
      ) flatMap {
        reopenedSegment =>
          segment.close map {
            _ =>
              reopenedSegment
          }
      }

    def reopen: Segment =
      tryReopen.assertGet

    def reopen(path: Path): Segment =
      tryReopen(path).assertGet
  }

  implicit class ReopenLevel(level: Level)(implicit ordering: Ordering[Slice[Byte]]) {

    def reopen: Level =
      reopen()

    def tryReopen: Try[Level] =
      tryReopen()

    def reopen(segmentSize: Long = level.segmentSize)(implicit keyValueLimiter: (Persistent, Segment) => Unit = keyValueLimiter,
                                                      fileOpenLimited: DBFile => Unit = fileOpenLimiter): Level =
      tryReopen(segmentSize).assertGet

    def tryReopen(segmentSize: Long = level.segmentSize)(implicit keyValueLimiter: (Persistent, Segment) => Unit = keyValueLimiter,
                                                         fileOpenLimited: DBFile => Unit = fileOpenLimiter): Try[Level] = {
      level.releaseLocks flatMap {
        _ =>
          level.close flatMap {
            _ =>
              Level(
                levelStorage = LevelStorage.Persistent(
                  mmapSegmentsOnWrite = level.mmapSegmentsOnWrite,
                  mmapSegmentsOnRead = level.mmapSegmentsOnRead,
                  dir = level.paths.headPath,
                  otherDirs = level.dirs.drop(1).map(dir => Dir(dir.path, 1))
                ),
                appendixStorage = AppendixStorage.Persistent(mmap = true, 4.mb),
                segmentSize = segmentSize,
                nextLevel = level.nextLevel,
                pushForward = level.pushForward,
                bloomFilterFalsePositiveRate = 0.1,
                throttle = level.throttle
              ).map(_.asInstanceOf[Level])
          }
      }
    }
  }

  implicit class ReopenLevelZero(level: LevelZero)(implicit ordering: Ordering[Slice[Byte]]) {

    def reopen: LevelZero =
      reopen()

    def reopen(mapSize: Long = mapSize)(implicit keyValueLimiter: (Persistent, Segment) => Unit = keyValueLimiter,
                                        fileOpenLimited: DBFile => Unit = fileOpenLimiter): LevelZero = {
      val reopened =
        level.releaseLocks flatMap {
          _ =>
            level.close flatMap {
              _ =>
                LevelZero(
                  mapSize = mapSize,
                  storage = Level0Storage.Persistent(true, level.path.getParent, RecoveryMode.ReportFailure),
                  nextLevel = level.nextLevel,
                  acceleration = Accelerator.brake(),
                  readRetryLimit = levelZeroReadRetryLimit
                )
            }
        }
      reopened.assertGet
    }
  }

  val once = 1

  implicit class TimesImplicits(int: Int) {
    def times = int

    def time = int
  }

  implicit class KeyValuesImplicits(keyValues: Iterable[KeyValue.WriteOnly]) {
    def updateStats: Slice[KeyValue.WriteOnly] = {
      val slice = Slice.create[KeyValue.WriteOnly](keyValues.size)
      keyValues foreach {
        keyValue =>
          slice.add(keyValue.updateStats(0.1, keyValue = slice.lastOption))
      }
      slice
    }

    def maxKey: MaxKey =
      keyValues.last match {
        case range: KeyValue.WriteOnly.Range =>
          MaxKey.Range(range.fromKey, range.toKey)
        case last =>
          MaxKey.Fixed(last.key)
      }

    def minKey: Slice[Byte] =
      keyValues.head.key
  }

  def createFile(bytes: Slice[Byte]): Path =
    IO.write(bytes, testDir.resolve(nextSegmentId)).assertGet

  def createFileReader(path: Path): FileReader =
    new FileReader(
      if (Random.nextBoolean())
        DBFile.channelRead(path, fileOpenLimiter).assertGet
      else
        DBFile.mmapRead(path, fileOpenLimiter).assertGet
    )

  def createFileChannelReader(bytes: Slice[Byte]): FileReader =
    createFileReader(createFile(bytes))

  object TestMap {
    def apply(keyValues: Slice[Memory],
              fileSize: Int = 4.mb,
              path: Path = testMapFile,
              flushOnOverflow: Boolean = false,
              mmap: Boolean = true)(implicit ordering: Ordering[Slice[Byte]],
                                    keyValueLimiter: (Persistent, Segment) => Unit = keyValueLimiter,
                                    fileOpenLimited: DBFile => Unit = fileOpenLimiter): map.Map[Slice[Byte], Memory] = {
      import swaydb.core.map.serializer.LevelZeroMapEntryWriter._
      import swaydb.core.map.serializer.LevelZeroMapEntryReader._
      implicit val merger = swaydb.core.level.zero.LevelZeroSkipListMerge

      val testMap =
        if (levelStorage.memory)
          map.Map.memory[Slice[Byte], Memory](
            fileSize = fileSize,
            flushOnOverflow = flushOnOverflow
          )
        else
          map.Map.persistent[Slice[Byte], Memory](
            folder = path,
            mmap = mmap,
            flushOnOverflow = flushOnOverflow,
            fileSize = fileSize
          ).assertGet

      keyValues foreach {
        keyValue =>
          testMap.write(MapEntry.Put(keyValue.key, keyValue))
      }
      testMap
    }
  }

  object TestSegment {
    def apply(keyValues: Slice[KeyValue.WriteOnly] = randomIntKeyStringValues(),
              removeDeletes: Boolean = false,
              path: Path = testSegmentFile,
              bloomFilterFalsePositiveRate: Double = 0.1)(implicit ordering: Ordering[Slice[Byte]],
                                                          keyValueLimiter: (Persistent, Segment) => Unit = keyValueLimiter,
                                                          fileOpenLimited: DBFile => Unit = fileOpenLimiter): Try[Segment] =
      if (levelStorage.memory)
        Segment.memory(
          path = path,
          keyValues = keyValues,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          removeDeletes = removeDeletes
        )
      else
        Segment.persistent(
          path = path,
          mmapReads = levelStorage.mmapSegmentsOnRead,
          mmapWrites = levelStorage.mmapSegmentsOnWrite,
          keyValues = keyValues,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          removeDeletes = removeDeletes
        )
  }

  object TestLevel {

    implicit class TestLevelImplicit(level: Level) {
      def addSegments(segments: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): Level = {
        val replyTo = TestActor[PushSegmentsResponse]()
        level ! PushSegments(segments, replyTo)
        replyTo.getMessage(5.seconds).result.assertGet
        //        level.segmentsCount() shouldBe segments.size
        level
      }
    }

    def testDefaultThrottle(meter: LevelMeter): Throttle =
      if (meter.segmentsCount > 15)
        Throttle(Duration.Zero, 20)
      else if (meter.segmentsCount > 10)
        Throttle(1.second, 20)
      else if (meter.segmentsCount > 5)
        Throttle(2.seconds, 10)
      else
        Throttle(3.seconds, 10)

    implicit def toSome[T](input: T): Option[T] =
      Some(input)

    def apply(levelStorage: LevelStorage = levelStorage,
              appendixStorage: AppendixStorage = appendixStorage,
              segmentSize: Long = segmentSize,
              nextLevel: Option[LevelRef] = None,
              pushForward: Boolean = false,
              throttle: LevelMeter => Throttle = testDefaultThrottle,
              bloomFilterFalsePositiveRate: Double = 0.01)(implicit ordering: Ordering[Slice[Byte]],
                                                           keyValueLimiter: (Persistent, Segment) => Unit = keyValueLimiter,
                                                           fileOpenLimited: DBFile => Unit = fileOpenLimiter): Level =
      Level(
        levelStorage = levelStorage,
        segmentSize = segmentSize,
        nextLevel = nextLevel,
        pushForward = pushForward,
        appendixStorage = appendixStorage,
        throttle = throttle,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate
      ).assertGet.asInstanceOf[Level]
  }

  object TestLevelZero {

    def apply(nextLevel: LevelRef,
              mapSize: Long = mapSize,
              brake: Level0Meter => Accelerator = Accelerator.brake(),
              readRetryLimit: Int = levelZeroReadRetryLimit)(implicit ordering: Ordering[Slice[Byte]],
                                                             keyValueLimiter: (Persistent, Segment) => Unit = keyValueLimiter,
                                                             fileOpenLimited: DBFile => Unit = fileOpenLimiter): LevelZero =
      LevelZero(
        mapSize = mapSize,
        storage = level0Storage,
        nextLevel = nextLevel,
        acceleration = brake,
        readRetryLimit = readRetryLimit
      ).assertGet
  }

  def assertOnLevel[T](keyValues: Slice[Memory],
                       assertion: Level => T)(implicit ordering: Ordering[Slice[Byte]]) = {
    val level = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
    level.putKeyValues(keyValues).assertGet

    assertion(level)
    assertion(level)

    if (persistent) {
      val levelReopened = level.reopen //reopen
      assertion(levelReopened)
      assertion(levelReopened)
      levelReopened.close.assertGet
    }
  }

  def assertOnLevel[T](keyValues: Slice[Memory],
                       assertionWithKeyValues: (Slice[Memory], Level) => T)(implicit ordering: Ordering[Slice[Byte]]) = {
    val level = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
    level.putKeyValues(keyValues).assertGet

    assertionWithKeyValues(keyValues, level)
    assertionWithKeyValues(keyValues, level)

    if (persistent) {
      val levelReopened = level.reopen //reopen
      assertionWithKeyValues(keyValues, levelReopened)
      assertionWithKeyValues(keyValues, levelReopened)
      levelReopened.close.assertGet
    }
  }

  def assertOnLevel[T](upperLevelKeyValues: Slice[Memory],
                       lowerLevelKeyValues: Slice[Memory],
                       assertion: Level => T)(implicit ordering: Ordering[Slice[Byte]]) = {
    val lowerLevel = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
    val level = TestLevel(nextLevel = Some(lowerLevel), throttle = (_) => Throttle(Duration.Zero, 0))

    if (lowerLevelKeyValues.nonEmpty)
      lowerLevel.putKeyValues(lowerLevelKeyValues).assertGet

    if (upperLevelKeyValues.nonEmpty)
      level.putKeyValues(upperLevelKeyValues).assertGet

    assertion(level)
    assertion(level)

    if (persistent) {
      val levelReopened = level.reopen //reopen
      assertion(levelReopened)
      assertion(levelReopened)
      levelReopened.close.assertGet
    }
  }

  def assertOnSegment[T](keyValues: Iterable[Memory],
                         assertion: Segment => T)(implicit ordering: Ordering[Slice[Byte]]) = {
    val segment = TestSegment(keyValues.toTransient).assertGet

    assertion(segment) //first
    assertion(segment) //with cache populated
    if (persistent) {
      segment.clearCache()
      assertion(segment) //same Segment but test with cleared cache.

      val segmentReopened = segment.reopen //test with reopen Segment
      assertion(segmentReopened)
      assertion(segmentReopened)
      segmentReopened.close.assertGet
    } else {
      segment.close.assertGet
    }
  }

  def assertOnSegment[T](keyValues: Slice[Memory],
                         assertionWithKeyValues: (Slice[Memory], Segment) => T)(implicit ordering: Ordering[Slice[Byte]]) = {
    val segment = TestSegment(keyValues.toTransient).assertGet

    assertionWithKeyValues(keyValues, segment)
    assertionWithKeyValues(keyValues, segment)

    if (persistent) {
      val segmentReopened = segment.reopen //reopen
      assertionWithKeyValues(keyValues, segmentReopened)
      assertionWithKeyValues(keyValues, segmentReopened)
      segmentReopened.close.assertGet
    } else {
      segment.close.assertGet
    }
  }
}
