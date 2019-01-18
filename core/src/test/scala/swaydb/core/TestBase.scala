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
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import scala.concurrent.duration._
import scala.util.{Random, Try}
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.TestLimitQueues._
import swaydb.core.actor.TestActor
import swaydb.core.data.{KeyValue, Memory, Time}
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.io.file.{DBFile, IO}
import swaydb.core.io.reader.FileReader
import swaydb.core.level.actor.LevelCommand.{PushSegments, PushSegmentsResponse}
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{Level, LevelRef}
import swaydb.core.map.MapEntry
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.Segment
import swaydb.core.util.IDGenerator
import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config.{Dir, RecoveryMode}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.storage.{AppendixStorage, Level0Storage, LevelStorage}
import swaydb.data.util.StorageUnits._
import swaydb.core.TryAssert._

trait TestBase extends WordSpec with Matchers with BeforeAndAfterAll with Eventually {

  implicit val idGenerator = IDGenerator()

  private val currentLevelId = new AtomicInteger(100)

  private def nextLevelId = currentLevelId.decrementAndGet()

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

  def nextTime(implicit timeGenerator: TestTimeGenerator): Time =
    timeGenerator.nextTime

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
    testDir.resolve(nextLevelId.toString)

  def createRandomIntDirectory: Path =
    if (persistent)
      IO.createDirectoriesIfAbsent(randomIntDirectory)
    else
      randomIntDirectory

  def createNextLevelPath: Path =
    IO.createDirectoriesIfAbsent(nextLevelPath)

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
    if (deleteFiles && IO.exists(folder))
      Files.walkFileTree(folder, new SimpleFileVisitor[Path]() {
        @throws[IOException]
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          IO.deleteIfExists(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          if (exc != null) throw exc
          IO.deleteIfExists(dir)
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

  object TestMap {
    def apply(keyValues: Slice[Memory.SegmentResponse],
              fileSize: Int = 4.mb,
              path: Path = testMapFile,
              flushOnOverflow: Boolean = false,
              mmap: Boolean = true)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                    keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter,
                                    fileOpenLimiter: DBFile => Unit = TestLimitQueues.fileOpenLimiter,
                                    timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long): map.Map[Slice[Byte], Memory.SegmentResponse] = {
      import swaydb.core.map.serializer.LevelZeroMapEntryReader._
      import swaydb.core.map.serializer.LevelZeroMapEntryWriter._
      implicit val merger = swaydb.core.level.zero.LevelZeroSkipListMerger

      val testMap =
        if (levelStorage.memory)
          map.Map.memory[Slice[Byte], Memory.SegmentResponse](
            fileSize = fileSize,
            flushOnOverflow = flushOnOverflow
          )
        else
          map.Map.persistent[Slice[Byte], Memory.SegmentResponse](
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
    def apply(keyValues: Slice[KeyValue.WriteOnly] = randomizedKeyValues()(TestTimeGenerator.Incremental(), KeyOrder.default, keyValueLimiter),
              removeDeletes: Boolean = false,
              path: Path = testSegmentFile,
              bloomFilterFalsePositiveRate: Double = TestData.falsePositiveRate)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                 keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter,
                                                                                 fileOpenLimiter: DBFile => Unit = TestLimitQueues.fileOpenLimiter,
                                                                                 timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                                                 groupingStrategy: Option[KeyValueGroupingStrategyInternal] = randomGroupingStrategyOption(randomIntMax(1000))): Try[Segment] =
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
      def addSegments(segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Level = {
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
              bloomFilterFalsePositiveRate: Double = 0.01,
              compressDuplicateValues: Boolean = true)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                       keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter,
                                                       fileOpenLimiter: DBFile => Unit = TestLimitQueues.fileOpenLimiter,
                                                       timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                       compression: Option[KeyValueGroupingStrategyInternal] = randomGroupingStrategy(randomNextInt(1000))): Level =
      Level(
        levelStorage = levelStorage,
        segmentSize = segmentSize,
        nextLevel = nextLevel,
        pushForward = pushForward,
        appendixStorage = appendixStorage,
        throttle = throttle,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        compressDuplicateValues = compressDuplicateValues
      ).assertGet.asInstanceOf[Level]
  }

  object TestLevelZero {

    def apply(nextLevel: Option[LevelRef],
              mapSize: Long = mapSize,
              brake: Level0Meter => Accelerator = Accelerator.brake(),
              readRetryLimit: Int = levelZeroReadRetryLimit,
              throttleOn: Boolean = true)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                          keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter,
                                          timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                          fileOpenLimiter: DBFile => Unit = TestLimitQueues.fileOpenLimiter): LevelZero =
      LevelZero(
        mapSize = mapSize,
        storage = level0Storage,
        nextLevel = nextLevel,
        throttleOn = throttleOn,
        acceleration = brake,
        readRetryLimit = readRetryLimit,
      ).assertGet
  }

  implicit class KeyValuesImplicits(keyValues: Iterable[KeyValue.WriteOnly]) {
    def updateStats: Slice[KeyValue.WriteOnly] = {
      val slice = Slice.create[KeyValue.WriteOnly](keyValues.size)
      keyValues foreach {
        keyValue =>
          slice.add(keyValue.updateStats(TestData.falsePositiveRate, previous = slice.lastOption))
      }
      slice
    }
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

  /**
    * Runs multiple asserts on individual levels and also one by one merges key-values from upper levels
    * to lower levels and asserts the results are still the same.
    *
    * The tests written only need to define a 3 level test case and this function will create a 4 level database
    * and run multiple passes for the test merging key-values from levels into lower levels asserting the results
    * are the same after merge.
    */
  def assertOnLevel(level0KeyValues: (Slice[Memory], Slice[Memory], TestTimeGenerator) => Slice[Memory] = (_, _, _) => Slice.empty,
                    assertLevel0: (Slice[Memory], Slice[Memory], Slice[Memory], LevelRef) => Unit = (_, _, _, _) => (),
                    level1KeyValues: (Slice[Memory], TestTimeGenerator) => Slice[Memory] = (_, _) => Slice.empty,
                    assertLevel1: (Slice[Memory], Slice[Memory], LevelRef) => Unit = (_, _, _) => (),
                    level2KeyValues: TestTimeGenerator => Slice[Memory] = _ => Slice.empty,
                    assertLevel2: (Slice[Memory], LevelRef) => Unit = (_, _) => (),
                    assertAllLevels: (Slice[Memory], Slice[Memory], Slice[Memory], LevelRef) => Unit = (_, _, _, _) => (),
                    throttleOn: Boolean = false,
                    incrementalTime: Boolean = true)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                     groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Unit = {

    def iterationMessage =
      s"Thread: ${Thread.currentThread().getId} - (throttleOn, incrementalTime): ($throttleOn, $incrementalTime)"

    println(iterationMessage)

    val noAssert =
      (_: LevelRef) => ()

    val timeGenerator: TestTimeGenerator =
      if (incrementalTime)
        TestTimeGenerator.Incremental()
      else
        TestTimeGenerator.Decremental()

    /**
      * If [[throttleOn]] is true then enable fast throttling
      * so that this test covers as many scenarios as possible.
      */
    val throttle: LevelMeter => Throttle = if (throttleOn) _ => Throttle(Duration.Zero, randomNextInt(3) max 1) else _ => Throttle(Duration.Zero, 0)

    val level4 = TestLevel(throttle = throttle)
    val level3 = TestLevel(nextLevel = Some(level4), throttle = throttle)
    val level2 = TestLevel(nextLevel = Some(level3), throttle = throttle)
    val level1 = TestLevel(nextLevel = Some(level2), throttle = throttle)
    val level0 = TestLevelZero(nextLevel = Some(level1), throttleOn = throttleOn)

    //start with a default timeGenerator.
    val level2KV = level2KeyValues(timeGenerator)

    //if upper levels should insert key-values at an older time start the timeGenerator to use older time
    val level1KV = level1KeyValues(level2KV, timeGenerator)

    //if upper levels should insert key-values at an older time start the timeGenerator to use older time
    val level0KV = level0KeyValues(level1KV, level2KV, timeGenerator)

    val level0Assert: LevelRef => Unit = assertLevel0(level0KV, level1KV, level2KV, _)
    val level1Assert: LevelRef => Unit = assertLevel1(level1KV, level2KV, _)
    val level2Assert: LevelRef => Unit = assertLevel2(level0KV, _)
    val levelAllAssert: LevelRef => Unit = assertAllLevels(level0KV, level1KV, level2KV, _)

    def runAsserts(asserts: Seq[((Slice[Memory], LevelRef => Unit), (Slice[Memory], LevelRef => Unit), (Slice[Memory], LevelRef => Unit), (Slice[Memory], LevelRef => Unit))]) =
      asserts.foldLeft(1) {
        case (count, ((level0KeyValues, level0Assert), (level1KeyValues, level1Assert), (level2KeyValues, level2Assert), (level3KeyValues, level3Assert))) => {
          println(s"\nRunning assert: $count/${asserts.size} - $iterationMessage")
          doAssertOnLevel(
            level0KeyValues = level0KeyValues,
            assertLevel0 = level0Assert,
            level0 = level0,
            level1KeyValues = level1KeyValues,
            assertLevel1 = level1Assert,
            level1 = level1,
            level2KeyValues = level2KeyValues,
            assertLevel2 = level2Assert,
            level2 = level2,
            level3KeyValues = level3KeyValues,
            assertLevel3 = level3Assert,
            level3 = level3,
            assertAllLevels = levelAllAssert,
            //if level3's key-values are empty - no need to run assert for this pass.
            assertLevel3ForAllLevels = level3KeyValues.nonEmpty
          )
          count + 1
        }
      }

    val incrementalAsserts =
      Seq(
        (
          (level0KV, level0Assert),
          (level1KV, level1Assert),
          (level2KV, level2Assert),
          (Slice.empty, noAssert),
        ),
        (
          (level0KV, level0Assert),
          (level1KV, level1Assert),
          (Slice.empty, level2Assert),
          (level2KV, level2Assert),
        ),
        (
          (level0KV, level0Assert),
          (Slice.empty, level1Assert),
          (level1KV, level1Assert),
          (level2KV, level2Assert),
        ),
        (
          (Slice.empty, level0Assert),
          (level0KV, level0Assert),
          (level1KV, level1Assert),
          (level2KV, level2Assert),
        ),
        (
          (Slice.empty, level0Assert),
          (Slice.empty, level0Assert),
          (level0KV, level0Assert),
          (level1KV, level1Assert)
        ),
        (
          (Slice.empty, level0Assert),
          (Slice.empty, level0Assert),
          (Slice.empty, level0Assert),
          (level0KV, level0Assert)
        )
      )

    //If key-values at upper Levels are in decremental order then
    // the only the last nonempty level's assert is valid since all the upper
    //key-values being of older time are be ignored during merge.
    val (lastNonEmptyLevel, lastValidAssertForDecrementalTimes) =
    if (level2KV.nonEmpty)
      (2, level2Assert)
    else if (level1KV.nonEmpty)
      (1, level1Assert)
    else if (level0KV.nonEmpty)
      (0, level0Assert)
    else
      (2, levelAllAssert) //this is still a valid assert for all Levels.

    val decrementalAsserts =
      Seq(
        (
          (level0KV, if (0 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
          (level1KV, if (1 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
          (level2KV, if (2 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
          (Slice.empty, noAssert)
        ),
        (
          (level0KV, if (0 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
          (level1KV, if (1 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
          (Slice.empty, if (2 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
          (level2KV, if (2 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
        ),
        (
          (level0KV, if (0 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
          (Slice.empty, if (1 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
          (level1KV, if (1 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
          (level2KV, if (2 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
        ),
        (
          (Slice.empty, if (0 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
          (level0KV, if (0 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
          (level1KV, if (1 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
          (level2KV, if (2 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
        ),
        (
          (Slice.empty, if (0 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
          (Slice.empty, if (0 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
          (level0KV, if (0 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert),
          (level1KV, if (1 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert)
        ),
        (
          (Slice.empty, noAssert),
          (Slice.empty, noAssert),
          (Slice.empty, noAssert),
          (level0KV, if (0 <= lastNonEmptyLevel) lastValidAssertForDecrementalTimes else noAssert)
        )
      )

    if (incrementalTime)
      runAsserts(incrementalAsserts)
    else
      runAsserts(decrementalAsserts)

    level0.close.assertGet

    if (!throttleOn)
      assertOnLevel(
        level0KeyValues = level0KeyValues,
        assertLevel0 = assertLevel0,
        level1KeyValues = level1KeyValues,
        assertLevel1 = assertLevel1,
        level2KeyValues = level2KeyValues,
        assertLevel2 = assertLevel2,
        assertAllLevels = assertAllLevels,
        throttleOn = true,
        incrementalTime = incrementalTime
      )

    if (incrementalTime)
      assertOnLevel(
        level0KeyValues = level0KeyValues,
        assertLevel0 = assertLevel0,
        level1KeyValues = level1KeyValues,
        assertLevel1 = assertLevel1,
        level2KeyValues = level2KeyValues,
        assertLevel2 = assertLevel2,
        assertAllLevels = assertAllLevels,
        throttleOn = false,
        incrementalTime = false
      )
  }

  private def doAssertOnLevel(level0KeyValues: Slice[Memory],
                              assertLevel0: LevelRef => Unit,
                              level0: LevelZero,
                              level1KeyValues: Slice[Memory],
                              assertLevel1: LevelRef => Unit,
                              level1: Level,
                              level2KeyValues: Slice[Memory],
                              assertLevel2: LevelRef => Unit,
                              level2: Level,
                              level3KeyValues: Slice[Memory],
                              assertLevel3: LevelRef => Unit,
                              level3: Level,
                              assertAllLevels: LevelRef => Unit,
                              assertLevel3ForAllLevels: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                 groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Unit = {
    if (level3KeyValues.nonEmpty) level3.putKeyValues(level3KeyValues).assertGet
    if (level2KeyValues.nonEmpty) level2.putKeyValues(level2KeyValues).assertGet
    if (level1KeyValues.nonEmpty) level1.putKeyValues(level1KeyValues).assertGet
    if (level0KeyValues.nonEmpty) level0.putKeyValues(level0KeyValues).assertGet

    println("asserting Level3")
    assertLevel3(level3)
    println("asserting Level2")
    assertLevel2(level2)
    println("asserting Level1")
    assertLevel1(level1)
    println("asserting Level0")
    assertLevel0(level0)
    if (assertLevel3ForAllLevels) {
      println("asserting all on Level3")
      assertAllLevels(level3)
    }
    println("asserting all on Level2")
    assertAllLevels(level2)
    println("asserting all on Level1")
    assertAllLevels(level1)
    println("asserting all on Level0")
    assertAllLevels(level0)
  }

  def assertOnSegment[T](keyValues: Slice[Memory],
                         assert: (Slice[Memory], Segment) => T,
                         testWithCachePopulated: Boolean = true,
                         closeAfterCreate: Boolean = false)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                            groupingStrategy: Option[KeyValueGroupingStrategyInternal]) = {
    val segment = TestSegment(keyValues.toTransient).assertGet
    if (closeAfterCreate) segment.close.assertGet

    assert(keyValues, segment) //first
    if (testWithCachePopulated) assert(keyValues, segment) //with cache populated

    if (persistent) {
      segment.clearCache()
      assert(keyValues, segment) //same Segment but test with cleared cache.

      val segmentReopened = segment.reopen //reopen
      assert(keyValues, segmentReopened)
      if (testWithCachePopulated) assert(keyValues, segmentReopened)
      segmentReopened.close.assertGet
    } else {
      segment.close.assertGet
    }
  }

}
