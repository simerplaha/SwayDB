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
import scala.util.Random
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.TestLimitQueues.{fileOpenLimiter, _}
import swaydb.core.actor.TestActor
import swaydb.core.data.{KeyValue, Memory, Time}
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.io.file.{DBFile, IOEffect}
import swaydb.core.io.reader.FileReader
import swaydb.core.level.actor.LevelCommand.{PushSegments, PushSegmentsResponse}
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{Level, LevelRef}
import swaydb.core.map.MapEntry
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.segment.Segment
import swaydb.core.util.IDGenerator
import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config.{Dir, RecoveryMode}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.storage.{AppendixStorage, Level0Storage, LevelStorage}
import swaydb.data.util.StorageUnits._
import swaydb.core.IOAssert._
import swaydb.data.IO

trait TestBase extends WordSpec with Matchers with BeforeAndAfterAll with Eventually {

  implicit val idGenerator = IDGenerator()

  private val currentLevelId = new AtomicInteger(100)

  private def nextLevelId = currentLevelId.decrementAndGet()

  val testFileDirectory = Paths.get(getClass.getClassLoader.getResource("").getPath).getParent.getParent.resolve("TEST_FILES")

  val testMemoryFileDirectory = Paths.get(getClass.getClassLoader.getResource("").getPath).getParent.getParent.resolve("TEST_MEMORY_FILES")

  //default setting, these can be overridden to apply different settings for test cases.

  def segmentSize: Long = 2.mb

  def mapSize: Long = 4.mb

  def levelFoldersCount = 0

  def mmapSegmentsOnWrite = true

  def mmapSegmentsOnRead = true

  def level0MMAP = true

  def appendixStorageMMAP = true

  def inMemoryStorage = false

  def nextTime(implicit testTimer: TestTimer): Time =
    testTimer.next

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
      IOEffect.createDirectoriesIfAbsent(randomIntDirectory)
    else
      randomIntDirectory

  def createNextLevelPath: Path =
    IOEffect.createDirectoriesIfAbsent(nextLevelPath)

  def nextLevelPath: Path =
    testDir.resolve(nextLevelId.toString)

  def testSegmentFile: Path =
    if (memory)
      randomIntDirectory.resolve(nextSegmentId)
    else
      IOEffect.createDirectoriesIfAbsent(randomIntDirectory).resolve(nextSegmentId)

  def testMapFile: Path =
    if (memory)
      randomIntDirectory.resolve(nextId.toString + ".map")
    else
      IOEffect.createDirectoriesIfAbsent(randomIntDirectory).resolve(nextId.toString + ".map")

  def farOut = new Exception("Far out! Something went wrong")

  def testDir = {
    val testDirPath = testFileDirectory.resolve(this.getClass.getSimpleName)
    if (inMemoryStorage)
      testDirPath
    else
      IOEffect.createDirectoriesIfAbsent(testDirPath)
  }

  def memoryTestDir =
    testFileDirectory.resolve(this.getClass.getSimpleName + "_MEMORY_DIR")

  def walkDeleteFolder(folder: Path): Unit =
    if (deleteFiles && IOEffect.exists(folder))
      Files.walkFileTree(folder, new SimpleFileVisitor[Path]() {
        @throws[IOException]
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          IOEffect.deleteIfExists(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          if (exc != null) throw exc
          IOEffect.deleteIfExists(dir)
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
                                    fileOpenLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter,
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
    def apply(keyValues: Slice[KeyValue.WriteOnly] = randomizedKeyValues()(TestTimer.Incremental(), KeyOrder.default, keyValueLimiter),
              removeDeletes: Boolean = false,
              path: Path = testSegmentFile,
              bloomFilterFalsePositiveRate: Double = TestData.falsePositiveRate)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                 keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter,
                                                                                 fileOpenLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter,
                                                                                 timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                                                 groupingStrategy: Option[KeyValueGroupingStrategyInternal] = randomGroupingStrategyOption(randomIntMax(1000))): IO[Segment] =
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
//        level ! PushSegments(segments, replyTo)
        ???
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
              bloomFilterFalsePositiveRate: Double = TestData.falsePositiveRate,
              compressDuplicateValues: Boolean = true,
              deleteSegmentsEventually: Boolean = false)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                        keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter,
                                                        fileOpenLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter,
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
        compressDuplicateValues = compressDuplicateValues,
        deleteSegmentsEventually = deleteSegmentsEventually
      ).assertGet.asInstanceOf[Level]
  }

  object TestLevelZero {

    def apply(nextLevel: Option[LevelRef],
              mapSize: Long = mapSize,
              brake: Level0Meter => Accelerator = Accelerator.brake(),
              throttleOn: Boolean = true)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                          keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter,
                                          timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                          fileOpenLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter): LevelZero =
      LevelZero(
        mapSize = mapSize,
        storage = level0Storage,
        nextLevel = nextLevel,
        throttleOn = throttleOn,
        acceleration = brake,
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
    IOEffect.write(bytes, testDir.resolve(nextSegmentId)).assertGet

  def createFileReader(path: Path): FileReader = {
    implicit val limiter = fileOpenLimiter
    new FileReader(
      if (Random.nextBoolean())
        DBFile.channelRead(path, autoClose = true).assertGet
      else
        DBFile.mmapRead(path, autoClose = true).assertGet
    )
  }

  def createFileChannelReader(bytes: Slice[Byte]): FileReader =
    createFileReader(createFile(bytes))

  /**
    * Runs multiple asserts on individual levels and also one by one merges key-values from upper levels
    * to lower levels and asserts the results are still the same.
    *
    * The tests written only need to define a 3 level test case and this function will create a 4 level database
    * and run multiple passes for the test merging key-values from levels into lower levels asserting the results
    * are the same after merge.
    *
    * Note: Tests for decremental time is not required because in reality upper Level cannot have lower time key-values
    * that are not merged into lower Level already. So there will never be a situation where upper Level's keys are
    * ignored completely due to it having a lower or equal time to lower Level. If it has a lower or same time this means
    * that it has already been merged into lower Levels already making the upper Level's read always valid.
    */
  def assertLevel(level0KeyValues: (Iterable[Memory], Iterable[Memory], TestTimer) => Iterable[Memory] = (_, _, _) => Iterable.empty,
                  assertLevel0: (Iterable[Memory], Iterable[Memory], Iterable[Memory], LevelRef) => Unit = (_, _, _, _) => (),
                  level1KeyValues: (Iterable[Memory], TestTimer) => Iterable[Memory] = (_, _) => Iterable.empty,
                  assertLevel1: (Iterable[Memory], Iterable[Memory], LevelRef) => Unit = (_, _, _) => (),
                  level2KeyValues: TestTimer => Iterable[Memory] = _ => Iterable.empty,
                  assertLevel2: (Iterable[Memory], LevelRef) => Unit = (_, _) => (),
                  assertAllLevels: (Iterable[Memory], Iterable[Memory], Iterable[Memory], LevelRef) => Unit = (_, _, _, _) => (),
                  throttleOn: Boolean = false)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                 groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Unit = {

    def iterationMessage =
      s"Thread: ${Thread.currentThread().getId} - throttleOn: $throttleOn"

    println(iterationMessage)

    val noAssert =
      (_: LevelRef) => ()

    val testTimer: TestTimer = TestTimer.Incremental()

    /**
      * If [[throttleOn]] is true then enable fast throttling
      * so that this test covers as many scenarios as possible.
      */
    val throttle: LevelMeter => Throttle = if (throttleOn) _ => Throttle(Duration.Zero, randomNextInt(3) max 1) else _ => Throttle(Duration.Zero, 0)

    println("Starting levels")

    val level4 = TestLevel(throttle = throttle)
    val level3 = TestLevel(nextLevel = Some(level4), throttle = throttle)
    val level2 = TestLevel(nextLevel = Some(level3), throttle = throttle)
    val level1 = TestLevel(nextLevel = Some(level2), throttle = throttle)
    val level0 = TestLevelZero(nextLevel = Some(level1), throttleOn = throttleOn)

    println("Levels started")

    //start with a default testTimer.
    val level2KV = level2KeyValues(testTimer)
    println("level2KV created.")

    //if upper levels should insert key-values at an older time start the testTimer to use older time
    val level1KV = level1KeyValues(level2KV, testTimer)
    println("level1KV created.")

    //if upper levels should insert key-values at an older time start the testTimer to use older time
    val level0KV = level0KeyValues(level1KV, level2KV, testTimer)
    println("level0KV created.")

    val level0Assert: LevelRef => Unit = assertLevel0(level0KV, level1KV, level2KV, _)
    val level1Assert: LevelRef => Unit = assertLevel1(level1KV, level2KV, _)
    val level2Assert: LevelRef => Unit = assertLevel2(level0KV, _)
    val levelAllAssert: LevelRef => Unit = assertAllLevels(level0KV, level1KV, level2KV, _)

    def runAsserts(asserts: Seq[((Iterable[Memory], LevelRef => Unit), (Iterable[Memory], LevelRef => Unit), (Iterable[Memory], LevelRef => Unit), (Iterable[Memory], LevelRef => Unit))]) =
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

    val asserts =
      if (throttleOn)
      //if throttle is only the top most Level's (Level0) assert should
      // be executed because throttle behaviour is unknown during runtime
      // and lower Level's key-values would change as compaction continues.
        (1 to 5) map (
          i =>
            if (i == 1)
              (
                (level0KV, level0Assert),
                (level1KV, noAssert),
                (level2KV, noAssert),
                (Iterable.empty, noAssert),
              )
            else
              (
                (Iterable.empty, level0Assert),
                (Iterable.empty, noAssert),
                (Iterable.empty, noAssert),
                (Iterable.empty, noAssert),
              )
          )
      else
        Seq(
          (
            (level0KV, level0Assert),
            (level1KV, level1Assert),
            (level2KV, level2Assert),
            (Iterable.empty, noAssert),
          ),
          (
            (level0KV, level0Assert),
            (level1KV, level1Assert),
            (Iterable.empty, level2Assert),
            (level2KV, level2Assert),
          ),
          (
            (level0KV, level0Assert),
            (Iterable.empty, level1Assert),
            (level1KV, level1Assert),
            (level2KV, level2Assert),
          ),
          (
            (Iterable.empty, level0Assert),
            (level0KV, level0Assert),
            (level1KV, level1Assert),
            (level2KV, level2Assert),
          ),
          (
            (Iterable.empty, level0Assert),
            (Iterable.empty, level0Assert),
            (level0KV, level0Assert),
            (level1KV, level1Assert)
          ),
          (
            (Iterable.empty, level0Assert),
            (Iterable.empty, level0Assert),
            (Iterable.empty, level0Assert),
            (level0KV, level0Assert)
          )
        )

    runAsserts(asserts)

    level0.close.assertGet

    if (!throttleOn)
      assertLevel(
        level0KeyValues = level0KeyValues,
        assertLevel0 = assertLevel0,
        level1KeyValues = level1KeyValues,
        assertLevel1 = assertLevel1,
        level2KeyValues = level2KeyValues,
        assertLevel2 = assertLevel2,
        assertAllLevels = assertAllLevels,
        throttleOn = true
      )
  }

  private def doAssertOnLevel(level0KeyValues: Iterable[Memory],
                              assertLevel0: LevelRef => Unit,
                              level0: LevelZero,
                              level1KeyValues: Iterable[Memory],
                              assertLevel1: LevelRef => Unit,
                              level1: Level,
                              level2KeyValues: Iterable[Memory],
                              assertLevel2: LevelRef => Unit,
                              level2: Level,
                              level3KeyValues: Iterable[Memory],
                              assertLevel3: LevelRef => Unit,
                              level3: Level,
                              assertAllLevels: LevelRef => Unit,
                              assertLevel3ForAllLevels: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                 groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Unit = {
    println("level3.putKeyValues")
    if (level3KeyValues.nonEmpty) level3.putKeyValues(level3KeyValues).assertGet
    println("level2.putKeyValues")
    if (level2KeyValues.nonEmpty) level2.putKeyValues(level2KeyValues).assertGet
    println("level1.putKeyValues")
    if (level1KeyValues.nonEmpty) level1.putKeyValues(level1KeyValues).assertGet
    println("level0.putKeyValues")
    if (level0KeyValues.nonEmpty) level0.putKeyValues(level0KeyValues).assertGet
    import RunThis._

    Seq(
      () => {
        println("asserting Level3")
        assertLevel3(level3)
      },
      () => {
        println("asserting Level2")
        assertLevel2(level2)
      },
      () => {
        println("asserting Level1")
        assertLevel1(level1)
      },

      () => {
        println("asserting Level0")
        assertLevel0(level0)
      },
      () => {
        if (assertLevel3ForAllLevels) {
          println("asserting all on Level3")
          assertAllLevels(level3)
        }
      },
      () => {
        println("asserting all on Level2")
        assertAllLevels(level2)
      },
      () => {
        println("asserting all on Level1")
        assertAllLevels(level1)
      },

      () => {
        println("asserting all on Level0")
        assertAllLevels(level0)
      }
    ).runThisRandomlyInParallel
  }

  def assertSegment[T](keyValues: Slice[Memory],
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
