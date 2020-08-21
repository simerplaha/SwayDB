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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core

import java.nio.file._
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import swaydb.ActorWire
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.core.data.{Memory, MemoryOption, Time}
import swaydb.core.io.file.{DBFile, Effect}
import swaydb.core.io.reader.FileReader
import swaydb.core.level.compaction._
import swaydb.core.level.compaction.throttle.{ThrottleCompactor, ThrottleState}
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{Level, LevelRef, NextLevel, PathsDistributor}
import swaydb.core.map.MapEntry
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.segment.merge.MergeStats
import swaydb.core.segment.{PersistentSegment, Segment, SegmentIO}
import swaydb.core.util.{BlockCacheFileIDGenerator, IDGenerator}
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{CompactionExecutionContext, LevelMeter, Throttle}
import swaydb.data.config.{Dir, MMAP, RecoveryMode}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.storage.{AppendixStorage, Level0Storage, LevelStorage}
import swaydb.data.util.OperatingSystem
import swaydb.data.util.StorageUnits._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random

trait TestBase extends AnyWordSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with Eventually {

  implicit val idGenerator = IDGenerator()

  private val currentLevelId = new AtomicInteger(100000000)

  private def nextLevelId: Int =
    currentLevelId.decrementAndGet()

  private val projectTargetFolder = getClass.getClassLoader.getResource("").getPath

  val projectDirectory =
    if (OperatingSystem.isWindows)
      Paths.get(projectTargetFolder.drop(1)).getParent.getParent
    else
      Paths.get(projectTargetFolder).getParent.getParent

  val testFileDirectory = projectDirectory.resolve("TEST_FILES")

  val testMemoryFileDirectory = projectDirectory.resolve("TEST_MEMORY_FILES")

  val testClassDirPath = testFileDirectory.resolve(this.getClass.getSimpleName)

  def levelFoldersCount = 0

  def mmapSegments: MMAP.Segment = MMAP.Enabled(OperatingSystem.isWindows)

  def level0MMAP: MMAP.Map = MMAP.Enabled(OperatingSystem.isWindows)

  def appendixStorageMMAP: MMAP.Map = MMAP.Enabled(OperatingSystem.isWindows)

  def isWindowsAndMMAPSegments(): Boolean =
    OperatingSystem.isWindows && mmapSegments.mmapReads && mmapSegments.mmapWrites

  def inMemoryStorage = false

  def nextTime(implicit testTimer: TestTimer): Time =
    testTimer.next

  def levelStorage: LevelStorage =
    if (inMemoryStorage)
      LevelStorage.Memory(dir = memoryTestClassDir.resolve(nextLevelId.toString))
    else
      LevelStorage.Persistent(
        dir = testClassDir.resolve(nextLevelId.toString),
        otherDirs =
          (0 until levelFoldersCount) map {
            _ =>
              Dir(testClassDir.resolve(nextLevelId.toString), 1)
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

  def randomDir(implicit sweeper: TestCaseSweeper) = testClassDir.resolve(s"${randomCharacters()}").sweep()

  def createRandomDir(implicit sweeper: TestCaseSweeper) = Files.createDirectory(randomDir).sweep()

  def randomFilePath(implicit sweeper: TestCaseSweeper) =
    testClassDir.resolve(s"${randomCharacters()}.test").sweep()

  def nextSegmentId = idGenerator.nextSegmentID

  def nextId = idGenerator.nextID

  def deleteFiles = true

  def randomIntDirectory: Path =
    testClassDir.resolve(nextLevelId.toString)

  def createRandomIntDirectory(implicit sweeper: TestCaseSweeper): Path =
    if (persistent)
      Effect.createDirectoriesIfAbsent(randomIntDirectory).sweep()
    else
      randomIntDirectory

  def createNextLevelPath: Path =
    Effect.createDirectoriesIfAbsent(nextLevelPath)

  def createPathDistributor =
    PathsDistributor(Seq(Dir(createNextLevelPath, 1)), () => Seq.empty)

  def nextLevelPath: Path =
    testClassDir.resolve(nextLevelId.toString)

  def testSegmentFile: Path =
    if (memory)
      randomIntDirectory.resolve(nextSegmentId)
    else
      Effect.createDirectoriesIfAbsent(randomIntDirectory).resolve(nextSegmentId)

  def testMapFile: Path =
    if (memory)
      randomIntDirectory.resolve(nextId.toString + ".map")
    else
      Effect.createDirectoriesIfAbsent(randomIntDirectory).resolve(nextId.toString + ".map")

  def farOut = new Exception("Far out! Something went wrong")

  def testClassDir: Path =
    if (inMemoryStorage)
      testClassDirPath
    else
      Effect.createDirectoriesIfAbsent(testClassDirPath)

  def memoryTestClassDir =
    testFileDirectory.resolve(this.getClass.getSimpleName + "_MEMORY_DIR")

  override protected def beforeAll(): Unit =
    if(deleteFiles)
      Effect.walkDelete(testClassDirPath)

  override protected def afterEach(): Unit =
    Effect.deleteIfExists(testClassDirPath)

  object TestMap {
    def apply(keyValues: Slice[Memory],
              fileSize: Int = 4.mb,
              path: Path = testMapFile,
              flushOnOverflow: Boolean = false,
              mmap: MMAP.Map = MMAP.Enabled(OperatingSystem.isWindows))(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                        timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                                        sweeper: TestCaseSweeper): map.Map[SliceOption[Byte], MemoryOption, Slice[Byte], Memory] = {
      import swaydb.core.map.serializer.LevelZeroMapEntryWriter._
      implicit val merger = swaydb.core.level.zero.LevelZeroSkipListMerger
      import sweeper._

      val testMap =
        if (levelStorage.memory)
          map.Map.memory[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](
            nullKey = Slice.Null,
            nullValue = Memory.Null,
            fileSize = fileSize,
            flushOnOverflow = flushOnOverflow
          )
        else
          map.Map.persistent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](
            nullKey = Slice.Null,
            nullValue = Memory.Null,
            folder = path,
            mmap = mmap,
            flushOnOverflow = flushOnOverflow,
            fileSize = fileSize
          ).runRandomIO.right.value

      keyValues foreach {
        keyValue =>
          testMap.writeSync(MapEntry.Put(keyValue.key, keyValue))
      }

      testMap.sweep()
    }
  }

  object TestSegment {
    def apply(keyValues: Slice[Memory] = randomizedKeyValues()(TestTimer.Incremental()),
              createdInLevel: Int = 1,
              path: Path = testSegmentFile,
              valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random,
              sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random,
              binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random,
              hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random,
              bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random,
              segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(mmap = mmapSegments))(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                                         timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                                                                         sweeper: TestCaseSweeper): Segment = {

      val segmentId = Effect.fileId(path)._1 - 1

      implicit val idGenerator: IDGenerator = IDGenerator(segmentId)

      implicit val pathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq.empty)

      val segments =
        many(
          createdInLevel = createdInLevel,
          keyValues = keyValues,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentConfig = segmentConfig.copy(minSize = Int.MaxValue, maxCount = Int.MaxValue)
        )

      segments should have size 1

      segments.head
    }

    def many(createdInLevel: Int = 1,
             keyValues: Slice[Memory] = randomizedKeyValues()(TestTimer.Incremental()),
             valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random,
             sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random,
             binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random,
             hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random,
             bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random,
             segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(mmap = mmapSegments))(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                                        timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                                                                        pathsDistributor: PathsDistributor,
                                                                                                        idGenerator: IDGenerator,
                                                                                                        sweeper: TestCaseSweeper): Slice[Segment] = {

      import sweeper._

      implicit val segmentIO: SegmentIO =
        SegmentIO(
          bloomFilterConfig = bloomFilterConfig,
          hashIndexConfig = hashIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          sortedIndexConfig = sortedIndexConfig,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig
        )

      val segment =
        if (levelStorage.memory)
          Segment.memory(
            minSegmentSize = segmentConfig.minSize,
            maxKeyValueCountPerSegment = segmentConfig.maxCount,
            pathsDistributor = pathsDistributor,
            createdInLevel = createdInLevel,
            keyValues = MergeStats.memoryBuilder(keyValues).close
          )
        else
          Segment.persistent(
            pathsDistributor = pathsDistributor,
            createdInLevel = createdInLevel,
            bloomFilterConfig = bloomFilterConfig,
            hashIndexConfig = hashIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            sortedIndexConfig = sortedIndexConfig,
            valuesConfig = valuesConfig,
            segmentConfig = segmentConfig,
            mergeStats = MergeStats.persistentBuilder(keyValues).close(sortedIndexConfig.enableAccessPositionIndex)
          )

      segment.foreach(_.sweep())

      segment
    }
  }

  object TestLevel {

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
              nextLevel: Option[NextLevel] = None,
              throttle: LevelMeter => Throttle = testDefaultThrottle,
              valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random,
              sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random,
              binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random,
              hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random,
              bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random,
              segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random2(pushForward = false, deleteEventually = false, mmap = mmapSegments),
              keyValues: Slice[Memory] = Slice.empty)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                      timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                      sweeper: TestCaseSweeper): Level = {
      import sweeper._

      val level =
        Level(
          levelStorage = levelStorage,
          appendixStorage = appendixStorage,
          nextLevel = nextLevel,
          throttle = throttle,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentConfig = segmentConfig
        )

      level.flatMap {
        level =>
          level.putKeyValuesTest(keyValues) map {
            _ =>
              level
          }
      }.right.value.sweep()
    }
  }

  object TestLevelZero {

    def apply(nextLevel: Option[Level],
              mapSize: Long = randomIntMax(10.mb),
              brake: LevelZeroMeter => Accelerator = Accelerator.brake(),
              throttle: LevelZeroMeter => FiniteDuration = _ => Duration.Zero)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                               timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                                               sweeper: TestCaseSweeper): LevelZero = {
      import sweeper._

      LevelZero(
        mapSize = mapSize,
        storage = level0Storage,
        nextLevel = nextLevel,
        enableTimer = true,
        cacheKeyValueIds = randomBoolean(),
        throttle = throttle,
        acceleration = brake
      ).value.sweep()
    }
  }

  def createFile(bytes: Slice[Byte])(implicit sweeper: TestCaseSweeper): Path =
    Effect.write(testClassDir.resolve(nextSegmentId).sweep(), bytes)

  def createRandomFileReader(path: Path)(implicit sweeper: TestCaseSweeper): FileReader = {
    if (Random.nextBoolean())
      createMMAPFileReader(path)
    else
      createFileChannelFileReader(path)
  }

  def createAllFilesReaders(path: Path)(implicit sweeper: TestCaseSweeper): List[FileReader] =
    List(
      createMMAPFileReader(path),
      createFileChannelFileReader(path)
    )

  def createMMAPFileReader(bytes: Slice[Byte])(implicit sweeper: TestCaseSweeper): FileReader =
    createMMAPFileReader(createFile(bytes))

  /**
   * Creates all file types currently supported which are MMAP and FileChannel.
   */
  def createDBFiles(mmapPath: Path, mmapBytes: Slice[Byte], channelPath: Path, channelBytes: Slice[Byte])(implicit sweeper: TestCaseSweeper): List[DBFile] =
    List(
      createMMAPWriteAndRead(mmapPath, mmapBytes),
      createChannelWriteAndRead(channelPath, channelBytes)
    )

  def createDBFiles(mmapBytes: Slice[Byte], channelBytes: Slice[Byte])(implicit sweeper: TestCaseSweeper): List[DBFile] =
    List(
      createMMAPWriteAndRead(randomFilePath, mmapBytes),
      createChannelWriteAndRead(randomFilePath, channelBytes)
    )

  def createMMAPWriteAndRead(path: Path, bytes: Slice[Byte])(implicit sweeper: TestCaseSweeper): DBFile = {
    import sweeper._

    DBFile.mmapWriteAndRead(
      path = path,
      ioStrategy = randomThreadSafeIOStrategy(),
      autoClose = true,
      deleteOnClean = OperatingSystem.isWindows,
      blockCacheFileId = BlockCacheFileIDGenerator.nextID,
      bytes = bytes
    )
  }

  def createChannelWriteAndRead(path: Path, bytes: Slice[Byte])(implicit sweeper: TestCaseSweeper): DBFile = {
    val blockCacheFileId = BlockCacheFileIDGenerator.nextID

    import sweeper._

    val file =
      DBFile.channelWrite(
        path = path,
        ioStrategy = randomThreadSafeIOStrategy(),
        blockCacheFileId = blockCacheFileId,
        autoClose = true
      )

    file.append(bytes)
    file.close()

    DBFile.mmapRead(
      path = path,
      ioStrategy = randomThreadSafeIOStrategy(),
      autoClose = true,
      deleteOnClean = OperatingSystem.isWindows,
      blockCacheFileId = blockCacheFileId
    )
  }

  def createMMAPFileReader(path: Path)(implicit sweeper: TestCaseSweeper): FileReader = {
    import sweeper._

    new FileReader(
      DBFile.mmapRead(
        path = path.sweep(),
        ioStrategy = randomThreadSafeIOStrategy(),
        autoClose = true,
        deleteOnClean = OperatingSystem.isWindows,
        blockCacheFileId = BlockCacheFileIDGenerator.nextID
      )
    )
  }

  def createFileChannelFileReader(bytes: Slice[Byte])(implicit sweeper: TestCaseSweeper): FileReader =
    createFileChannelFileReader(createFile(bytes))

  def createFileChannelFileReader(path: Path)(implicit sweeper: TestCaseSweeper): FileReader = {
    import sweeper._

    val file =
      DBFile.channelRead(
        path = path.sweep(),
        ioStrategy = randomThreadSafeIOStrategy(),
        autoClose = true,
        blockCacheFileId = BlockCacheFileIDGenerator.nextID
      )

    new FileReader(file)
  }

  def createRandomFileReader(bytes: Slice[Byte])(implicit sweeper: TestCaseSweeper): FileReader =
    createRandomFileReader(createFile(bytes))

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
   * ignored completely due to it having a lower or equal time to lower Level. If it has a lower or same time thi®s means
   * that it has already been merged into lower Levels already making the upper Level's read always valid.
   */
  def assertLevel(level0KeyValues: (Slice[Memory], Slice[Memory], TestTimer) => Slice[Memory] = (_, _, _) => Slice.empty,
                  assertLevel0: (Slice[Memory], Slice[Memory], Slice[Memory], LevelRef) => Unit = (_, _, _, _) => (),
                  level1KeyValues: (Slice[Memory], TestTimer) => Slice[Memory] = (_, _) => Slice.empty,
                  assertLevel1: (Slice[Memory], Slice[Memory], LevelRef) => Unit = (_, _, _) => (),
                  level2KeyValues: TestTimer => Slice[Memory] = _ => Slice.empty,
                  assertLevel2: (Slice[Memory], LevelRef) => Unit = (_, _) => (),
                  assertAllLevels: (Slice[Memory], Slice[Memory], Slice[Memory], LevelRef) => Unit = (_, _, _, _) => (),
                  throttleOn: Boolean = false)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                               ec: ExecutionContext = TestExecutionContext.executionContext): Unit = {

    def iterationMessage =
      s"Thread: ${Thread.currentThread().getId} - throttleOn: $throttleOn"

    implicit val compactionStrategy: Compactor[ThrottleState] =
      ThrottleCompactor

    println(iterationMessage)

    val noAssert =
      (_: LevelRef) => ()

    val testTimer: TestTimer = TestTimer.Incremental()

    /**
     * If [[throttleOn]] is true then enable fast throttling
     * so that this test covers as many scenarios as possible.
     */
    val levelThrottle: LevelMeter => Throttle = if (throttleOn) _ => Throttle(Duration.Zero, randomNextInt(3) max 1) else _ => Throttle(Duration.Zero, 0)
    val levelZeroThrottle: LevelZeroMeter => FiniteDuration = if (throttleOn) _ => Duration.Zero else _ => 365.days

    println("Starting levels")

    implicit val levelSweeper: TestCaseSweeper = TestCaseSweeper()

    val level4 = TestLevel(throttle = levelThrottle)
    val level3 = TestLevel(nextLevel = Some(level4), throttle = levelThrottle)
    val level2 = TestLevel(nextLevel = Some(level3), throttle = levelThrottle)
    val level1 = TestLevel(nextLevel = Some(level2), throttle = levelThrottle)
    val level0 = TestLevelZero(nextLevel = Some(level1), throttle = levelZeroThrottle)

    val compaction: Option[ActorWire[Compactor[ThrottleState], ThrottleState]] =
      if (throttleOn)
        Some(
          CoreInitializer.initialiseCompaction(
            zero = level0,
            executionContexts = CompactionExecutionContext.Create(TestExecutionContext.executionContext) +: List.fill(4)(CompactionExecutionContext.Shared)
          ) get
        )
      else
        None

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

    val asserts: Seq[((Slice[Memory], LevelRef => Unit), (Slice[Memory], LevelRef => Unit), (Slice[Memory], LevelRef => Unit), (Slice[Memory], LevelRef => Unit))] =
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
                (Slice.empty, noAssert)
              )
            else
              (
                (Slice.empty, level0Assert),
                (Slice.empty, noAssert),
                (Slice.empty, noAssert),
                (Slice.empty, noAssert)
              )
          )
      else
        Seq(
          (
            (level0KV, level0Assert),
            (level1KV, level1Assert),
            (level2KV, level2Assert),
            (Slice.empty, noAssert)
          ),
          (
            (level0KV, level0Assert),
            (level1KV, level1Assert),
            (Slice.empty, level2Assert),
            (level2KV, level2Assert)
          ),
          (
            (level0KV, level0Assert),
            (Slice.empty, level1Assert),
            (level1KV, level1Assert),
            (level2KV, level2Assert)
          ),
          (
            (Slice.empty, level0Assert),
            (level0KV, level0Assert),
            (level1KV, level1Assert),
            (level2KV, level2Assert)
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

    runAsserts(asserts)

    import swaydb.data.RunThis._

    compaction foreach {
      implicit compaction =>
        CoreShutdown.close(level0, 2.seconds).await(10.seconds)
    }

    level0.delete(2.seconds).await(10.seconds)

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
                                                                 levelSweeper: TestCaseSweeper): Unit = {
    println("level3.putKeyValues")
    if (level3KeyValues.nonEmpty) level3.putKeyValuesTest(level3KeyValues).runRandomIO.right.value
    println("level2.putKeyValues")
    if (level2KeyValues.nonEmpty) level2.putKeyValuesTest(level2KeyValues).runRandomIO.right.value
    println("level1.putKeyValues")
    if (level1KeyValues.nonEmpty) level1.putKeyValuesTest(level1KeyValues).runRandomIO.right.value
    println("level0.putKeyValues")
    if (level0KeyValues.nonEmpty) level0.putKeyValues(level0KeyValues).runRandomIO.right.value
    import swaydb.data.RunThis._

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
                       segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(mmap = mmapSegments),
                       ensureOneSegmentOnly: Boolean = true,
                       testAgainAfterAssert: Boolean = true,
                       closeAfterCreate: Boolean = false,
                       valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random,
                       sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random,
                       binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random,
                       hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random,
                       bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                                    ec: ExecutionContext = TestExecutionContext.executionContext,
                                                                                                    sweeper: TestCaseSweeper,
                                                                                                    segmentIO: SegmentIO = SegmentIO.random) = {
    println(s"assertSegment - keyValues: ${keyValues.size}")

    //ensure that only one segment gets created
    val adjustedSegmentConfig =
      if (!ensureOneSegmentOnly || keyValues.size == 1)
        segmentConfig //one doesn't matter.
      else if (memory)
        segmentConfig.copy(minSize = Int.MaxValue, maxCount = Int.MaxValue)
      else
        segmentConfig.copy(minSize = Int.MaxValue, maxCount = randomIntMax(keyValues.size + 1))

    val segment =
      TestSegment(
        keyValues = keyValues,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        segmentConfig = adjustedSegmentConfig
      )

    if (closeAfterCreate) segment.close

    assert(keyValues, segment) //first
    if (testAgainAfterAssert) {
      assert(keyValues, segment) //with cache populated

      //clear cache and assert
      segment.clearCachedKeyValues()
      assert(keyValues, segment) //same Segment but test with cleared cache.

      //clear all caches and assert
      segment.clearAllCaches()
      assert(keyValues, segment) //same Segment but test with cleared cache.
    }

    segment match {
      case segment: PersistentSegment =>
        val segmentReopened = segment.reopen //reopen
        if (closeAfterCreate) segmentReopened.close
        assert(keyValues, segmentReopened)

        if (testAgainAfterAssert) assert(keyValues, segmentReopened)
        segmentReopened.close

      case _: Segment =>
      //memory segment cannot be reopened
    }

    segment.close
  }
}