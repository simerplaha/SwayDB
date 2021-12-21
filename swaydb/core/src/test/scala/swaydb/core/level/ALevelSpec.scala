//package swaydb.core.level
//
//import swaydb.{DefActor, Glass, IO}
//import swaydb.config.{Atomic, MMAP, OptimiseWrites, RecoveryMode}
//import swaydb.config.accelerate.{Accelerator, LevelZeroMeter}
//import swaydb.config.compaction.{CompactionConfig, LevelMeter, LevelThrottle, LevelZeroThrottle}
//import swaydb.config.storage.{Level0Storage, LevelStorage}
//import swaydb.core.{CoreInitialiser, TestExecutionContext, GenForceSave, CoreTestSweeper, TestTimer}
//import swaydb.core.compaction.{Compactor, CompactorCreator}
//import swaydb.core.compaction.throttle.ThrottleCompactorCreator
//import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
//import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
//import swaydb.core.segment.block.segment.SegmentBlockConfig
//import swaydb.core.segment.block.values.ValuesBlockConfig
//import swaydb.core.segment.io.SegmentCompactionIO
//import swaydb.core.CommonAssertions.randomPushStrategy
//import swaydb.core.level.zero.LevelZero
//import swaydb.core.segment.block.hashindex.HashIndexBlockConfig
//import swaydb.core.segment.block.sortedindex.SortedIndexBlockConfig
//import swaydb.core.segment.data.{SegmentKeyOrders, Memory}
//import swaydb.core.CoreTestData._
//import swaydb.slice.Slice
//import swaydb.slice.order.{KeyOrder, TimeOrder}
//import swaydb.testkit.TestKit.{randomBoolean, randomIntMax, randomNextInt}
//import swaydb.effect.IOValues._
//import swaydb.core.segment.{ASegmentSpec, pathDistributor}
//import swaydb.core.CoreTestSweeper._
//import swaydb.core.file.sweeper.FileSweeper
//import swaydb.effect.{Dir, Effect}
//import swaydb.utils.OperatingSystem
//import swaydb.utils.StorageUnits._
//
//import java.nio.file.Path
//import scala.concurrent.ExecutionContext
//import scala.concurrent.duration._
//
//trait ALevelSpec extends AnyWordSpec {
//
//  def nextLevelId: Int = nextIntReversed
//
//  def levelFoldersCount = 0
//
//  def level0MMAP: MMAP.Log = MMAP.On(OperatingSystem.isWindows(), GenForceSave.mmap())
//
//  def createNextLevelPath: Path =
//    Effect.createDirectoriesIfAbsent(nextLevelPath)
//
//  def createPathDistributor()(implicit sweeper: CoreTestSweeper) =
//    pathDistributor(Seq(Dir(createNextLevelPath.sweep(), 1)), () => Seq.empty)
//
//  def nextLevelPath: Path =
//    testClassDir.resolve(nextIntReversed.toString)
//
//  def levelStorage: LevelStorage =
//    if (isMemorySpec)
//      LevelStorage.Memory(dir = memoryTestClassDir.resolve(nextLevelId.toString))
//    else
//      LevelStorage.Persistent(
//        dir = testClassDir.resolve(nextLevelId.toString),
//        otherDirs =
//          (0 until levelFoldersCount) map {
//            _ =>
//              Dir(testClassDir.resolve(nextLevelId.toString), 1)
//          },
//        appendixMMAP = appendixStorageMMAP,
//        appendixFlushCheckpointSize = 4.mb
//      )
//
//  def level0Storage: Level0Storage =
//    if (isMemorySpec)
//      Level0Storage.Memory
//    else
//      Level0Storage.Persistent(mmap = level0MMAP, randomIntDirectory, RecoveryMode.ReportFailure)
//
//
//  object TestLevel {
//
//    def testDefaultThrottle(meter: LevelMeter): LevelThrottle =
//      if (meter.segmentsCount > 15)
//        LevelThrottle(Duration.Zero, 20)
//      else if (meter.segmentsCount > 10)
//        LevelThrottle(1.second, 20)
//      else if (meter.segmentsCount > 5)
//        LevelThrottle(2.seconds, 10)
//      else
//        LevelThrottle(3.seconds, 10)
//
//    implicit def toSome[T](input: T): Option[T] =
//      Some(input)
//
//    def apply(levelStorage: LevelStorage = levelStorage,
//              nextLevel: Option[NextLevel] = None,
//              throttle: LevelMeter => LevelThrottle = testDefaultThrottle,
//              valuesConfig: ValuesBlockConfig = ValuesBlockConfig.random,
//              sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random,
//              binarySearchIndexConfig: BinarySearchIndexBlockConfig = BinarySearchIndexBlockConfig.random,
//              hashIndexConfig: HashIndexBlockConfig = HashIndexBlockConfig.random,
//              bloomFilterConfig: BloomFilterBlockConfig = BloomFilterBlockConfig.random,
//              segmentConfig: SegmentBlockConfig = SegmentBlockConfig.random2(deleteDelay = Duration.Zero, mmap = mmapSegments),
//              keyValues: Slice[Memory] = Slice.empty)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
//                                                      timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
//                                                      sweeper: CoreTestSweeper): Level = {
//      import sweeper._
//
//      implicit val keyOrders: SegmentKeyOrders =
//        SegmentKeyOrders(keyOrder)
//
//      val level =
//        Level(
//          levelStorage = levelStorage,
//          nextLevel = nextLevel,
//          throttle = throttle,
//          valuesConfig = valuesConfig,
//          sortedIndexConfig = sortedIndexConfig,
//          binarySearchIndexConfig = binarySearchIndexConfig,
//          hashIndexConfig = hashIndexConfig,
//          bloomFilterConfig = bloomFilterConfig,
//          segmentConfig = segmentConfig
//        )
//
//      level.flatMap {
//        level =>
//          if (keyValues.nonEmpty)
//            level.put(keyValues) map {
//              _ =>
//                level
//            }
//          else
//            IO[swaydb.Error.Level, Level](level)
//      }.right.value.sweep()
//    }
//  }
//
//  object TestLevelZero {
//
//    def apply(nextLevel: Option[Level],
//              logSize: Int = randomIntMax(10.mb),
//              appliedFunctionsLogSize: Int = randomIntMax(1.mb),
//              clearAppliedFunctionsOnBoot: Boolean = false,
//              enableTimer: Boolean = true,
//              brake: LevelZeroMeter => Accelerator = Accelerator.brake(),
//              throttle: LevelZeroMeter => LevelZeroThrottle = _ => LevelZeroThrottle(Duration.Zero, 1))(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
//                                                                                                        timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
//                                                                                                        sweeper: CoreTestSweeper,
//                                                                                                        optimiseWrites: OptimiseWrites = OptimiseWrites.random,
//                                                                                                        atomic: Atomic = Atomic.random): LevelZero = {
//      import sweeper._
//
//      LevelZero(
//        logSize = logSize,
//        appliedFunctionsLogSize = appliedFunctionsLogSize,
//        clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
//        storage = level0Storage,
//        nextLevel = nextLevel,
//        enableTimer = enableTimer,
//        cacheKeyValueIds = randomBoolean(),
//        throttle = throttle,
//        acceleration = brake
//      ).value.sweep()
//    }
//  }
//
//  /**
//   * Runs multiple asserts on individual levels and also one by one merges key-values from upper levels
//   * to lower levels and asserts the results are still the same.
//   *
//   * The tests written only need to define a 3 level test case and this function will create a 4 level database
//   * and run multiple passes for the test merging key-values from levels into lower levels asserting the results
//   * are the same after merge.
//   *
//   * Note: Tests for decremental time is not required because in reality upper Level cannot have lower time key-values
//   * that are not merged into lower Level already. So there will never be a situation where upper Level's keys are
//   * ignored completely due to it having a lower or equal time to lower Level. If it has a lower or same time thi®s means
//   * that it has already been merged into lower Levels already making the upper Level's read always valid.
//   */
//  def assertLevel(level0KeyValues: (Slice[Memory], Slice[Memory], TestTimer) => Slice[Memory] = (_, _, _) => Slice.empty,
//                  assertLevel0: (Slice[Memory], Slice[Memory], Slice[Memory], LevelRef) => Unit = (_, _, _, _) => (),
//                  level1KeyValues: (Slice[Memory], TestTimer) => Slice[Memory] = (_, _) => Slice.empty,
//                  assertLevel1: (Slice[Memory], Slice[Memory], LevelRef) => Unit = (_, _, _) => (),
//                  level2KeyValues: TestTimer => Slice[Memory] = _ => Slice.empty,
//                  assertLevel2: (Slice[Memory], LevelRef) => Unit = (_, _) => (),
//                  assertAllLevels: (Slice[Memory], Slice[Memory], Slice[Memory], LevelRef) => Unit = (_, _, _, _) => (),
//                  throttleOn: Boolean = false)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default): Unit = {
//
//    def iterationMessage =
//      s"Thread: ${Thread.currentThread().getId} - throttleOn: $throttleOn"
//
//    implicit val compactionStrategy: CompactorCreator =
//      ThrottleCompactorCreator
//
//    println(iterationMessage)
//
//    val noAssert =
//      (_: LevelRef) => ()
//
//    val testTimer: TestTimer = TestTimer.Incremental()
//
//    /**
//     * If [[throttleOn]] is true then enable fast throttling
//     * so that this test covers as many scenarios as possible.
//     */
//    val levelThrottle: LevelMeter => LevelThrottle = if (throttleOn) _ => LevelThrottle(Duration.Zero, randomNextInt(3) max 1) else _ => LevelThrottle(Duration.Zero, 0)
//    val levelZeroThrottle: LevelZeroMeter => LevelZeroThrottle = if (throttleOn) _ => LevelZeroThrottle(Duration.Zero, 1) else _ => LevelZeroThrottle(365.days, 0)
//
//    println("Starting levels")
//
//    implicit val levelSweeper: CoreTestSweeper = new CoreTestSweeper()
//    implicit val fileSweeper: FileSweeper.On = levelSweeper.fileSweeper
//
//    val level4 = TestLevel(throttle = levelThrottle)
//    val level3 = TestLevel(nextLevel = Some(level4), throttle = levelThrottle)
//    val level2 = TestLevel(nextLevel = Some(level3), throttle = levelThrottle)
//    val level1 = TestLevel(nextLevel = Some(level2), throttle = levelThrottle)
//    val level0 = TestLevelZero(nextLevel = Some(level1), throttle = levelZeroThrottle)
//
//    val compaction: Option[DefActor[Compactor]] =
//      if (throttleOn) {
//        val compactor =
//          CoreInitialiser.initialiseCompaction(
//            zero = level0,
//            compactionConfig =
//              CompactionConfig(
//                resetCompactionPriorityAtInterval = randomIntMax(10).max(1),
//                actorExecutionContext = TestExecutionContext.executionContext,
//                compactionExecutionContext = TestExecutionContext.executionContext,
//                pushStrategy = randomPushStrategy()
//              ),
//          ).value
//
//        Some(compactor)
//      } else {
//        None
//      }
//
//    println("Levels started")
//
//    //start with a default testTimer.
//    val level2KV = level2KeyValues(testTimer)
//    println("level2KV created.")
//
//    //if upper levels should insert key-values at an older time start the testTimer to use older time
//    val level1KV = level1KeyValues(level2KV, testTimer)
//    println("level1KV created.")
//
//    //if upper levels should insert key-values at an older time start the testTimer to use older time
//    val level0KV = level0KeyValues(level1KV, level2KV, testTimer)
//    println("level0KV created.")
//
//    val level0Assert: LevelRef => Unit = assertLevel0(level0KV, level1KV, level2KV, _)
//    val level1Assert: LevelRef => Unit = assertLevel1(level1KV, level2KV, _)
//    val level2Assert: LevelRef => Unit = assertLevel2(level0KV, _)
//    val levelAllAssert: LevelRef => Unit = assertAllLevels(level0KV, level1KV, level2KV, _)
//
//    def runAsserts(asserts: Seq[((Slice[Memory], LevelRef => Unit), (Slice[Memory], LevelRef => Unit), (Slice[Memory], LevelRef => Unit), (Slice[Memory], LevelRef => Unit))]) =
//      asserts.foldLeft(1) {
//        case (count, ((level0KeyValues, level0Assert), (level1KeyValues, level1Assert), (level2KeyValues, level2Assert), (level3KeyValues, level3Assert))) => {
//          println(s"\nRunning assert: $count/${asserts.size} - $iterationMessage")
//          doAssertOnLevel(
//            level0KeyValues = level0KeyValues,
//            assertLevel0 = level0Assert,
//            level0 = level0,
//            level1KeyValues = level1KeyValues,
//            assertLevel1 = level1Assert,
//            level1 = level1,
//            level2KeyValues = level2KeyValues,
//            assertLevel2 = level2Assert,
//            level2 = level2,
//            level3KeyValues = level3KeyValues,
//            assertLevel3 = level3Assert,
//            level3 = level3,
//            assertAllLevels = levelAllAssert,
//            //if level3's key-values are empty - no need to run assert for this pass.
//            assertLevel3ForAllLevels = level3KeyValues.nonEmpty
//          )
//          count + 1
//        }
//      }
//
//    val asserts: Seq[((Slice[Memory], LevelRef => Unit), (Slice[Memory], LevelRef => Unit), (Slice[Memory], LevelRef => Unit), (Slice[Memory], LevelRef => Unit))] =
//      if (throttleOn)
//      //if throttle is only the top most Level's (Level0) assert should
//      // be executed because throttle behaviour is unknown during runtime
//      // and lower Level's key-values would change as compaction continues.
//        (1 to 5) map (
//          i =>
//            if (i == 1)
//              (
//                (level0KV, level0Assert),
//                (level1KV, noAssert),
//                (level2KV, noAssert),
//                (Slice.empty, noAssert)
//              )
//            else
//              (
//                (Slice.empty, level0Assert),
//                (Slice.empty, noAssert),
//                (Slice.empty, noAssert),
//                (Slice.empty, noAssert)
//              )
//          )
//      else
//        Seq(
//          (
//            (level0KV, level0Assert),
//            (level1KV, level1Assert),
//            (level2KV, level2Assert),
//            (Slice.empty, noAssert)
//          ),
//          (
//            (level0KV, level0Assert),
//            (level1KV, level1Assert),
//            (Slice.empty, level2Assert),
//            (level2KV, level2Assert)
//          ),
//          (
//            (level0KV, level0Assert),
//            (Slice.empty, level1Assert),
//            (level1KV, level1Assert),
//            (level2KV, level2Assert)
//          ),
//          (
//            (Slice.empty, level0Assert),
//            (level0KV, level0Assert),
//            (level1KV, level1Assert),
//            (level2KV, level2Assert)
//          ),
//          (
//            (Slice.empty, level0Assert),
//            (Slice.empty, level0Assert),
//            (level0KV, level0Assert),
//            (level1KV, level1Assert)
//          ),
//          (
//            (Slice.empty, level0Assert),
//            (Slice.empty, level0Assert),
//            (Slice.empty, level0Assert),
//            (level0KV, level0Assert)
//          )
//        )
//
//    runAsserts(asserts)
//
//    compaction.foreach(_.terminateAndClear[Glass]())
//
//    level0.delete[Glass]()
//
//    if (!throttleOn)
//      assertLevel(
//        level0KeyValues = level0KeyValues,
//        assertLevel0 = assertLevel0,
//        level1KeyValues = level1KeyValues,
//        assertLevel1 = assertLevel1,
//        level2KeyValues = level2KeyValues,
//        assertLevel2 = assertLevel2,
//        assertAllLevels = assertAllLevels,
//        throttleOn = true
//      )
//  }
//
//  private def doAssertOnLevel(level0KeyValues: Slice[Memory],
//                              assertLevel0: LevelRef => Unit,
//                              level0: LevelZero,
//                              level1KeyValues: Slice[Memory],
//                              assertLevel1: LevelRef => Unit,
//                              level1: Level,
//                              level2KeyValues: Slice[Memory],
//                              assertLevel2: LevelRef => Unit,
//                              level2: Level,
//                              level3KeyValues: Slice[Memory],
//                              assertLevel3: LevelRef => Unit,
//                              level3: Level,
//                              assertAllLevels: LevelRef => Unit,
//                              assertLevel3ForAllLevels: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
//                                                                 levelSweeper: CoreTestSweeper): Unit = {
//    implicit val ec: ExecutionContext =
//      TestExecutionContext.executionContext
//
//    implicit val compactionIOActor: SegmentCompactionIO.Actor =
//      SegmentCompactionIO.create().sweep()
//
//    println("level3.putKeyValues")
//    if (level3KeyValues.nonEmpty) level3.put(level3KeyValues).runRandomIO.get
//    println("level2.putKeyValues")
//    if (level2KeyValues.nonEmpty) level2.put(level2KeyValues).runRandomIO.get
//    println("level1.putKeyValues")
//    if (level1KeyValues.nonEmpty) level1.put(level1KeyValues).runRandomIO.get
//    println("level0.putKeyValues")
//    if (level0KeyValues.nonEmpty) level0.putKeyValues(level0KeyValues).runRandomIO.get
//    import swaydb.testkit.RunThis._
//
//    Seq(
//      () => {
//        println("asserting Level3")
//        assertLevel3(level3)
//      },
//      () => {
//        println("asserting Level2")
//        assertLevel2(level2)
//      },
//      () => {
//        println("asserting Level1")
//        assertLevel1(level1)
//      },
//
//      () => {
//        println("asserting Level0")
//        assertLevel0(level0)
//      },
//      () => {
//        if (assertLevel3ForAllLevels) {
//          println("asserting all on Level3")
//          assertAllLevels(level3)
//        }
//      },
//      () => {
//        println("asserting all on Level2")
//        assertAllLevels(level2)
//      },
//      () => {
//        println("asserting all on Level1")
//        assertAllLevels(level1)
//      },
//
//      () => {
//        println("asserting all on Level0")
//        assertAllLevels(level0)
//      }
//    ).runThisRandomlyInParallel
//  }
//
//}
