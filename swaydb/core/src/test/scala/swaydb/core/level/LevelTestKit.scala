package swaydb.core.level

import swaydb.config.compaction.{LevelMeter, LevelThrottle}
import swaydb.config.storage.{Level0Storage, LevelStorage}
import swaydb.config.{Atomic, MMAP, OptimiseWrites, RecoveryMode}
import swaydb.core.level.zero.LevelZero.LevelZeroLog
import swaydb.core.segment.data.{KeyValue, Memory, SegmentKeyOrders}
import swaydb.core.segment.io.SegmentCompactionIO
import swaydb.core.{TestExecutionContext, TestForceSave, TestSweeper}
import swaydb.effect.Dir
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.utils.OperatingSystem
import swaydb.{Error, Glass, IO}
import swaydb.config.accelerate.Accelerator
import swaydb.core.level.zero.LevelZero
import swaydb.core.segment.{MemorySegment, PersistentSegment, Segment}
import swaydb.slice.Slice
import swaydb.testkit.TestKit.randomBoolean
import swaydb.core.segment.data.KeyValueTestKit._
import TestSweeper._
import swaydb.config.CoreConfigTestKit._
import swaydb.IOValues._
import swaydb.core.log.LogTestKit.SliceKeyValueImplicits
import swaydb.utils.StorageUnits._

object LevelTestKit {

  implicit class ReopenLevel(level: Level)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                           timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long) {

    import swaydb.Error.Level.ExceptionHandler
    import swaydb.testkit.RunThis._

    implicit val keyOrders: SegmentKeyOrders = SegmentKeyOrders(keyOrder)

    //This test function is doing too much. This shouldn't be the case! There needs to be an easier way to write
    //key-values in a Level without that level copying it forward to lower Levels.
    def put(keyValues: Iterable[Memory], removeDeletes: Boolean = false)(implicit sweeper: TestSweeper,
                                                                         compactionActor: SegmentCompactionIO.Actor): IO[Error.Level, Unit] = {

      implicit val idGenerator = level.segmentIDGenerator

      //      def fetchNextPath = {
      //        val segmentId = level.segmentIDGenerator.nextID
      //        val path = level.pathDistributor.next().resolve(IDGenerator.segmentId(segmentId))
      //        (segmentId, path)
      //      }

      implicit val segmentIO = level.segmentIO
      implicit val fileSweeper = level.fileSweeper
      implicit val blockCache = level.blockCacheSweeper
      implicit val bufferCleaner = level.bufferCleaner
      implicit val keyValueSweeper = level.keyValueMemorySweeper
      implicit val forceSaveApplier = level.forceSaveApplier
      implicit val ec = TestExecutionContext.executionContext

      if (keyValues.isEmpty)
        IO.failed("KeyValues are empty")
      else {
        val segments =
          if (level.inMemory)
            MemorySegment(
              keyValues = keyValues.iterator,
              //            fetchNextPath = fetchNextPath,
              pathsDistributor = level.pathDistributor,
              removeDeletes = false,
              minSegmentSize = level.segmentConfig.minSize,
              maxKeyValueCountPerSegment = level.segmentConfig.maxCount,
              createdInLevel = level.levelNumber
            )
          else
            PersistentSegment(
              keyValues = keyValues,
              createdInLevel = level.levelNumber,
              pathsDistributor = level.pathDistributor,
              removeDeletes = false,
              valuesConfig = level.valuesConfig,
              sortedIndexConfig = level.sortedIndexConfig,
              binarySearchIndexConfig = level.binarySearchIndexConfig,
              hashIndexConfig = level.hashIndexConfig,
              bloomFilterConfig = level.bloomFilterConfig,
              segmentConfig = level.segmentConfig //level.segmentConfig.copy(minSize = Int.MaxValue, maxCount = Int.MaxValue)
            ).awaitInf.map(_.sweep())

        //        segments should have size 1

        level.putSegments(segments = segments, removeDeletes = removeDeletes) onRightSideEffect {
          _ =>
            segments.foreach(_.delete())
        }
      }
    }

    def put(segment: Segment)(implicit sweeper: TestSweeper,
                              compactionActor: SegmentCompactionIO.Actor): IO[Error.Level, Unit] =
      putSegments(Seq(segment))

    def putSegments(segments: Iterable[Segment], removeDeletes: Boolean = false)(implicit sweeper: TestSweeper,
                                                                                 compactionActor: SegmentCompactionIO.Actor): IO[Error.Level, Unit] = {
      implicit val ec = TestExecutionContext.executionContext

      if (segments.isEmpty) {
        IO.failed("Segments are empty")
      } else {

        IO(level.assign(segments, level.segments(), removeDeletes)) flatMap {
          assign =>

            IO(level.merge(assign, removeDeletes).awaitInf) flatMap {
              merge =>
                level.commitPersisted(merge)
            }
        }
      }
    }

    def putMap(map: LevelZeroLog)(implicit sweeper: TestSweeper,
                                  compactionActor: SegmentCompactionIO.Actor): IO[Error.Level, Unit] = {
      implicit val ec = TestExecutionContext.executionContext

      if (map.cache.isEmpty) {
        IO.failed("Map is empty")
      } else {
        val removeDeletes = false

        IO(level.assign(newKeyValues = map, targetSegments = level.segments(), removeDeletedRecords = removeDeletes)) flatMap {
          assign =>
            IO(level.merge(assigment = assign, removeDeletedRecords = removeDeletes).awaitInf) flatMap {
              merge =>
                level.commitPersisted(merge)
            }
        }
      }
    }

    def reopen(implicit sweeper: TestSweeper): Level =
      reopen()

    def tryReopen(implicit sweeper: TestSweeper): IO[swaydb.Error.Level, Level] =
      tryReopen()

    def reopen(segmentSize: Int = level.minSegmentSize,
               throttle: LevelMeter => LevelThrottle = level.throttle,
               nextLevel: Option[NextLevel] = level.nextLevel)(implicit sweeper: TestSweeper): Level =
      tryReopen(
        segmentSize = segmentSize,
        throttle = throttle,
        nextLevel = nextLevel
      ).right.value

    def tryReopen(segmentSize: Int = level.minSegmentSize,
                  throttle: LevelMeter => LevelThrottle = level.throttle,
                  nextLevel: Option[NextLevel] = level.nextLevel)(implicit sweeper: TestSweeper): IO[swaydb.Error.Level, Level] = {

      val closeResult =
        if (OperatingSystem.isWindows() && level.hasMMAP)
          IO {
            level.close[Glass]()
          }
        else
          level.closeNoSweep()

      closeResult and {
        import sweeper._

        Level(
          bloomFilterConfig = level.bloomFilterConfig,
          hashIndexConfig = level.hashIndexConfig,
          binarySearchIndexConfig = level.binarySearchIndexConfig,
          sortedIndexConfig = level.sortedIndexConfig,
          valuesConfig = level.valuesConfig,
          segmentConfig = level.segmentConfig.copy(minSize = segmentSize),
          levelStorage =
            LevelStorage.Persistent(
              dir = level.pathDistributor.headPath,
              otherDirs = level.dirs.drop(1).map(dir => Dir(dir.path, 1)),
              appendixMMAP = MMAP.randomForLog(),
              appendixFlushCheckpointSize = 4.mb
            ),
          nextLevel = nextLevel,
          throttle = throttle
        ).map(_.sweep())
      }
    }
  }

  implicit class ReopenLevelZero(level: LevelZero)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default) {

    import swaydb.core.log.serialiser.LevelZeroLogEntryWriter._

    def reopen(implicit sweeper: TestSweeper): LevelZero =
      reopen()

    def reopen(logSize: Int = level.logs.log.fileSize,
               appliedFunctionsLogSize: Int = level.appliedFunctionsLog.map(_.fileSize).getOrElse(0),
               clearAppliedFunctionsOnBoot: Boolean = false)(implicit timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                             sweeper: TestSweeper): LevelZero = {

      if (OperatingSystem.isWindows() && level.hasMMAP)
        level.close[Glass]()

      val reopened =
        level.releaseLocks flatMap {
          _ =>
            level.closeSegments() flatMap {
              _ =>
                import sweeper._

                implicit val optimiseWrites = OptimiseWrites.random
                implicit val atomic: Atomic = Atomic.random

                LevelZero(
                  logSize = logSize,
                  appliedFunctionsLogSize = appliedFunctionsLogSize,
                  clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
                  storage =
                    Level0Storage.Persistent(
                      mmap = MMAP.on(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap()),
                      dir = level.path.getParent,
                      recovery = RecoveryMode.ReportFailure
                    ),
                  enableTimer = true,
                  cacheKeyValueIds = randomBoolean(),
                  nextLevel = level.nextLevel,
                  acceleration = Accelerator.brake(),
                  throttle = level.throttle
                ).map(_.sweep())
            }
        }
      reopened.value
    }

    def putKeyValues(keyValues: Iterable[KeyValue]): IO[swaydb.Error.Level, Unit] =
      if (keyValues.isEmpty)
        IO.unit
      else
        keyValues.toLogEntry match {
          case Some(value) =>
            IO[swaydb.Error.Level, Unit] {
              level
                .put(_ => value)
            }

          case None =>
            IO.unit
        }
  }

}
