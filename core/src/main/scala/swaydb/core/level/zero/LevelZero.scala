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

package swaydb.core.level.zero

import com.typesafe.scalalogging.LazyLogging
import swaydb.Bag.Implicits._
import swaydb.Error.Level.ExceptionHandler
import swaydb.Exception.FunctionNotFound
import swaydb.core.data.KeyValue.{Put, PutOption}
import swaydb.core.data.Value.FromValue
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.level.seek._
import swaydb.core.level.zero.LevelZero.LevelZeroLog
import swaydb.core.level.{LevelRef, LevelSeek, NextLevel}
import swaydb.core.log.applied.{AppliedFunctionsLog, AppliedFunctionsLogCache}
import swaydb.core.log.serializer.AppliedFunctionsLogEntryWriter
import swaydb.core.log.timer.Timer
import swaydb.core.log.{Log, LogEntry, Logs}
import swaydb.core.segment.entry.reader.PersistentReader
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.FileSweeper
import swaydb.skiplist.SkipList
import swaydb.core.util.MinMax
import swaydb.core.{CoreState, MemoryPathGenerator}
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, LevelZeroThrottle}
import swaydb.data.config.MMAP
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.{Slice, SliceOption}
import swaydb.data.storage.Level0Storage
import swaydb.data.{Atomic, OptimiseWrites}
import swaydb.effect.{Effect, FileLocker}
import swaydb.utils.{DropIterator, Options}
import swaydb.{Bag, Error, Glass, IO, OK}

import java.nio.channels.FileChannel
import java.nio.file.{Path, StandardOpenOption}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

private[core] case object LevelZero extends LazyLogging {

  type LevelZeroLog = Log[Slice[Byte], Memory, LevelZeroLogCache]

  def apply(logSize: Int,
            appliedFunctionsLogSize: Int,
            clearAppliedFunctionsOnBoot: Boolean,
            storage: Level0Storage,
            enableTimer: Boolean,
            cacheKeyValueIds: Boolean,
            coreState: CoreState,
            nextLevel: Option[NextLevel],
            acceleration: LevelZeroMeter => Accelerator,
            throttle: LevelZeroMeter => LevelZeroThrottle)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                           timeOrder: TimeOrder[Slice[Byte]],
                                                           bufferCleaner: ByteBufferSweeperActor,
                                                           functionStore: FunctionStore,
                                                           forceSaveApplier: ForceSaveApplier,
                                                           optimiseWrites: OptimiseWrites,
                                                           atomic: Atomic): IO[swaydb.Error.Level, LevelZero] = {
    import swaydb.core.log.serializer.LevelZeroLogEntryReader.Level0Reader
    import swaydb.core.log.serializer.LevelZeroLogEntryWriter._

    if (cacheKeyValueIds)
      PersistentReader.populateBaseEntryIds()
    else
      logger.info("cacheKeyValueIds is false. Key-value IDs cache disabled!")

    val logsAndPathAndLock =
      storage match {
        case Level0Storage.Persistent(mmap, databaseDirectory, recovery) =>

          val timer =
            if (enableTimer)
              Timer.persistent(
                path = databaseDirectory,
                mmap = mmap
              )
            else
              IO.Right(Timer.empty)

          timer flatMap {
            implicit timer =>
              val levelZeroDirectory = databaseDirectory.resolve("0")

              logger.info("{}: Acquiring lock.", levelZeroDirectory)
              val lockFile = levelZeroDirectory.resolve("LOCK")
              Effect createDirectoriesIfAbsent levelZeroDirectory
              Effect createFileIfAbsent lockFile

              val lockChannel = IO(FileChannel.open(lockFile, StandardOpenOption.WRITE))

              val levelLock =
                lockChannel map {
                  channel =>
                    val lock = channel.tryLock()
                    FileLocker(lock, channel)
                }

              levelLock flatMap {
                lock =>
                  //LevelZero does not required FileSweeper since they are all Log files.
                  implicit val fileSweeper: FileSweeper = FileSweeper.Off
                  logger.info("{}: Recovering logs.", levelZeroDirectory)

                  val logs =
                    Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                      path = levelZeroDirectory,
                      mmap = mmap,
                      fileSize = logSize,
                      acceleration = acceleration,
                      recovery = recovery
                    ).onLeftSideEffect {
                      _ =>
                        timer.close
                    }

                  logs flatMap {
                    logs =>
                      if (enableTimer) {
                        val appliedFunctionsLog =
                          AppliedFunctionsLog(
                            dir = databaseDirectory,
                            fileSize = appliedFunctionsLogSize,
                            mmap = mmap
                          )

                        appliedFunctionsLog
                          .result
                          .andThen((logs, Some(appliedFunctionsLog.item), levelZeroDirectory, Some(lock)))
                          .onLeftSideEffect {
                            _ =>
                              timer.close
                              appliedFunctionsLog.item.close()
                              logs.close().get
                          }

                      } else {
                        IO.Right((logs, None, levelZeroDirectory, Some(lock)))
                      }
                  }
              } onLeftSideEffect {
                _ =>
                  timer.close
              }
          }

        case Level0Storage.Memory =>
          val timer =
            if (enableTimer)
              LevelRef.firstPersistentLevel(nextLevel).map(_.rootPath) match {
                case Some(persistentPath) =>
                  val databaseDirectory = persistentPath.getParent

                  val mmap = LevelRef.getMmapForLogOrDisable(nextLevel)

                  val appliedFunctionsLog =
                    AppliedFunctionsLog(
                      dir = databaseDirectory,
                      fileSize = appliedFunctionsLogSize,
                      mmap = mmap
                    )

                  appliedFunctionsLog
                    .result
                    .and {
                      Timer.persistent(
                        path = databaseDirectory,
                        mmap = mmap
                      )
                    }
                    .map {
                      timer =>
                        (timer, Some(appliedFunctionsLog.item))
                    }
                    .onLeftSideEffect {
                      _ =>
                        appliedFunctionsLog.item.close()
                    }

                case None =>
                  IO.Right((Timer.memory(), None))
              }
            else
              IO.Right((Timer.empty, None))

          timer map {
            case (timer, appliedFunctionsLog) =>
              //LevelZero does not required FileSweeper since they are all Log files.
              implicit val fileSweeper: FileSweeper = FileSweeper.Off
              implicit val implicitTimer: Timer = timer

              val log =
                Logs.memory[Slice[Byte], Memory, LevelZeroLogCache](
                  fileSize = logSize,
                  acceleration = acceleration
                )

              val path = nextLevel.map(_.rootPath.getParent).getOrElse(MemoryPathGenerator.next())
              (log, appliedFunctionsLog, path.resolve(0.toString), None)
          }
      }

    logsAndPathAndLock
      .flatMap {
        case (logs, appliedFunctions, path, lock: Option[FileLocker]) =>
          val zero =
            new LevelZero(
              path = path,
              logSize = logSize,
              logs = logs,
              nextLevel = nextLevel,
              inMemory = storage.memory,
              throttle = throttle,
              appliedFunctionsLog = appliedFunctions,
              coreState = coreState,
              lock = lock
            )

          appliedFunctions match {
            case Some(appliedFunctions) =>
              if (clearAppliedFunctionsOnBoot)
                IO(zero.clearAppliedFunctions())
                  .and(AppliedFunctionsLog.validate(appliedFunctions, functionStore))
                  .andThen(zero)
                  .onLeftSideEffect(_ => zero.close[Glass]())
              else
                AppliedFunctionsLog
                  .validate(appliedFunctions, functionStore)
                  .andThen(zero)
                  .onLeftSideEffect(_ => zero.close[Glass]())

            case None =>
              IO.Right(zero)
          }
      }
  }
}

private[swaydb] case class LevelZero(path: Path,
                                     logSize: Int,
                                     logs: Logs[Slice[Byte], Memory, LevelZeroLogCache],
                                     nextLevel: Option[NextLevel],
                                     inMemory: Boolean,
                                     throttle: LevelZeroMeter => LevelZeroThrottle,
                                     appliedFunctionsLog: Option[Log[Slice[Byte], Slice.Null.type, AppliedFunctionsLogCache]],
                                     coreState: CoreState,
                                     private val lock: Option[FileLocker])(implicit val keyOrder: KeyOrder[Slice[Byte]],
                                                                           val timeOrder: TimeOrder[Slice[Byte]],
                                                                           val functionStore: FunctionStore) extends LevelRef with LazyLogging {

  logger.info("{}: Level0 started.", path)

  import keyOrder._
  import swaydb.core.log.serializer.LevelZeroLogEntryWriter._

  val levelZeroMeter: LevelZeroMeter =
    logs.meter

  def onNextLogCallback(event: () => Unit): Unit =
    logs onNextLogCallback event

  def releaseLocks: IO[swaydb.Error.Close, Unit] =
    IO[swaydb.Error.Close, Unit](Effect.release(lock))
      .and(nextLevel.map(_.releaseLocks) getOrElse IO.unit)

  def validateInput(key: Slice[Byte]): Unit =
    if (key.isEmpty) {
      throw new IllegalArgumentException("key cannot be empty.")
    }

  def validateInput(fromKey: Slice[Byte], toKey: Slice[Byte]): Unit =
    if (fromKey.isEmpty)
      throw new IllegalArgumentException("fromKey cannot be empty.")
    else if (toKey.isEmpty)
      throw new IllegalArgumentException("toKey cannot be empty.")
    else if (fromKey > toKey) //fromKey cannot also be equal to toKey. The invoking this assert should also check for equality and call update on single key-value.
      throw new IllegalArgumentException("fromKey should be less than toKey.")

  def put(key: Slice[Byte]): OK = {
    validateInput(key)
    logs.write(timer => LogEntry.Put[Slice[Byte], Memory.Put](key, Memory.Put(key, Slice.Null, None, timer.next)))
    OK.instance
  }

  def put(key: Slice[Byte], value: Slice[Byte]): OK = {
    validateInput(key)
    logs.write(timer => LogEntry.Put[Slice[Byte], Memory.Put](key, Memory.Put(key, value, None, timer.next)))
    OK.instance
  }

  def put(key: Slice[Byte], value: SliceOption[Byte], removeAt: Deadline): OK = {
    validateInput(key)
    logs.write(timer => LogEntry.Put[Slice[Byte], Memory.Put](key, Memory.Put(key, value, Some(removeAt), timer.next)))
    OK.instance
  }

  def put(key: Slice[Byte], value: SliceOption[Byte]): OK = {
    validateInput(key)
    logs.write(timer => LogEntry.Put[Slice[Byte], Memory.Put](key, Memory.Put(key, value, None, timer.next)))
    OK.instance
  }

  def put(entry: Timer => LogEntry[Slice[Byte], Memory]): OK = {
    logs write entry
    OK.instance
  }

  def remove(key: Slice[Byte]): OK = {
    validateInput(key)
    logs.write(timer => LogEntry.Put[Slice[Byte], Memory.Remove](key, Memory.Remove(key, None, timer.next)))
    OK.instance
  }

  def remove(key: Slice[Byte], at: Deadline): OK = {
    validateInput(key)
    logs.write(timer => LogEntry.Put[Slice[Byte], Memory.Remove](key, Memory.Remove(key, Some(at), timer.next)))
    OK.instance
  }

  def remove(fromKey: Slice[Byte], toKey: Slice[Byte]): OK = {
    validateInput(fromKey, toKey)

    if (fromKey equiv toKey)
      remove(fromKey)
    else
      logs
        .write {
          timer =>
            (LogEntry.Put[Slice[Byte], Memory.Range](fromKey, Memory.Range(fromKey, toKey, Value.FromValue.Null, Value.Remove(None, timer.next))): LogEntry[Slice[Byte], Memory]) ++
              LogEntry.Put[Slice[Byte], Memory.Remove](toKey, Memory.Remove(toKey, None, timer.next))
        }

    OK.instance
  }

  def remove(fromKey: Slice[Byte], toKey: Slice[Byte], at: Deadline): OK = {
    validateInput(fromKey, toKey)

    if (fromKey equiv toKey)
      remove(fromKey)
    else
      logs
        .write {
          timer =>
            (LogEntry.Put[Slice[Byte], Memory.Range](fromKey, Memory.Range(fromKey, toKey, Value.FromValue.Null, Value.Remove(Some(at), timer.next))): LogEntry[Slice[Byte], Memory]) ++
              LogEntry.Put[Slice[Byte], Memory.Remove](toKey, Memory.Remove(toKey, Some(at), timer.next))
        }

    OK.instance
  }

  def update(key: Slice[Byte], value: Slice[Byte]): OK = {
    validateInput(key)
    logs.write(timer => LogEntry.Put[Slice[Byte], Memory.Update](key, Memory.Update(key, value, None, timer.next)))
    OK.instance
  }

  def update(key: Slice[Byte], value: SliceOption[Byte]): OK = {
    validateInput(key)
    logs.write(timer => LogEntry.Put[Slice[Byte], Memory.Update](key, Memory.Update(key, value, None, timer.next)))
    OK.instance
  }

  def update(fromKey: Slice[Byte], toKey: Slice[Byte], value: SliceOption[Byte]): OK = {
    validateInput(fromKey, toKey)

    if (fromKey equiv toKey)
      update(fromKey, value)
    else
      logs
        .write {
          timer =>
            (LogEntry.Put[Slice[Byte], Memory.Range](
              key = fromKey,
              value = Memory.Range(
                fromKey = fromKey,
                toKey = toKey,
                fromValue = Value.FromValue.Null,
                rangeValue = Value.Update(value, None, timer.next)
              )
            ): LogEntry[Slice[Byte], Memory]) ++ LogEntry.Put[Slice[Byte], Memory.Update](toKey, Memory.Update(toKey, value, None, timer.next))
        }

    OK.instance
  }

  def clearAppliedFunctions(): ListBuffer[String] =
    appliedFunctionsLog match {
      case Some(appliedFunctions) =>
        val totalAppliedFunctions = appliedFunctions.cache.maxKeyValueCount
        logger.info(s"Checking for applied functions to clear from $totalAppliedFunctions registered functions.")

        //used for logging progress information
        var progress = 0

        val cleared =
          appliedFunctions.cache.skipList.foldLeft(ListBuffer.empty[String]) {
            case (cleaned, (functionId, _)) =>
              progress += 1

              if (progress % 100 == 0)
                logger.info(s"Checked $progress/$totalAppliedFunctions applied functions. Removed ${cleaned.size}.")

              if (!this.mightContainFunction(functionId)) {
                appliedFunctions.writeSync(LogEntry.Remove(functionId)(AppliedFunctionsLogEntryWriter.FunctionsRemoveLogEntryWriter))
                cleaned += functionId.readString()
                cleaned
              } else {
                cleaned
              }
          }

        logger.info(s"${cleared.size} applied functions.")
        cleared

      case None =>
        ListBuffer.empty
    }

  def clearAppliedAndRegisteredFunctions(): ListBuffer[String] = {
    val clearedFunctions = clearAppliedFunctions()
    val totalClearedFunctions = clearedFunctions.size

    val totalAppliedFunctions = functionStore.size
    logger.info(s"Checking for applied registered functions to clear from $totalAppliedFunctions registered functions.")

    //vars used for logging progress information
    var progress = 0
    var registeredFunctionsCleared = 0

    clearedFunctions foreach {
      removedFunctions =>
        functionStore.remove(Slice.writeString[Byte](removedFunctions))
    }

    val cleared =
      functionStore.asScala.foldLeft(clearedFunctions) {
        case (totalCleaned, (functionId, _)) =>
          progress += 1

          if (progress % 100 == 0)
            logger.info(s"Checked $progress/$totalAppliedFunctions applied functions. Removed ${totalCleaned.size}.")

          if (!this.mightContainFunction(functionId)) {
            functionStore.remove(functionId)
            registeredFunctionsCleared += 1
            totalCleaned += functionId.readString()
          } else {
            totalCleaned
          }
      }

    logger.debug(s"Cleared total ${cleared.size} functions. $totalClearedFunctions applied functions and $registeredFunctionsCleared registered functions.")
    cleared
  }

  def isFunctionApplied(functionId: Slice[Byte]): Boolean =
    appliedFunctionsLog.exists(_.cache.skipList.contains(functionId))

  def clear(readState: ThreadReadState): OK =
    headKey(readState) match {
      case headKey: Slice[Byte] =>
        lastKey(readState) match {
          case lastKey: Slice[Byte] =>
            remove(headKey, lastKey)

          case Slice.Null =>
            OK.instance //might have been removed by another thread?
        }

      case Slice.Null =>
        OK.instance
    }

  def registerFunction(functionId: Slice[Byte], function: SwayFunction): OK =
    functionStore.put(functionId, function)

  private def saveAppliedFunctionNoSync(function: Slice[Byte]): Unit =
    appliedFunctionsLog foreach {
      appliedFunctions =>
        if (appliedFunctions.cache.skipList.notContains(function)) {
          //writeNoSync because this already because this functions is already being
          //called un log.write which is a sync function.
          appliedFunctions.writeNoSync(LogEntry.Put(function, Slice.Null)(AppliedFunctionsLogEntryWriter.FunctionsPutLogEntryWriter))
        }
    }

  def applyFunction(key: Slice[Byte], function: Slice[Byte]): OK =
    if (functionStore.notContains(function)) {
      throw FunctionNotFound(function.readString())
    } else {
      validateInput(key)

      logs.write {
        timer =>
          if (timer.isEmptyTimer) {
            throw new IllegalArgumentException("Functions are disabled.")
          } else {
            saveAppliedFunctionNoSync(function)
            LogEntry.Put[Slice[Byte], Memory.Function](key, Memory.Function(key, function, timer.next))
          }
      }

      OK.instance
    }

  def applyFunction(fromKey: Slice[Byte], toKey: Slice[Byte], function: Slice[Byte]): OK =
    if (functionStore.notContains(function)) {
      throw FunctionNotFound(function.readString())
    } else {
      validateInput(fromKey, toKey)

      if (fromKey equiv toKey)
        applyFunction(fromKey, function)
      else
        logs.write {
          timer =>
            if (timer.isEmptyTimer) {
              throw new IllegalArgumentException("Functions are disabled.")
            } else {
              saveAppliedFunctionNoSync(function)

              (LogEntry.Put[Slice[Byte], Memory.Range](fromKey, Memory.Range(fromKey, toKey, Value.FromValue.Null, Value.Function(function, timer.next))): LogEntry[Slice[Byte], Memory]) ++
                LogEntry.Put[Slice[Byte], Memory.Function](toKey, Memory.Function(toKey, function, timer.next))
            }
        }

      OK.instance
    }

  private def getFromLog(key: Slice[Byte],
                         theLog: LevelZeroLog): MemoryOption =
    if (theLog.cache.hasRange)
      theLog.cache.floorOptimised(key) match {
        case floor: Memory.Fixed if floor.key equiv key =>
          floor

        case floor: Memory.Range if key < floor.toKey =>
          floor

        case _ =>
          Memory.Null
      }
    else
      theLog.cache.getOptimised(key)

  private def getFromNextLevel(key: Slice[Byte],
                               readState: ThreadReadState,
                               tailLogs: DropIterator.Flat[Null, LevelZeroLog]): KeyValue.PutOption = {
    val headOrNull = tailLogs.headOrNull

    if (headOrNull == null)
      nextLevel match {
        case Some(nextLevel) =>
          nextLevel.get(key, readState)

        case None =>
          KeyValue.Put.Null
      }
    else
      find(
        key = key,
        readState = readState,
        headLog = headOrNull,
        tailLogs = tailLogs.dropHeadDuplicate()
      )
  }

  def currentGetter(theLog: LevelZeroLog) =
    new CurrentGetter {
      override def get(key: Slice[Byte], readState: ThreadReadState): MemoryOption =
        getFromLog(key, theLog)
    }

  def nextGetter(tailLogs: DropIterator.Flat[Null, LevelZeroLog]) =
    new NextGetter {
      override def get(key: Slice[Byte], readState: ThreadReadState): KeyValue.PutOption =
        getFromNextLevel(key, readState, tailLogs)
    }

  private def find(key: Slice[Byte],
                   readState: ThreadReadState,
                   headLog: LevelZeroLog,
                   tailLogs: DropIterator.Flat[Null, LevelZeroLog]): KeyValue.PutOption =
    Get.seek(
      key = key,
      readState = readState,
      currentGetter = currentGetter(headLog),
      nextGetter = nextGetter(tailLogs)
    )

  def get(key: Slice[Byte],
          readState: ThreadReadState): KeyValue.PutOption = {
    val iterator = logs.readDropIterator()

    find(
      key = key,
      readState = readState,
      headLog = iterator.headOrNull,
      tailLogs = iterator.dropHeadDuplicate()
    )
  }

  def getKey(key: Slice[Byte],
             readState: ThreadReadState): SliceOption[Byte] =
    get(key, readState).mapSliceOptional(_.key)

  def firstKeyFromLogs: SliceOption[Byte] =
    logs.reduce[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory], SliceOption[Byte]](
      nullResult = Slice.Null,
      applier = _.cache.headKeyOptimised,
      reducer = MinMax.minFavourLeftC[SliceOption[Byte], Slice[Byte]](_, _)(keyOrder)
    )

  def lastKeyFromLogs: SliceOption[Byte] =
    logs.reduce[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory], SliceOption[Byte]](
      nullResult = Slice.Null,
      applier =
        log =>
          log
            .cache
            .lastOptimised
            .flatMapSomeS(Slice.Null: SliceOption[Byte]) {
              case fixed: KeyValue.Fixed =>
                fixed.key

              case range: KeyValue.Range =>
                range.toKey
            },
      reducer = MinMax.maxFavourLeftC[SliceOption[Byte], Slice[Byte]](_, _)(keyOrder)
    )

  def lastKey(readState: ThreadReadState): SliceOption[Byte] =
    last(readState).mapSliceOptional(_.key)

  override def headKey(readState: ThreadReadState): SliceOption[Byte] =
    head(readState).mapSliceOptional(_.key)

  def head(readState: ThreadReadState): KeyValue.PutOption = {
    //read from LevelZero first
    val logHead = firstKeyFromLogs

    nextLevel match {
      case Some(nextLevel) =>
        nextLevel.headKey(readState) match {
          case nextLevelFirstKey: Slice[Byte] =>
            MinMax
              .minFavourLeftC[SliceOption[Byte], Slice[Byte]](logHead, nextLevelFirstKey)(keyOrder)
              .flatMapSomeC(KeyValue.Put.Null: KeyValue.PutOption)(ceiling(_, readState))

          case Slice.Null =>
            logHead.flatMapSomeC(KeyValue.Put.Null: KeyValue.PutOption)(ceiling(_, readState))
        }

      case None =>
        logHead.flatMapSomeC(KeyValue.Put.Null: KeyValue.PutOption)(ceiling(_, readState))
    }
  }

  def last(readState: ThreadReadState): KeyValue.PutOption = {
    //read from LevelZero first
    val logLast = lastKeyFromLogs

    nextLevel match {
      case Some(nextLevel) =>
        nextLevel.lastKey(readState) match {
          case nextLevelLastKey: Slice[Byte] =>
            MinMax
              .maxFavourLeftC[SliceOption[Byte], Slice[Byte]](logLast, nextLevelLastKey)(keyOrder)
              .flatMapSomeC(KeyValue.Put.Null: KeyValue.PutOption)(floor(_, readState))

          case Slice.Null =>
            logLast.flatMapSomeC(KeyValue.Put.Null: KeyValue.PutOption)(floor(_, readState))
        }

      case None =>
        logLast.flatMapSomeC(KeyValue.Put.Null: KeyValue.PutOption)(floor(_, readState))
    }
  }

  def ceiling(key: Slice[Byte],
              readState: ThreadReadState): KeyValue.PutOption = {
    val iterator = logs.readDropIterator()

    ceiling(
      key = key,
      readState = readState,
      headLog = iterator.headOrNull,
      tailLogs = iterator.dropHeadDuplicate()
    )
  }

  def ceiling(key: Slice[Byte],
              readState: ThreadReadState,
              headLog: LevelZeroLog,
              tailLogs: DropIterator.Flat[Null, LevelZeroLog]): KeyValue.PutOption = {
    val (left, right) = tailLogs.duplicate()

    find(
      key = key,
      readState = readState,
      headLog = headLog,
      tailLogs = left
    ) orElse {
      findHigher(
        key = key,
        readState = readState,
        currentLog = headLog,
        tailLogs = right
      )
    }
  }

  def floor(key: Slice[Byte],
            readState: ThreadReadState): KeyValue.PutOption = {
    val iterator = logs.readDropIterator()

    floor(
      key = key,
      readState = readState,
      headLog = iterator.headOrNull,
      tailLogs = iterator.dropHeadDuplicate()
    )
  }

  def floor(key: Slice[Byte],
            readState: ThreadReadState,
            headLog: LevelZeroLog,
            tailLogs: DropIterator.Flat[Null, LevelZeroLog]): KeyValue.PutOption = {
    val (left, right) = tailLogs.duplicate()

    find(
      key = key,
      readState = readState,
      headLog = headLog,
      tailLogs = left
    ) orElse {
      findLower(
        key = key,
        readState = readState,
        currentLog = headLog,
        tailLogs = right
      )
    }
  }

  private def higherFromLog(key: Slice[Byte],
                            theLog: LevelZeroLog): MemoryOption =
    if (theLog.cache.hasRange)
      theLog.cache.floorOptimised(key) match {
        case floorRange: Memory.Range if key >= floorRange.fromKey && key < floorRange.toKey =>
          floorRange

        case _ =>
          theLog.cache.higherOptimised(key)
      }
    else
      theLog.cache.higherOptimised(key)

  def findHigherInNextLevel(key: Slice[Byte],
                            readState: ThreadReadState,
                            tailLogs: DropIterator.Flat[Null, LevelZeroLog]): KeyValue.PutOption = {
    val nextLogOrNull = tailLogs.headOrNull

    if (nextLogOrNull == null)
      nextLevel match {
        case Some(nextLevel) =>
          nextLevel.higher(key, readState)

        case None =>
          KeyValue.Put.Null
      }
    else
      findHigher(
        key = key,
        readState = readState,
        currentLog = nextLogOrNull,
        tailLogs = tailLogs.dropHeadDuplicate()
      )
  }

  def currentWalker(currentLog: LevelZeroLog,
                    tailLogs: DropIterator.Flat[Null, LevelZeroLog]) =
    new CurrentWalker {
      override def get(key: Slice[Byte], readState: ThreadReadState): KeyValue.PutOption =
        find(
          key = key,
          readState = readState,
          headLog = currentLog,
          tailLogs = tailLogs
        )

      override def higher(key: Slice[Byte], readState: ThreadReadState): LevelSeek[Memory] =
        LevelSeek(
          segmentNumber = 0,
          result = higherFromLog(key, currentLog).toOptionS
        )

      override def lower(key: Slice[Byte], readState: ThreadReadState): LevelSeek[Memory] =
        LevelSeek(
          segmentNumber = 0,
          result = lowerFromLog(key, currentLog).toOptionS
        )

      override def levelNumber: String =
        "current"
    }

  def nextWalker(tailLogs: DropIterator.Flat[Null, LevelZeroLog]) =
    new NextWalker {
      override def higher(key: Slice[Byte],
                          readState: ThreadReadState): KeyValue.PutOption =
        findHigherInNextLevel(key, readState, tailLogs)

      override def lower(key: Slice[Byte],
                         readState: ThreadReadState): KeyValue.PutOption =
        findLowerInNextLevel(key, readState, tailLogs)

      override def get(key: Slice[Byte],
                       readState: ThreadReadState): KeyValue.PutOption =
        getFromNextLevel(key, readState, tailLogs)

      override def levelNumber: String =
        s"Log."
    }

  def findHigher(key: Slice[Byte],
                 readState: ThreadReadState,
                 currentLog: LevelZeroLog,
                 tailLogs: DropIterator.Flat[Null, LevelZeroLog]): KeyValue.PutOption =
    Higher.seek(
      key = key,
      readState = readState,
      currentSeek = Seek.Current.Read(Int.MinValue),
      nextSeek = Seek.Next.Read,
      currentWalker = currentWalker(currentLog, tailLogs),
      nextWalker = nextWalker(tailLogs),
      keyOrder = keyOrder,
      timeOrder = timeOrder,
      functionStore = functionStore
    )

  /**
   * Higher cannot use an iterator because a single Log can value read requests multiple times for cases where a Log contains a range
   * to fetch ceiling key.
   *
   * Higher queries require iteration of all logs anyway so a full initial conversion to a List is acceptable.
   */
  def higher(key: Slice[Byte],
             readState: ThreadReadState): KeyValue.PutOption = {
    val iterator = logs.readDropIterator()

    findHigher(
      key = key,
      readState = readState,
      currentLog = iterator.headOrNull,
      tailLogs = iterator.dropHeadDuplicate()
    )
  }

  private def lowerFromLog(key: Slice[Byte],
                           theLog: LevelZeroLog): MemoryOption =
    if (theLog.cache.hasRange)
      theLog.cache.floorOptimised(key) match {
        case floorRange: Memory.Range if key > floorRange.fromKey && key <= floorRange.toKey =>
          floorRange

        case _ =>
          theLog.cache.lowerOptimised(key)
      }
    else
      theLog.cache.lowerOptimised(key)

  def findLowerInNextLevel(key: Slice[Byte],
                           readState: ThreadReadState,
                           tailLogs: DropIterator.Flat[Null, LevelZeroLog]): KeyValue.PutOption = {
    val nextLogOrNull = tailLogs.headOrNull

    if (nextLogOrNull == null)
      nextLevel match {
        case Some(nextLevel) =>
          nextLevel.lower(key, readState)

        case None =>
          KeyValue.Put.Null
      }
    else
      findLower(
        key = key,
        readState = readState,
        currentLog = nextLogOrNull,
        tailLogs = tailLogs.dropHeadDuplicate()
      )
  }

  def findLower(key: Slice[Byte],
                readState: ThreadReadState,
                currentLog: LevelZeroLog,
                tailLogs: DropIterator.Flat[Null, LevelZeroLog]): KeyValue.PutOption =
    Lower.seek(
      key = key,
      readState = readState,
      currentSeek = Seek.Current.Read(Int.MinValue),
      nextSeek = Seek.Next.Read,
      currentWalker = currentWalker(currentLog, tailLogs),
      nextWalker = nextWalker(tailLogs),
      keyOrder = keyOrder,
      timeOrder = timeOrder,
      functionStore = functionStore
    )

  /**
   * Lower cannot use an iterator because a single Log can value read requests multiple times for cases where a Log contains a range
   * to fetch ceiling key.
   *
   * Lower queries require iteration of all logs anyway so a full initial conversion to a List is acceptable.
   */
  def lower(key: Slice[Byte],
            readState: ThreadReadState): KeyValue.PutOption = {
    val iterator = logs.readDropIterator()

    findLower(
      key = key,
      readState = readState,
      currentLog = iterator.headOrNull,
      tailLogs = iterator.dropHeadDuplicate()
    )
  }

  def contains(key: Slice[Byte],
               readState: ThreadReadState): Boolean =
    get(key, readState).isSome

  def valueSize(key: Slice[Byte],
                readState: ThreadReadState): Option[Int] =
    get(key, readState) match {
      case Put.Null =>
        Options.zero

      case put: Put =>
        Some(put.valueLength)
    }

  def keyValueCount: Int = {
    val keyValueCountInLogs = logs.keyValueCount
    nextLevel match {
      case Some(nextLevel) =>
        nextLevel.keyValueCount + keyValueCountInLogs

      case None =>
        keyValueCountInLogs
    }
  }

  def deadline(key: Slice[Byte],
               readState: ThreadReadState): Option[Deadline] =
    get(
      key = key,
      readState = readState
    ).flatMapOption(_.deadline)

  def sizeOfSegments: Long =
    nextLevel match {
      case Some(nextLevel) =>
        nextLevel.sizeOfSegments

      case None =>
        0L
    }

  override def blockCacheSize(): Option[Long] =
    nextLevel.flatMap(_.blockCacheSize())

  override def cachedKeyValuesSize(): Option[Long] =
    nextLevel.flatMap(_.cachedKeyValuesSize())

  def openedFiles(): Option[Long] =
    nextLevel.map(_.openedFiles())

  def pendingDeletes(): Option[Long] =
    nextLevel.map(_.pendingDeletes())

  def existsOnDisk: Boolean =
    Effect.exists(path)

  def mightContainKey(key: Slice[Byte], threadState: ThreadReadState): Boolean = {
    val mightContainInLogs =
      logs.find[MemoryOption](
        nullResult = Memory.Null,
        matcher = _.cache.getOptimised(key)
      )

    mightContainInLogs != Memory.Null || nextLevel.exists(_.mightContainKey(key, threadState))
  }

  private def findFunctionInLogs(functionId: Slice[Byte]): Boolean =
    logs.find[Boolean](
      nullResult = false,
      matcher =
        log =>
          log
            .cache
            .skipList //functions don't need atomic reads so it can access the skipList directly.
            .values()
            .exists {
              case _: Memory.Put | _: Memory.Remove | _: Memory.Update =>
                false

              case function: Memory.Function =>
                function.function equiv functionId

              case pendingApply: Memory.PendingApply =>
                FunctionStore.containsFunction(functionId, pendingApply.applies)

              case range: Memory.Range =>
                val values =
                  range.fromValue match {
                    case FromValue.Null =>
                      Slice(range.rangeValue)

                    case fromValue: Value.FromValue =>
                      Slice(range.rangeValue, fromValue)
                  }

                FunctionStore.containsFunction(functionId, values)
            }
    )

  def mightContainFunction(functionId: Slice[Byte]): Boolean =
    findFunctionInLogs(functionId) ||
      nextLevel.exists(_.mightContainFunction(functionId))

  override def hasNextLevel: Boolean =
    nextLevel.isDefined

  override def appendixPath: Path =
    nextLevel match {
      case Some(nextLevel) =>
        nextLevel.appendixPath

      case None =>
        throw new Exception("LevelZero does not have appendix.")
    }

  override def rootPath: Path =
    path

  override def isEmpty: Boolean =
    logs.isEmpty

  override def segmentsCount(): Int =
    nextLevel
      .map(_.segmentsCount())
      .getOrElse(0)

  override def segmentFilesOnDisk: Seq[Path] =
    nextLevel
      .map(_.segmentFilesOnDisk)
      .getOrElse(Seq.empty)

  override def foreachSegment[T](f: (Slice[Byte], Segment) => T): Unit =
    nextLevel.foreach(_.foreachSegment(f))

  override def containsSegmentWithMinKey(minKey: Slice[Byte]): Boolean =
    nextLevel.exists(_.containsSegmentWithMinKey(minKey))

  override def getSegment(minKey: Slice[Byte]): SegmentOption =
    nextLevel match {
      case Some(segment) =>
        segment.getSegment(minKey)

      case None =>
        Segment.Null
    }

  override def meterFor(levelNumber: Int): Option[LevelMeter] =
    nextLevel.flatMap(_.meterFor(levelNumber))

  override def levelNumber: Int = 0

  override def isZero: Boolean = true

  override def stateId: Long =
    logs.stateId

  override def nextCompactionDelay: FiniteDuration =
    throttle(levelZeroMeter).compactionDelay

  def logsToCompact: Int =
    throttle(levelZeroMeter).logsToCompact

  def iterator(state: ThreadReadState): Iterator[PutOption] =
    new Iterator[PutOption] {
      var nextKeyValue: PutOption = _

      override def hasNext: Boolean =
        if (nextKeyValue == null) {
          nextKeyValue = head(state)
          nextKeyValue.isSome
        } else {
          nextKeyValue = higher(nextKeyValue.getKey.getC, state)
          nextKeyValue.isSome
        }

      override def next(): PutOption =
        nextKeyValue
    }

  def reverseIterator(state: ThreadReadState): Iterator[PutOption] =
    new Iterator[PutOption] {
      var nextKeyValue: PutOption = _

      override def hasNext: Boolean =
        if (nextKeyValue == null) {
          nextKeyValue = last(state)
          nextKeyValue.isSome
        } else {
          nextKeyValue = lower(nextKeyValue.getKey.getC, state)
          nextKeyValue.isSome
        }

      override def next(): PutOption =
        nextKeyValue
    }

  private def closeLogs: IO[Error.Log, Unit] =
    logs
      .close()
      .onLeftSideEffect {
        exception =>
          logger.error(s"$path: Failed to close logs", exception)
      }

  def closeNoSweep: IO[swaydb.Error.Level, Unit] =
    closeLogs
      .and(
        nextLevel
          .map(_.closeNoSweep)
          .getOrElse(IO.unit)
      )
      .and(releaseLocks)

  override def close[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] =
    bag
      .fromIO(closeLogs)
      .andThen(appliedFunctionsLog.foreach(_.close()))
      .and(
        nextLevel
          .map(_.close())
          .getOrElse(bag.unit)
      )
      .andIO(releaseLocks)

  def closeSegments: IO[swaydb.Error.Level, Unit] =
    nextLevel
      .map(_.closeSegments())
      .getOrElse(IO.unit)

  override def delete[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] =
    bag
      .fromIO(closeLogs)
      .andThen(appliedFunctionsLog.foreach(_.close()))
      .and(
        nextLevel
          .map(_.delete())
          .getOrElse(bag.unit)
      )
      .andIO(releaseLocks)
      .and(bag(Effect.walkDelete(path.getParent)))

  def mmap: MMAP =
    logs.mmap
}
