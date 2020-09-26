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

package swaydb.core.level.zero

import java.nio.channels.FileChannel
import java.nio.file.{Path, StandardOpenOption}

import com.typesafe.scalalogging.LazyLogging
import swaydb.Bag.Implicits._
import swaydb.Error.Level.ExceptionHandler
import swaydb.Exception.FunctionNotFound
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.data.KeyValue.{Put, PutOption}
import swaydb.core.data.Value.FromValue
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.{Effect, FileLocker, ForceSaveApplier}
import swaydb.core.level.seek._
import swaydb.core.level.{LevelRef, LevelSeek, NextLevel}
import swaydb.core.map.applied.{AppliedFunctionsMap, AppliedFunctionsMapCache}
import swaydb.core.map.serializer.AppliedFunctionsMapEntryWriter
import swaydb.core.map.timer.Timer
import swaydb.core.map.{MapEntry, Maps}
import swaydb.core.segment.format.a.entry.reader.PersistentReader
import swaydb.core.segment.{Segment, SegmentOption, ThreadReadState}
import swaydb.core.util.MinMax
import swaydb.core.{CoreState, MemoryPathGenerator, map}
import swaydb.data.OptimiseWrites
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.LevelMeter
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.storage.Level0Storage
import swaydb.data.util.Options
import swaydb.{Actor, Bag, Error, IO, OK}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Deadline, _}

private[core] case object LevelZero extends LazyLogging {

  def apply(mapSize: Long,
            appliedFunctionsMapSize: Long,
            clearAppliedFunctionsOnBoot: Boolean,
            storage: Level0Storage,
            enableTimer: Boolean,
            cacheKeyValueIds: Boolean,
            coreState: CoreState,
            nextLevel: Option[NextLevel],
            acceleration: LevelZeroMeter => Accelerator,
            throttle: LevelZeroMeter => FiniteDuration)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                        timeOrder: TimeOrder[Slice[Byte]],
                                                        bufferCleaner: ByteBufferSweeperActor,
                                                        functionStore: FunctionStore,
                                                        forceSaveApplier: ForceSaveApplier,
                                                        optimiseWrites: OptimiseWrites): IO[swaydb.Error.Level, LevelZero] = {
    import swaydb.core.map.serializer.LevelZeroMapEntryReader.Level0Reader
    import swaydb.core.map.serializer.LevelZeroMapEntryWriter._

    if (cacheKeyValueIds)
      PersistentReader.populateBaseEntryIds()
    else
      logger.info("cacheKeyValueIds is false. Key-value IDs cache disabled!")

    val mapsAndPathAndLock =
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
                  //LevelZero does not required FileSweeper since they are all Map files.
                  implicit val fileSweeper: FileSweeperActor = Actor.deadActor()
                  logger.info("{}: Recovering logs.", levelZeroDirectory)

                  val maps =
                    Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
                      path = levelZeroDirectory,
                      mmap = mmap,
                      fileSize = mapSize,
                      acceleration = acceleration,
                      recovery = recovery,
                      enableHashIndex = optimiseWrites.enableHashIndex
                    ).onLeftSideEffect {
                      _ =>
                        timer.close
                    }

                  maps flatMap {
                    maps =>
                      if (enableTimer) {
                        val appliedFunctionsMap =
                          AppliedFunctionsMap(
                            dir = databaseDirectory,
                            fileSize = appliedFunctionsMapSize,
                            mmap = mmap
                          )

                        appliedFunctionsMap
                          .result
                          .andThen((maps, Some(appliedFunctionsMap.item), levelZeroDirectory, Some(lock)))
                          .onLeftSideEffect {
                            _ =>
                              timer.close
                              appliedFunctionsMap.item.close()
                              maps.close().get
                          }

                      } else {
                        IO.Right((maps, None, levelZeroDirectory, Some(lock)))
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

                  val appliedFunctionsMap =
                    AppliedFunctionsMap(
                      dir = databaseDirectory,
                      fileSize = appliedFunctionsMapSize,
                      mmap = mmap
                    )

                  appliedFunctionsMap
                    .result
                    .and {
                      Timer.persistent(
                        path = databaseDirectory,
                        mmap = mmap
                      )
                    }
                    .map {
                      timer =>
                        (timer, Some(appliedFunctionsMap.item))
                    }
                    .onLeftSideEffect {
                      _ =>
                        appliedFunctionsMap.item.close()
                    }

                case None =>
                  IO.Right((Timer.memory(), None))
              }
            else
              IO.Right((Timer.empty, None))

          timer map {
            case (timer, appliedFunctionsMap) =>
              //LevelZero does not required FileSweeper since they are all Map files.
              implicit val fileSweeper: FileSweeperActor = Actor.deadActor()
              implicit val implicitTimer: Timer = timer

              val map =
                Maps.memory[Slice[Byte], Memory, LevelZeroMapCache](
                  fileSize = mapSize,
                  enableHashIndex = optimiseWrites.enableHashIndex,
                  acceleration = acceleration
                )

              val path = nextLevel.map(_.rootPath.getParent).getOrElse(MemoryPathGenerator.next())
              (map, appliedFunctionsMap, path.resolve(0.toString), None)
          }
      }

    mapsAndPathAndLock
      .flatMap {
        case (maps, appliedFunctions, path, lock: Option[FileLocker]) =>
          val zero =
            new LevelZero(
              path = path,
              mapSize = mapSize,
              maps = maps,
              nextLevel = nextLevel,
              inMemory = storage.memory,
              throttle = throttle,
              appliedFunctionsMap = appliedFunctions,
              coreState = coreState,
              lock = lock
            )

          appliedFunctions match {
            case Some(appliedFunctions) =>
              if (clearAppliedFunctionsOnBoot)
                IO(zero.clearAppliedFunctions())
                  .and(AppliedFunctionsMap.validate(appliedFunctions, functionStore))
                  .andThen(zero)
                  .onLeftSideEffect(_ => zero.close[Bag.Less]())
              else
                AppliedFunctionsMap
                  .validate(appliedFunctions, functionStore)
                  .andThen(zero)
                  .onLeftSideEffect(_ => zero.close[Bag.Less]())

            case None =>
              IO.Right(zero)
          }
      }
  }
}

private[swaydb] case class LevelZero(path: Path,
                                     mapSize: Long,
                                     maps: Maps[Slice[Byte], Memory, LevelZeroMapCache],
                                     nextLevel: Option[NextLevel],
                                     inMemory: Boolean,
                                     throttle: LevelZeroMeter => FiniteDuration,
                                     appliedFunctionsMap: Option[map.Map[Slice[Byte], Slice.Null.type, AppliedFunctionsMapCache]],
                                     coreState: CoreState,
                                     private val lock: Option[FileLocker])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                           timeOrder: TimeOrder[Slice[Byte]],
                                                                           functionStore: FunctionStore) extends LevelRef with LazyLogging {

  logger.info("{}: Level0 started.", path)

  import keyOrder._
  import swaydb.core.map.serializer.LevelZeroMapEntryWriter._

  val levelZeroMeter: LevelZeroMeter =
    maps.meter

  def onNextMapCallback(event: () => Unit): Unit =
    maps onNextMapCallback event

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
    maps.write(timer => MapEntry.Put[Slice[Byte], Memory.Put](key, Memory.Put(key, Slice.Null, None, timer.next)))
    OK.instance
  }

  def put(key: Slice[Byte], value: Slice[Byte]): OK = {
    validateInput(key)
    maps.write(timer => MapEntry.Put[Slice[Byte], Memory.Put](key, Memory.Put(key, value, None, timer.next)))
    OK.instance
  }

  def put(key: Slice[Byte], value: SliceOption[Byte], removeAt: Deadline): OK = {
    validateInput(key)
    maps.write(timer => MapEntry.Put[Slice[Byte], Memory.Put](key, Memory.Put(key, value, Some(removeAt), timer.next)))
    OK.instance
  }

  def put(key: Slice[Byte], value: SliceOption[Byte]): OK = {
    validateInput(key)
    maps.write(timer => MapEntry.Put[Slice[Byte], Memory.Put](key, Memory.Put(key, value, None, timer.next)))
    OK.instance
  }

  def put(entry: Timer => MapEntry[Slice[Byte], Memory]): OK = {
    maps write entry
    OK.instance
  }

  def remove(key: Slice[Byte]): OK = {
    validateInput(key)
    maps.write(timer => MapEntry.Put[Slice[Byte], Memory.Remove](key, Memory.Remove(key, None, timer.next)))
    OK.instance
  }

  def remove(key: Slice[Byte], at: Deadline): OK = {
    validateInput(key)
    maps.write(timer => MapEntry.Put[Slice[Byte], Memory.Remove](key, Memory.Remove(key, Some(at), timer.next)))
    OK.instance
  }

  def remove(fromKey: Slice[Byte], toKey: Slice[Byte]): OK = {
    validateInput(fromKey, toKey)

    if (fromKey equiv toKey)
      remove(fromKey)
    else
      maps
        .write {
          timer =>
            (MapEntry.Put[Slice[Byte], Memory.Range](fromKey, Memory.Range(fromKey, toKey, Value.FromValue.Null, Value.Remove(None, timer.next))): MapEntry[Slice[Byte], Memory]) ++
              MapEntry.Put[Slice[Byte], Memory.Remove](toKey, Memory.Remove(toKey, None, timer.next))
        }

    OK.instance
  }

  def remove(fromKey: Slice[Byte], toKey: Slice[Byte], at: Deadline): OK = {
    validateInput(fromKey, toKey)

    if (fromKey equiv toKey)
      remove(fromKey)
    else
      maps
        .write {
          timer =>
            (MapEntry.Put[Slice[Byte], Memory.Range](fromKey, Memory.Range(fromKey, toKey, Value.FromValue.Null, Value.Remove(Some(at), timer.next))): MapEntry[Slice[Byte], Memory]) ++
              MapEntry.Put[Slice[Byte], Memory.Remove](toKey, Memory.Remove(toKey, Some(at), timer.next))
        }

    OK.instance
  }

  def update(key: Slice[Byte], value: Slice[Byte]): OK = {
    validateInput(key)
    maps.write(timer => MapEntry.Put[Slice[Byte], Memory.Update](key, Memory.Update(key, value, None, timer.next)))
    OK.instance
  }

  def update(key: Slice[Byte], value: SliceOption[Byte]): OK = {
    validateInput(key)
    maps.write(timer => MapEntry.Put[Slice[Byte], Memory.Update](key, Memory.Update(key, value, None, timer.next)))
    OK.instance
  }

  def update(fromKey: Slice[Byte], toKey: Slice[Byte], value: SliceOption[Byte]): OK = {
    validateInput(fromKey, toKey)

    if (fromKey equiv toKey)
      update(fromKey, value)
    else
      maps
        .write {
          timer =>
            (MapEntry.Put[Slice[Byte], Memory.Range](
              key = fromKey,
              value = Memory.Range(
                fromKey = fromKey,
                toKey = toKey,
                fromValue = Value.FromValue.Null,
                rangeValue = Value.Update(value, None, timer.next)
              )
            ): MapEntry[Slice[Byte], Memory]) ++ MapEntry.Put[Slice[Byte], Memory.Update](toKey, Memory.Update(toKey, value, None, timer.next))
        }

    OK.instance
  }

  def clearAppliedFunctions(): ListBuffer[String] =
    appliedFunctionsMap match {
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
                appliedFunctions.writeSync(MapEntry.Remove(functionId)(AppliedFunctionsMapEntryWriter.FunctionsRemoveMapEntryWriter))
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
    appliedFunctionsMap.exists(_.cache.skipList.contains(functionId))

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
    appliedFunctionsMap foreach {
      appliedFunctions =>
        if (appliedFunctions.cache.skipList.notContains(function)) {
          //writeNoSync because this already because this functions is already being
          //called un map.write which is a sync function.
          appliedFunctions.writeNoSync(MapEntry.Put(function, Slice.Null)(AppliedFunctionsMapEntryWriter.FunctionsPutMapEntryWriter))
        }
    }

  def applyFunction(key: Slice[Byte], function: Slice[Byte]): OK =
    if (functionStore.notContains(function)) {
      throw FunctionNotFound(function.readString())
    } else {
      validateInput(key)

      maps.write {
        timer =>
          if (timer.isEmptyTimer) {
            throw new IllegalArgumentException("Functions are disabled.")
          } else {
            saveAppliedFunctionNoSync(function)
            MapEntry.Put[Slice[Byte], Memory.Function](key, Memory.Function(key, function, timer.next))
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
        maps.write {
          timer =>
            if (timer.isEmptyTimer) {
              throw new IllegalArgumentException("Functions are disabled.")
            } else {
              saveAppliedFunctionNoSync(function)

              (MapEntry.Put[Slice[Byte], Memory.Range](fromKey, Memory.Range(fromKey, toKey, Value.FromValue.Null, Value.Function(function, timer.next))): MapEntry[Slice[Byte], Memory]) ++
                MapEntry.Put[Slice[Byte], Memory.Function](toKey, Memory.Function(toKey, function, timer.next))
            }
        }

      OK.instance
    }

  private def getFromSkipList(key: Slice[Byte],
                              skipList: LevelSkipList): MemoryOption =
    if (skipList.hasRange)
      skipList.skipList.floor(key) match {
        case floor: Memory.Fixed if floor.key equiv key =>
          floor

        case floor: Memory.Range if key < floor.toKey =>
          floor

        case _ =>
          Memory.Null
      }
    else
      skipList.skipList.get(key)

  private def getFromNextLevel(key: Slice[Byte],
                               readState: ThreadReadState,
                               tailSkipLists: Slice[LevelSkipList]): KeyValue.PutOption = {
    val headOrNull = tailSkipLists.headOrNull

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
        levelSkipList = headOrNull,
        tailLevelSkipLists = tailSkipLists.dropHead()
      )
  }

  def currentGetter(skipList: LevelSkipList) =
    new CurrentGetter {
      override def get(key: Slice[Byte], readState: ThreadReadState): MemoryOption =
        getFromSkipList(key, skipList)
    }

  def nextGetter(tailMaps: Slice[LevelSkipList]) =
    new NextGetter {
      override def get(key: Slice[Byte], readState: ThreadReadState): KeyValue.PutOption =
        getFromNextLevel(key, readState, tailMaps)
    }

  private def find(key: Slice[Byte],
                   readState: ThreadReadState,
                   levelSkipList: LevelSkipList,
                   tailLevelSkipLists: Slice[LevelSkipList]): KeyValue.PutOption =
    Get.seek(
      key = key,
      readState = readState,
      currentGetter = currentGetter(levelSkipList),
      nextGetter = nextGetter(tailLevelSkipLists)
    )

  def get(key: Slice[Byte],
          readState: ThreadReadState): KeyValue.PutOption = {
    val snapshot = maps.flatMap(_.cache.leveledSkipList.asSlice)

    find(
      key = key,
      readState = readState,
      levelSkipList = snapshot.head,
      tailLevelSkipLists = snapshot.dropHead()
    )
  }

  def getKey(key: Slice[Byte],
             readState: ThreadReadState): SliceOption[Byte] =
    get(key, readState).mapSliceOptional(_.key)

  def firstKeyFromMaps: SliceOption[Byte] =
    maps.reduce[LevelSkipList, SliceOption[Byte]](
      nullResult = Slice.Null,
      flatMap = _.cache.leveledSkipList.asScala,
      applier = _.skipList.headKey,
      reducer = MinMax.minFavourLeftC[SliceOption[Byte], Slice[Byte]](_, _)(keyOrder)
    )

  def lastKeyFromMaps: SliceOption[Byte] =
    maps.reduce[LevelSkipList, SliceOption[Byte]](
      nullResult = Slice.Null,
      flatMap = _.cache.leveledSkipList.asScala,
      applier =
        skipList =>
          skipList
            .skipList
            .last()
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
    val mapHead = firstKeyFromMaps

    nextLevel match {
      case Some(nextLevel) =>
        nextLevel.headKey(readState) match {
          case nextLevelFirstKey: Slice[Byte] =>
            MinMax
              .minFavourLeftC[SliceOption[Byte], Slice[Byte]](mapHead, nextLevelFirstKey)(keyOrder)
              .flatMapSomeC(KeyValue.Put.Null: KeyValue.PutOption)(ceiling(_, readState))

          case Slice.Null =>
            mapHead.flatMapSomeC(KeyValue.Put.Null: KeyValue.PutOption)(ceiling(_, readState))
        }

      case None =>
        mapHead.flatMapSomeC(KeyValue.Put.Null: KeyValue.PutOption)(ceiling(_, readState))
    }
  }

  def last(readState: ThreadReadState): KeyValue.PutOption = {
    //read from LevelZero first
    val mapLast = lastKeyFromMaps

    nextLevel match {
      case Some(nextLevel) =>
        nextLevel.lastKey(readState) match {
          case nextLevelLastKey: Slice[Byte] =>
            MinMax
              .maxFavourLeftC[SliceOption[Byte], Slice[Byte]](mapLast, nextLevelLastKey)(keyOrder)
              .flatMapSomeC(KeyValue.Put.Null: KeyValue.PutOption)(floor(_, readState))

          case Slice.Null =>
            mapLast.flatMapSomeC(KeyValue.Put.Null: KeyValue.PutOption)(floor(_, readState))
        }

      case None =>
        mapLast.flatMapSomeC(KeyValue.Put.Null: KeyValue.PutOption)(floor(_, readState))
    }
  }

  def ceiling(key: Slice[Byte],
              readState: ThreadReadState): KeyValue.PutOption = {
    val snapshot = maps.flatMap(_.cache.leveledSkipList.asSlice)

    ceiling(
      key = key,
      readState = readState,
      headSkipList = snapshot.head,
      tailSkipLists = snapshot.dropHead()
    )
  }

  def ceiling(key: Slice[Byte],
              readState: ThreadReadState,
              headSkipList: LevelSkipList,
              tailSkipLists: Slice[LevelSkipList]): KeyValue.PutOption =
    find(
      key = key,
      readState = readState,
      levelSkipList = headSkipList,
      tailLevelSkipLists = tailSkipLists
    ) orElse {
      findHigher(
        key = key,
        readState = readState,
        currentMap = headSkipList,
        tailMaps = tailSkipLists
      )
    }

  def floor(key: Slice[Byte],
            readState: ThreadReadState): KeyValue.PutOption = {
    val snapshot = maps.flatMap(_.cache.leveledSkipList.asSlice)

    floor(
      key = key,
      readState = readState,
      headSkipList = snapshot.head,
      tailSkipLists = snapshot.dropHead()
    )
  }

  def floor(key: Slice[Byte],
            readState: ThreadReadState,
            headSkipList: LevelSkipList,
            tailSkipLists: Slice[LevelSkipList]): KeyValue.PutOption =
    find(
      key = key,
      readState = readState,
      levelSkipList = headSkipList,
      tailLevelSkipLists = tailSkipLists
    ) orElse {
      findLower(
        key = key,
        readState = readState,
        currentMap = headSkipList,
        tailMaps = tailSkipLists
      )
    }

  private def higherFromSkipList(key: Slice[Byte],
                                 levelSkipList: LevelSkipList): MemoryOption =
    if (levelSkipList.hasRange)
      levelSkipList.skipList.floor(key) match {
        case floorRange: Memory.Range if key >= floorRange.fromKey && key < floorRange.toKey =>
          floorRange

        case _ =>
          levelSkipList.skipList.higher(key)
      }
    else
      levelSkipList.skipList.higher(key)

  def findHigherInNextLevel(key: Slice[Byte],
                            readState: ThreadReadState,
                            tailMaps: Slice[LevelSkipList]): KeyValue.PutOption = {
    val nextMapOrNull = tailMaps.headOrNull
    if (nextMapOrNull == null)
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
        currentMap = nextMapOrNull,
        tailMaps = tailMaps.dropHead()
      )
  }

  def currentWalker(currentMap: LevelSkipList,
                    tailMaps: Slice[LevelSkipList]) =
    new CurrentWalker {
      override def get(key: Slice[Byte], readState: ThreadReadState): KeyValue.PutOption =
        find(
          key = key,
          readState = readState,
          levelSkipList = currentMap,
          tailLevelSkipLists = tailMaps
        )

      override def higher(key: Slice[Byte], readState: ThreadReadState): LevelSeek[Memory] =
        LevelSeek(
          segmentId = 0,
          result = higherFromSkipList(key, currentMap).toOptionS
        )

      override def lower(key: Slice[Byte], readState: ThreadReadState): LevelSeek[Memory] =
        LevelSeek(
          segmentId = 0,
          result = lowerFromMap(key, currentMap).toOptionS
        )

      override def levelNumber: String =
        "current"
    }

  def nextWalker(tailMaps: Slice[LevelSkipList]) =
    new NextWalker {
      override def higher(key: Slice[Byte],
                          readState: ThreadReadState): KeyValue.PutOption =
        findHigherInNextLevel(key, readState, tailMaps)

      override def lower(key: Slice[Byte],
                         readState: ThreadReadState): KeyValue.PutOption =
        findLowerInNextLevel(key, readState, tailMaps)

      override def get(key: Slice[Byte],
                       readState: ThreadReadState): KeyValue.PutOption =
        getFromNextLevel(key, readState, tailMaps)

      override def levelNumber: String =
        s"Map - Remaining maps: ${tailMaps.size}."
    }

  def findHigher(key: Slice[Byte],
                 readState: ThreadReadState,
                 currentMap: LevelSkipList,
                 tailMaps: Slice[LevelSkipList]): KeyValue.PutOption =
    Higher.seek(
      key = key,
      readState = readState,
      currentSeek = Seek.Current.Read(Int.MinValue),
      nextSeek = Seek.Next.Read,
      currentWalker = currentWalker(currentMap, tailMaps),
      nextWalker = nextWalker(tailMaps),
      keyOrder = keyOrder,
      timeOrder = timeOrder,
      functionStore = functionStore
    )

  /**
   * Higher cannot use an iterator because a single Map can value read requests multiple times for cases where a Map contains a range
   * to fetch ceiling key.
   *
   * Higher queries require iteration of all maps anyway so a full initial conversion to a List is acceptable.
   */
  def higher(key: Slice[Byte],
             readState: ThreadReadState): KeyValue.PutOption = {
    val snapshot = maps.flatMap(_.cache.leveledSkipList.asSlice)

    findHigher(
      key = key,
      readState = readState,
      currentMap = snapshot.head,
      tailMaps = snapshot.dropHead()
    )
  }

  private def lowerFromMap(key: Slice[Byte],
                           levelSkipList: LevelSkipList): MemoryOption =
    if (levelSkipList.hasRange)
      levelSkipList.skipList.floor(key) match {
        case floorRange: Memory.Range if key > floorRange.fromKey && key <= floorRange.toKey =>
          floorRange

        case _ =>
          levelSkipList.skipList.lower(key)
      }
    else
      levelSkipList.skipList.lower(key)

  def findLowerInNextLevel(key: Slice[Byte],
                           readState: ThreadReadState,
                           tailMaps: Slice[LevelSkipList]): KeyValue.PutOption = {
    val nextMapOrNull = tailMaps.headOrNull
    if (nextMapOrNull == null)
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
        currentMap = nextMapOrNull,
        tailMaps = tailMaps.dropHead()
      )
  }

  def findLower(key: Slice[Byte],
                readState: ThreadReadState,
                currentMap: LevelSkipList,
                tailMaps: Slice[LevelSkipList]): KeyValue.PutOption =
    Lower.seek(
      key = key,
      readState = readState,
      currentSeek = Seek.Current.Read(Int.MinValue),
      nextSeek = Seek.Next.Read,
      currentWalker = currentWalker(currentMap, tailMaps),
      nextWalker = nextWalker(tailMaps),
      keyOrder = keyOrder,
      timeOrder = timeOrder,
      functionStore = functionStore
    )

  /**
   * Lower cannot use an iterator because a single Map can value read requests multiple times for cases where a Map contains a range
   * to fetch ceiling key.
   *
   * Lower queries require iteration of all maps anyway so a full initial conversion to a List is acceptable.
   */
  def lower(key: Slice[Byte],
            readState: ThreadReadState): KeyValue.PutOption = {
    val snapshot = maps.flatMap(_.cache.leveledSkipList.asSlice)

    findLower(
      key = key,
      readState = readState,
      currentMap = snapshot.head,
      tailMaps = snapshot.dropHead()
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
    val keyValueCountInMaps = maps.keyValueCount
    nextLevel match {
      case Some(nextLevel) =>
        nextLevel.keyValueCount + keyValueCountInMaps

      case None =>
        keyValueCountInMaps
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

  def existsOnDisk: Boolean =
    Effect.exists(path)

  def mightContainKey(key: Slice[Byte]): Boolean = {
    val mightContainInMaps =
      maps.find[LevelSkipList, MemoryOption](
        nullResult = Memory.Null,
        flatMap = _.cache.leveledSkipList.asScala,
        matcher = _.skipList.get(key)
      )

    mightContainInMaps != Memory.Null || nextLevel.exists(_.mightContainKey(key))
  }

  private def findFunctionInMaps(functionId: Slice[Byte]): Boolean =
    maps.find[LevelSkipList, Boolean](
      nullResult = false,
      flatMap = _.cache.leveledSkipList.asScala,
      matcher =
        skipList =>
          skipList.skipList.values() exists {
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
    findFunctionInMaps(functionId) ||
      nextLevel.exists(_.mightContainFunction(functionId))

  override def hasNextLevel: Boolean =
    nextLevel.isDefined

  override def appendixPath: Path =
    nextLevel match {
      case Some(nextLevel) =>
        nextLevel.appendixPath

      case None =>
        throw IO.throwable("LevelZero does not have appendix.")
    }

  override def rootPath: Path =
    path

  override def isEmpty: Boolean =
    maps.isEmpty

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

  override def isTrash: Boolean =
    false

  override def levelNumber: Int = 0

  override def isZero: Boolean = true

  override def stateId: Long =
    maps.stateId

  override def nextCompactionDelay: FiniteDuration =
    throttle(levelZeroMeter)

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

  private def closeMaps: IO[Error.Map, Unit] =
    maps
      .close()
      .onLeftSideEffect {
        exception =>
          logger.error(s"$path: Failed to close maps", exception)
      }

  def closeNoSweep: IO[swaydb.Error.Level, Unit] =
    closeMaps
      .and(
        nextLevel
          .map(_.closeNoSweep)
          .getOrElse(IO.unit)
      )
      .and(releaseLocks)

  override def close[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] =
    bag
      .fromIO(closeMaps)
      .andThen(appliedFunctionsMap.foreach(_.close()))
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
      .fromIO(closeMaps)
      .andThen(appliedFunctionsMap.foreach(_.close()))
      .and(
        nextLevel
          .map(_.delete())
          .getOrElse(bag.unit)
      )
      .andIO(releaseLocks)
      .and(bag(Effect.walkDelete(path.getParent)))

  def mmap: MMAP =
    maps.mmap
}
