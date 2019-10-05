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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.level.zero

import java.nio.channels.{FileChannel, FileLock}
import java.nio.file.{Path, Paths, StandardOpenOption}

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Level.ExceptionHandler
import swaydb.core.actor.FileSweeper
import swaydb.core.data.KeyValue._
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.Effect
import swaydb.core.level.seek._
import swaydb.core.level.{LevelRef, LevelSeek, NextLevel}
import swaydb.core.map
import swaydb.core.map.serializer.{TimerMapEntryReader, TimerMapEntryWriter}
import swaydb.core.map.timer.Timer
import swaydb.core.map.{MapEntry, Maps, SkipListMerger}
import swaydb.core.segment.{ReadState, Segment}
import swaydb.core.util.MinMax
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.LevelMeter
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.storage.Level0Storage
import swaydb.data.util.StorageUnits._
import swaydb.{Error, IO, Tag}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.{Deadline, _}

private[core] object LevelZero extends LazyLogging {

  def apply(mapSize: Long,
            storage: Level0Storage,
            enableTimer: Boolean,
            nextLevel: Option[NextLevel],
            acceleration: LevelZeroMeter => Accelerator,
            throttle: LevelZeroMeter => FiniteDuration)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                        timeOrder: TimeOrder[Slice[Byte]],
                                                        functionStore: FunctionStore): IO[swaydb.Error.Level, LevelZero] = {
    import swaydb.core.map.serializer.LevelZeroMapEntryReader.Level0Reader
    import swaydb.core.map.serializer.LevelZeroMapEntryWriter._

    //LevelZero does not required FileSweeper since they are all Map files.
    implicit val fileSweeper: FileSweeper = FileSweeper.Disabled

    implicit val skipListMerger: SkipListMerger[Slice[Byte], Memory] = LevelZeroSkipListMerger
    val mapsAndPathAndLock =
      storage match {
        case Level0Storage.Persistent(mmap, databaseDirectory, recovery) =>
          val timer =
            if (enableTimer) {
              val timerDir = databaseDirectory.resolve("0").resolve("timer")
              Effect createDirectoriesIfAbsent timerDir
              Timer.persistent(
                path = timerDir,
                mmap = mmap,
                mod = 100000,
                flushCheckpointSize = 1.mb
              )(keyOrder = KeyOrder.default,
                timeOrder = timeOrder,
                functionStore = functionStore,
                writer = TimerMapEntryWriter.TimerPutMapEntryWriter,
                reader = TimerMapEntryReader.TimerPutMapEntryReader)
            } else {
              IO.Right(Timer.empty)
            }

          timer flatMap {
            implicit timer =>
              val path = databaseDirectory.resolve("0")
              logger.info("{}: Acquiring lock.", path)
              val lockFile = path.resolve("LOCK")
              Effect createDirectoriesIfAbsent path
              Effect createFileIfAbsent lockFile
              IO(FileChannel.open(lockFile, StandardOpenOption.WRITE).tryLock()) flatMap {
                lock =>
                  logger.info("{}: Recovering Maps.", path)
                  Maps.persistent[Slice[Byte], Memory](
                    path = path,
                    mmap = mmap,
                    fileSize = mapSize,
                    acceleration = acceleration,
                    recovery = recovery
                  ) map {
                    maps =>
                      (maps, path, Some(lock))
                  }
              }
          }

        case Level0Storage.Memory =>
          val timer =
            if (enableTimer)
              LevelRef.firstPersistentPath(nextLevel) match {
                case Some(persistentPath) =>
                  val timerDir = persistentPath.getParent.resolve("0").resolve("timer")
                  Effect createDirectoriesIfAbsent timerDir
                  Timer.persistent(
                    path = timerDir,
                    mmap = LevelRef.hasMMAP(nextLevel),
                    mod = 100000,
                    flushCheckpointSize = 1.mb
                  )(keyOrder = KeyOrder.default,
                    timeOrder = timeOrder,
                    functionStore = functionStore,
                    writer = TimerMapEntryWriter.TimerPutMapEntryWriter,
                    reader = TimerMapEntryReader.TimerPutMapEntryReader)

                case None =>
                  IO.Right(Timer.memory())
              }
            else
              IO.Right(Timer.empty)

          timer map {
            implicit timer =>
              val map =
                Maps.memory[Slice[Byte], Memory](
                  fileSize = mapSize,
                  acceleration = acceleration
                )

              (map, Paths.get("MEMORY_DB").resolve(0.toString), None)
          }
      }

    mapsAndPathAndLock map {
      case (maps, path, lock: Option[FileLock]) =>
        new LevelZero(
          path = path,
          mapSize = mapSize,
          maps = maps,
          nextLevel = nextLevel,
          inMemory = storage.memory,
          throttle = throttle,
          lock = lock
        )
    }
  }

  def delete(zero: LevelZero): IO[swaydb.Error.Delete, Unit] =
    zero
      .close
      .flatMap {
        _ =>
          zero
            .nextLevel
            .map(_.delete)
            .getOrElse(IO[swaydb.Error.Delete, Unit](Effect.walkDelete(zero.path.getParent)))
      }
}

private[core] case class LevelZero(path: Path,
                                   mapSize: Long,
                                   maps: Maps[Slice[Byte], Memory],
                                   nextLevel: Option[NextLevel],
                                   inMemory: Boolean,
                                   throttle: LevelZeroMeter => FiniteDuration,
                                   private val lock: Option[FileLock])(implicit keyOrder: KeyOrder[Slice[Byte]],
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
    IO[swaydb.Error.Close, Unit](Effect.release(lock)) flatMap {
      _ =>
        nextLevel.map(_.releaseLocks) getOrElse IO.unit
    }

  def assertRun(key: Slice[Byte])(block: => Unit): IO.Done =
    if (key.isEmpty) {
      throw new IllegalArgumentException("key cannot be empty.")
    } else {
      block
      IO.Done
    }

  def assertRun(fromKey: Slice[Byte], toKey: Slice[Byte])(block: => Unit): IO.Done =
    if (fromKey.isEmpty)
      throw new IllegalArgumentException("fromKey cannot be empty.")
    else if (toKey.isEmpty)
      throw new IllegalArgumentException("toKey cannot be empty.")
    else if (fromKey > toKey) //fromKey cannot also be equal to toKey. The invoking this assert should also check for equality and call update on single key-value.
      throw new IllegalArgumentException("fromKey should be less than toKey.")
    else {
      block
      IO.Done
    }

  def put(key: Slice[Byte]): IO.Done =
    assertRun(key) {
      maps.write(timer => MapEntry.Put[Slice[Byte], Memory](key, Memory.Put(key, None, None, timer.next)))
    }

  def put(key: Slice[Byte], value: Slice[Byte]): IO.Done =
    assertRun(key) {
      maps.write(timer => MapEntry.Put(key, Memory.Put(key, Some(value), None, timer.next)))
    }

  def put(key: Slice[Byte], value: Option[Slice[Byte]], removeAt: Deadline): IO.Done =
    assertRun(key) {
      maps.write(timer => MapEntry.Put(key, Memory.Put(key, value, Some(removeAt), timer.next)))
    }

  def put(key: Slice[Byte], value: Option[Slice[Byte]]): IO.Done =
    assertRun(key) {
      maps.write(timer => MapEntry.Put(key, Memory.Put(key, value, None, timer.next)))
    }

  def put(entry: Timer => MapEntry[Slice[Byte], Memory]): IO.Done = {
    maps write entry
    IO.Done
  }

  def remove(key: Slice[Byte]): IO.Done =
    assertRun(key) {
      maps.write(timer => MapEntry.Put[Slice[Byte], Memory.Remove](key, Memory.Remove(key, None, timer.next)))
    }

  def remove(key: Slice[Byte], at: Deadline): IO.Done =
    assertRun(key) {
      maps.write(timer => MapEntry.Put[Slice[Byte], Memory.Remove](key, Memory.Remove(key, Some(at), timer.next)))
    }

  def remove(fromKey: Slice[Byte], toKey: Slice[Byte]): IO.Done =
    assertRun(fromKey, toKey) {
      if (fromKey equiv toKey)
        remove(fromKey)
      else
        maps
          .write {
            timer =>
              (MapEntry.Put[Slice[Byte], Memory.Range](fromKey, Memory.Range(fromKey, toKey, None, Value.Remove(None, timer.next))): MapEntry[Slice[Byte], Memory]) ++
                MapEntry.Put[Slice[Byte], Memory.Remove](toKey, Memory.Remove(toKey, None, timer.next))
          }
    }

  def remove(fromKey: Slice[Byte], toKey: Slice[Byte], at: Deadline): IO.Done =
    assertRun(fromKey, toKey) {
      if (fromKey equiv toKey)
        remove(fromKey)
      else
        maps
          .write {
            timer =>
              (MapEntry.Put[Slice[Byte], Memory.Range](fromKey, Memory.Range(fromKey, toKey, None, Value.Remove(Some(at), timer.next))): MapEntry[Slice[Byte], Memory]) ++
                MapEntry.Put[Slice[Byte], Memory.Remove](toKey, Memory.Remove(toKey, Some(at), timer.next))
          }
    }

  def update(key: Slice[Byte], value: Slice[Byte]): IO.Done =
    assertRun(key) {
      maps.write(timer => MapEntry.Put(key, Memory.Update(key, Some(value), None, timer.next)))
    }

  def update(key: Slice[Byte], value: Option[Slice[Byte]]): IO.Done =
    assertRun(key) {
      maps.write(timer => MapEntry.Put(key, Memory.Update(key, value, None, timer.next)))
    }

  def update(fromKey: Slice[Byte], toKey: Slice[Byte], value: Slice[Byte]): IO.Done =
    update(fromKey, toKey, Some(value))

  def update(fromKey: Slice[Byte], toKey: Slice[Byte], value: Option[Slice[Byte]]): IO.Done =
    assertRun(fromKey, toKey) {
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
                  fromValue = None,
                  rangeValue = Value.Update(value, None, timer.next)
                )
              ): MapEntry[Slice[Byte], Memory]) ++ MapEntry.Put[Slice[Byte], Memory.Update](toKey, Memory.Update(toKey, value, None, timer.next))
          }
    }

  def clear(readState: ReadState): IO.Defer[swaydb.Error.Level, IO.Done] =
    headKey(readState)
      .flatMap {
        case Some(headKey) =>
          lastKey(readState)
            .flatMap {
              case Some(lastKey) =>
                IO.Defer(remove(headKey, lastKey))

              case None =>
                IO.Defer.done //might have been removed by another thread?
            }

        case None =>
          IO.Defer.done
      }

  def registerFunction(functionID: Slice[Byte], function: SwayFunction): SwayFunction =
    functionStore.put(functionID, function)

  def applyFunction(key: Slice[Byte], function: Slice[Byte]): IO.Done =
    if (!functionStore.exists(function))
      throw new IllegalArgumentException("Function does not exists in function store.")
    else
      assertRun(key) {
        maps.write {
          timer =>
            if (timer.empty)
              throw new IllegalArgumentException("Functions are disabled.")
            else
              MapEntry.Put[Slice[Byte], Memory.Function](key, Memory.Function(key, function, timer.next))
        }
      }

  def applyFunction(fromKey: Slice[Byte], toKey: Slice[Byte], function: Slice[Byte]): IO.Done =
    if (!functionStore.exists(function))
      throw new IllegalArgumentException("Function does not exists in function store.")
    else
      assertRun(fromKey, toKey) {
        if (fromKey equiv toKey)
          applyFunction(fromKey, function)
        else
          maps.write {
            timer =>
              if (timer.empty)
                throw new IllegalArgumentException("Functions are disabled.")
              else
                (MapEntry.Put[Slice[Byte], Memory.Range](fromKey, Memory.Range(fromKey, toKey, None, Value.Function(function, timer.next))): MapEntry[Slice[Byte], Memory]) ++
                  MapEntry.Put[Slice[Byte], Memory.Function](toKey, Memory.Function(toKey, function, timer.next))
          }
      }

  private def getFromMap(key: Slice[Byte],
                         currentMap: map.Map[Slice[Byte], Memory]): Option[Memory] =
    if (currentMap.hasRange)
      currentMap.skipList.floor(key) match {
        case floor @ Some(floorRange: Memory.Range) if key < floorRange.toKey =>
          floor

        case floor @ Some(keyValue) if keyValue.key equiv key =>
          floor

        case _ =>
          None
      }
    else
      currentMap
        .skipList
        .get(key)

  private def getFromNextLevel(key: Slice[Byte],
                               readState: ReadState,
                               otherMaps: Slice[map.Map[Slice[Byte], Memory]]): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    otherMaps.headOption match {
      case Some(nextMap) =>
        find(
          key = key,
          readState = readState,
          currentMap = nextMap,
          otherMaps = otherMaps.dropHead()
        )

      case None =>
        nextLevel match {
          case Some(nextLevel) =>
            nextLevel.get(key, readState)

          case None =>
            IO.Defer.none
        }
    }

  def currentGetter(currentMap: map.Map[Slice[Byte], Memory]) =
    new CurrentGetter {
      override def get(key: Slice[Byte], readState: ReadState): Option[Memory] =
        getFromMap(key, currentMap)
    }

  def nextGetter(readState: ReadState, otherMaps: Slice[map.Map[Slice[Byte], Memory]]) =
    new NextGetter {
      override def get(key: Slice[Byte], readState: ReadState): IO.Defer[swaydb.Error.Level, Option[ReadOnly.Put]] =
        getFromNextLevel(key, readState, otherMaps)
    }

  private def find(key: Slice[Byte],
                   readState: ReadState,
                   currentMap: map.Map[Slice[Byte], Memory],
                   otherMaps: Slice[map.Map[Slice[Byte], Memory]]): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    Get.seek(
      key = key,
      readState = readState,
      currentGetter = currentGetter(currentMap),
      nextGetter = nextGetter(readState, otherMaps)
    )

  def get(key: Slice[Byte],
          readState: ReadState): IO.Defer[swaydb.Error.Level, Option[ReadOnly.Put]] = {
    val currentMaps = maps.currentMapsSlice
    find(
      key = key,
      readState = readState,
      currentMap = currentMaps.head,
      otherMaps = currentMaps.dropHead()
    )
  }

  def getKey(key: Slice[Byte],
             readState: ReadState): IO.Defer[swaydb.Error.Level, Option[Slice[Byte]]] =
    get(key, readState).map(_.map(_.key))

  def firstKeyFromMaps =
    maps.reduce[Slice[Byte]](_.skipList.headKey, MinMax.minFavourLeft(_, _)(keyOrder))

  def lastKeyFromMaps =
    maps.reduce[Slice[Byte]](
      matcher =
        map =>
          map
            .skipList
            .last()
            .map {
              case fixed: KeyValue.ReadOnly.Fixed =>
                fixed.key

              case range: KeyValue.ReadOnly.Range =>
                range.toKey
            },
      reduce = MinMax.maxFavourLeft(_, _)(keyOrder)
    )

  def lastKey(readState: ReadState): IO.Defer[swaydb.Error.Level, Option[Slice[Byte]]] =
    last(readState).map(_.map(_.key))

  override def headKey(readState: ReadState): IO.Defer[swaydb.Error.Level, Option[Slice[Byte]]] =
    head(readState).map(_.map(_.key))

  def head(readState: ReadState): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    nextLevel match {
      case Some(nextLevel) =>
        nextLevel
          .headKey(readState)
          .flatMap {
            nextLevelFirstKey =>
              MinMax.minFavourLeft(firstKeyFromMaps, nextLevelFirstKey)(keyOrder)
                .map(ceiling(_, readState))
                .getOrElse(IO.Defer.none)
          }

      case None =>
        firstKeyFromMaps.map(ceiling(_, readState)) getOrElse IO.Defer.none
    }

  def last(readState: ReadState): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    nextLevel match {
      case Some(nextLevel) =>
        nextLevel
          .lastKey(readState)
          .flatMap {
            nextLevelLastKey =>
              MinMax
                .maxFavourLeft(lastKeyFromMaps, nextLevelLastKey)(keyOrder)
                .map(floor(_, readState))
                .getOrElse(IO.Defer.none)
          }

      case None =>
        lastKeyFromMaps.map(floor(_, readState)) getOrElse IO.Defer.none
    }

  def ceiling(key: Slice[Byte],
              readState: ReadState): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] = {
    val currentMaps = maps.currentMapsSlice
    ceiling(
      key = key,
      readState = readState,
      currentMap = currentMaps.head,
      otherMaps = currentMaps.dropHead()
    )
  }

  def ceiling(key: Slice[Byte],
              readState: ReadState,
              currentMap: map.Map[Slice[Byte], Memory],
              otherMaps: Slice[map.Map[Slice[Byte], Memory]]): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    find(
      key = key,
      readState = readState,
      currentMap = currentMap,
      otherMaps = otherMaps
    ) flatMap {
      found =>
        if (found.isDefined)
          IO.Defer(found)
        else
          findHigher(key, readState, currentMap, otherMaps)
    }

  def floor(key: Slice[Byte],
            readState: ReadState): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] = {
    val currentMaps = maps.currentMapsSlice
    floor(
      key = key,
      readState = readState,
      currentMap = currentMaps.head,
      otherMaps = currentMaps.dropHead()
    )
  }

  def floor(key: Slice[Byte],
            readState: ReadState,
            currentMap: map.Map[Slice[Byte], Memory],
            otherMaps: Slice[map.Map[Slice[Byte], Memory]]): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    find(
      key = key,
      readState = readState,
      currentMap = currentMap,
      otherMaps = otherMaps
    ) flatMap {
      found =>
        if (found.isDefined)
          IO.Defer(found)
        else
          findLower(key, readState, currentMap, otherMaps)
    }

  private def higherFromMap(key: Slice[Byte],
                            currentMap: map.Map[Slice[Byte], Memory]): Option[Memory] =
    if (currentMap.hasRange)
      currentMap.skipList.floor(key) match {
        case Some(floorRange: Memory.Range) if key >= floorRange.fromKey && key < floorRange.toKey =>
          Some(floorRange)

        case _ =>
          currentMap
            .skipList
            .higher(key)
      }
    else
      currentMap
        .skipList
        .higher(key)

  def findHigherInNextLevel(key: Slice[Byte],
                            readState: ReadState,
                            otherMaps: Slice[map.Map[Slice[Byte], Memory]]): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    otherMaps.headOption match {
      case Some(nextMap) =>
        //        println(s"Finding higher for key: ${key.readInt()} in Map: ${nextMap.pathOption}. Remaining map: ${otherMaps.size}")
        findHigher(key, readState, nextMap, otherMaps.dropHead())

      case None =>
        //        println(s"Finding higher for key: ${key.readInt()} in ${nextLevel.rootPath}")
        nextLevel match {
          case Some(nextLevel) =>
            nextLevel.higher(key, readState)

          case None =>
            IO.Defer.none
        }
    }

  def currentWalker(currentMap: map.Map[Slice[Byte], Memory],
                    otherMaps: Slice[map.Map[Slice[Byte], Memory]]) =
    new CurrentWalker {
      override def get(key: Slice[Byte], readState: ReadState): IO.Defer[swaydb.Error.Level, Option[ReadOnly.Put]] =
        find(
          key = key,
          readState = readState,
          currentMap = currentMap,
          otherMaps = otherMaps
        )

      override def higher(key: Slice[Byte], readState: ReadState): LevelSeek[Memory] =
        LevelSeek(
          segmentId = 0,
          result = higherFromMap(key, currentMap)
        )

      override def lower(key: Slice[Byte], readState: ReadState): LevelSeek[Memory] =
        LevelSeek(
          segmentId = 0,
          result = lowerFromMap(key, currentMap)
        )

      override def levelNumber: String =
        "current"
    }

  def nextWalker(otherMaps: Slice[map.Map[Slice[Byte], Memory]]) =
    new NextWalker {
      override def higher(key: Slice[Byte],
                          readState: ReadState): IO.Defer[swaydb.Error.Level, Option[ReadOnly.Put]] =
        findHigherInNextLevel(key, readState, otherMaps)

      override def lower(key: Slice[Byte],
                         readState: ReadState): IO.Defer[swaydb.Error.Level, Option[ReadOnly.Put]] =
        findLowerInNextLevel(key, readState, otherMaps)

      override def get(key: Slice[Byte],
                       readState: ReadState): IO.Defer[swaydb.Error.Level, Option[ReadOnly.Put]] =
        getFromNextLevel(key, readState, otherMaps)

      override def levelNumber: String =
        s"Map - Remaining maps: ${otherMaps.size}."
    }

  def findHigher(key: Slice[Byte],
                 readState: ReadState,
                 currentMap: map.Map[Slice[Byte], Memory],
                 otherMaps: Slice[map.Map[Slice[Byte], Memory]]): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    Higher.seek(
      key = key,
      readState = readState,
      currentSeek = Seek.Current.Read(Int.MinValue),
      nextSeek = Seek.Next.Read,
      currentWalker = currentWalker(currentMap, otherMaps),
      nextWalker = nextWalker(otherMaps),
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
             readState: ReadState): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] = {
    val currentMaps = maps.currentMapsSlice
    findHigher(
      key = key,
      readState = readState,
      currentMap = currentMaps.head,
      otherMaps = currentMaps.dropHead()
    )
  }

  private def lowerFromMap(key: Slice[Byte],
                           currentMap: map.Map[Slice[Byte], Memory]): Option[Memory] =
    if (currentMap.hasRange)
      currentMap.skipList.floor(key) match {
        case Some(floorRange: Memory.Range) if key > floorRange.fromKey && key <= floorRange.toKey =>
          Some(floorRange)

        case _ =>
          currentMap.skipList.lower(key)
      }
    else
      currentMap.skipList.lower(key)

  def findLowerInNextLevel(key: Slice[Byte],
                           readState: ReadState,
                           otherMaps: Slice[map.Map[Slice[Byte], Memory]]): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    otherMaps.headOption match {
      case Some(nextMap) =>
        //println(s"Finding lower for key: ${key.readInt()} in ${nextMap.pathOption}")
        findLower(key, readState, nextMap, otherMaps.dropHead())

      case None =>
        //println(s"Finding lower for key: ${key.readInt()} in ${nextLevel.rootPath}")
        nextLevel
          .map(_.lower(key, readState))
          .getOrElse(IO.Defer.none)
    }

  def findLower(key: Slice[Byte],
                readState: ReadState,
                currentMap: map.Map[Slice[Byte], Memory],
                otherMaps: Slice[map.Map[Slice[Byte], Memory]]): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    Lower.seek(
      key = key,
      readState = readState,
      currentSeek = Seek.Current.Read(Int.MinValue),
      nextSeek = Seek.Next.Read,
      currentWalker = currentWalker(currentMap, otherMaps),
      nextWalker = nextWalker(otherMaps),
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
            readState: ReadState): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] = {
    val currentMaps = maps.currentMapsSlice
    findLower(
      key = key,
      readState = readState,
      currentMap = currentMaps.head,
      otherMaps = currentMaps.dropHead()
    )
  }

  def contains(key: Slice[Byte],
               readState: ReadState): IO.Defer[swaydb.Error.Level, Boolean] =
    get(key, readState).map(_.isDefined)

  def valueSize(key: Slice[Byte],
                readState: ReadState): IO.Defer[swaydb.Error.Level, Option[Int]] =
    get(key, readState) map {
      result =>
        result map {
          response =>
            response.valueLength
        }
    }

  def bloomFilterKeyValueCount: Int = {
    val keyValueCountInMaps = maps.keyValueCount.getOrElse(0)
    nextLevel match {
      case Some(nextLevel) =>
        nextLevel.bloomFilterKeyValueCount + keyValueCountInMaps

      case None =>
        keyValueCountInMaps
    }
  }

  def deadline(key: Slice[Byte],
               readState: ReadState): IO.Defer[swaydb.Error.Level, Option[Deadline]] =
    get(
      key = key,
      readState = readState
    ).map(_.flatMap(_.deadline))

  def sizeOfSegments: Long =
    nextLevel match {
      case Some(nextLevel) =>
        nextLevel.sizeOfSegments

      case None =>
        0L
    }

  def existsOnDisk: Boolean =
    Effect.exists(path)

  def close: IO[swaydb.Error.Close, Unit] = {
    //    Delay.cancelTimer()
    maps
      .close
      .onLeftSideEffect {
        exception =>
          logger.error(s"$path: Failed to close maps", exception)
      }

    releaseLocks

    nextLevel
      .map(_.close)
      .getOrElse(IO.unit)
  }

  def closeSegments: IO[swaydb.Error.Level, Unit] =
    nextLevel
      .map(_.closeSegments())
      .getOrElse(IO.unit)

  def mightContainKey(key: Slice[Byte]): Boolean =
    maps.contains(key) ||
      nextLevel.exists(_.mightContainKey(key))

  private def findFunctionInMaps(functionId: Slice[Byte]): Boolean =
    maps.find[Boolean] {
      map =>
        Option {
          map.skipList.values().asScala exists {
            case _: Memory.Put | _: Memory.Remove | _: Memory.Update =>
              false

            case function: Memory.Function =>
              function.function equiv functionId

            case pendingApply: Memory.PendingApply =>
              FunctionStore.containsFunction(functionId, pendingApply.applies)

            case range: Memory.Range =>
              FunctionStore.containsFunction(functionId, Slice(range.rangeValue) ++ range.fromValue)
          }
        }
    } getOrElse false

  def mightContainFunctionInMaps(functionId: Slice[Byte]): Boolean =
    maps.queuedMapsCountWithCurrent >= 2 ||
      findFunctionInMaps(functionId)

  def mightContainFunction(functionId: Slice[Byte]): Boolean =
    mightContainFunctionInMaps(functionId) ||
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

  override def getSegment(minKey: Slice[Byte]): Option[Segment] =
    nextLevel.flatMap(_.getSegment(minKey))

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

  override def delete: IO[swaydb.Error.Delete, Unit] =
    LevelZero.delete(this)

  @tailrec
  final def run[T[_]](apply: LevelZero => IO.Defer[swaydb.Error.Level, Option[ReadOnly.Put]])(implicit tag: Tag[T]): T[Option[(Slice[Byte], Option[Slice[Byte]])]] = {
    import Tag.Implicits._

    @volatile var failed: Option[Error] = None

    val result: T[Option[(Slice[Byte], Option[Slice[Byte]])]] =
      apply(this)
        .run
        .flatMap {
          case Some(put) =>
            try
              tag.success(Some(put.key, put.getOrFetchValue)) //getOrFetchValue could also result in ReservedResource.
            catch {
              case throwable: Throwable =>
                failed = Some(IO.ExceptionHandler.toError(throwable))
                tag.failure(throwable)
            }

          case None =>
            tag.none
        }

    //if fetching value failed perform read again.
    failed match {
      case Some(error) =>
        error match {
          case _: Error.NoSuchFile | _: Error.FileNotFound | _: Error.NullMappedByteBuffer =>
            run(apply)

          case _: Error =>
            result
        }

      case None =>
        result
    }
  }
}
