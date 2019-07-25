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
import java.util

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.core.data.KeyValue._
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.IOEffect
import swaydb.core.level.seek._
import swaydb.core.level.{LevelRef, NextLevel}
import swaydb.core.map
import swaydb.core.map.serializer.{TimerMapEntryReader, TimerMapEntryWriter}
import swaydb.core.map.timer.Timer
import swaydb.core.map.{MapEntry, Maps, SkipListMerger}
import swaydb.core.queue.FileLimiter
import swaydb.core.segment.Segment
import swaydb.core.util.MinMax
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.LevelMeter
import swaydb.data.io.Core
import swaydb.data.io.Core.Error.Level.ErrorHandler
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.storage.Level0Storage
import swaydb.data.util.StorageUnits._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.{Deadline, _}

private[core] object LevelZero extends LazyLogging {

  def apply(mapSize: Long,
            storage: Level0Storage,
            nextLevel: Option[NextLevel],
            acceleration: LevelZeroMeter => Accelerator,
            throttle: LevelZeroMeter => FiniteDuration)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                        timeOrder: TimeOrder[Slice[Byte]],
                                                        limiter: FileLimiter,
                                                        functionStore: FunctionStore): IO[Core.Error.Level, LevelZero] = {
    import swaydb.core.map.serializer.LevelZeroMapEntryReader.Level0Reader
    import swaydb.core.map.serializer.LevelZeroMapEntryWriter._
    implicit val timerReader = TimerMapEntryReader.TimerPutMapEntryReader
    implicit val timerWriter = TimerMapEntryWriter.TimerPutMapEntryWriter

    implicit val skipListMerger: SkipListMerger[Slice[Byte], Memory.SegmentResponse] = LevelZeroSkipListMerger
    val mapsAndPathAndLock =
      storage match {
        case Level0Storage.Persistent(mmap, databaseDirectory, recovery) =>
          val timerDir = databaseDirectory.resolve("0").resolve("timer")
          IOEffect createDirectoriesIfAbsent timerDir
          Timer.persistent(
            path = timerDir,
            mmap = mmap,
            mod = 100000,
            flushCheckpointSize = 1.mb
          ) flatMap {
            implicit timer =>
              val path = databaseDirectory.resolve("0")
              logger.info("{}: Acquiring lock.", path)
              val lockFile = path.resolve("LOCK")
              IOEffect createFileIfAbsent lockFile
              IO(FileChannel.open(lockFile, StandardOpenOption.WRITE).tryLock()) flatMap {
                lock =>
                  logger.info("{}: Recovering Maps.", path)
                  Maps.persistent[Slice[Byte], Memory.SegmentResponse](
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
            LevelRef.firstPersistentPath(nextLevel) match {
              case Some(persistentPath) =>
                val timerDir = persistentPath.getParent.resolve("0").resolve("timer")
                IOEffect createDirectoriesIfAbsent timerDir
                Timer.persistent(
                  path = timerDir,
                  mmap = LevelRef.hasMMAP(nextLevel),
                  mod = 100000,
                  flushCheckpointSize = 1.mb
                )

              case None =>
                IO.Success(Timer.memory())
            }

          timer map {
            implicit timer =>
              val map =
                Maps.memory[Slice[Byte], Memory.SegmentResponse](
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

  def delete(zero: LevelZero): IO[Core.Error.Delete, Unit] =
    zero
      .close
      .flatMap {
        _ =>
          zero
            .nextLevel
            .map(_.delete)
            .getOrElse(IOEffect.walkDelete(zero.path.getParent))
      }
}

private[core] case class LevelZero(path: Path,
                                   mapSize: Long,
                                   maps: Maps[Slice[Byte], Memory.SegmentResponse],
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

  def releaseLocks: IO[Core.Error.Close, Unit] =
    IOEffect.release(lock) flatMap {
      _ =>
        nextLevel.map(_.releaseLocks) getOrElse IO.unit
    }

  def assertKey(key: Slice[Byte])(block: => IO[Core.Error.Level, IO.Done]): IO[Core.Error.Level, IO.Done] =
    if (key.isEmpty)
      IO.failed(new IllegalArgumentException("Input key(s) cannot be empty."))
    else
      block

  def put(key: Slice[Byte]): IO[Core.Error.Level, IO.Done] =
    assertKey(key) {
      maps.write(timer => MapEntry.Put[Slice[Byte], Memory.SegmentResponse](key, Memory.Put(key, None, None, timer.next)))
    }

  def put(key: Slice[Byte], value: Slice[Byte]): IO[Core.Error.Level, IO.Done] =
    assertKey(key) {
      maps.write(timer => MapEntry.Put(key, Memory.Put(key, Some(value), None, timer.next)))
    }

  def put(key: Slice[Byte], value: Option[Slice[Byte]], removeAt: Deadline): IO[Core.Error.Level, IO.Done] =
    assertKey(key) {
      maps.write(timer => MapEntry.Put(key, Memory.Put(key, value, Some(removeAt), timer.next)))
    }

  def put(key: Slice[Byte], value: Option[Slice[Byte]]): IO[Core.Error.Level, IO.Done] =
    assertKey(key) {
      maps.write(timer => MapEntry.Put(key, Memory.Put(key, value, None, timer.next)))
    }

  def put(entry: Timer => MapEntry[Slice[Byte], Memory.SegmentResponse]): IO[Core.Error.Level, IO.Done] =
    maps write entry

  def remove(key: Slice[Byte]): IO[Core.Error.Level, IO.Done] =
    assertKey(key) {
      maps.write(timer => MapEntry.Put[Slice[Byte], Memory.Remove](key, Memory.Remove(key, None, timer.next)))
    }

  def remove(key: Slice[Byte], at: Deadline): IO[Core.Error.Level, IO.Done] =
    assertKey(key) {
      maps.write(timer => MapEntry.Put[Slice[Byte], Memory.Remove](key, Memory.Remove(key, Some(at), timer.next)))
    }

  def remove(fromKey: Slice[Byte], toKey: Slice[Byte]): IO[Core.Error.Level, IO.Done] =
    assertKey(fromKey) {
      assertKey(toKey) {
        if (fromKey equiv toKey)
          remove(fromKey)
        else if (fromKey > toKey)
          IO.failed(new Exception("fromKey should be less than or equal to toKey"))
        else
          maps.write {
            timer =>
              (MapEntry.Put[Slice[Byte], Memory.Range](fromKey, Memory.Range(fromKey, toKey, None, Value.Remove(None, timer.next))): MapEntry[Slice[Byte], Memory.SegmentResponse]) ++
                MapEntry.Put[Slice[Byte], Memory.Remove](toKey, Memory.Remove(toKey, None, timer.next))
          }
      }
    }

  def remove(fromKey: Slice[Byte], toKey: Slice[Byte], at: Deadline): IO[Core.Error.Level, IO.Done] =
    assertKey(fromKey) {
      assertKey(toKey) {
        if (fromKey equiv toKey)
          remove(fromKey)
        else if (fromKey > toKey)
          IO.failed(new Exception("fromKey should be less than or equal to toKey"))
        else
          maps.write {
            timer =>
              (MapEntry.Put[Slice[Byte], Memory.Range](fromKey, Memory.Range(fromKey, toKey, None, Value.Remove(Some(at), timer.next))): MapEntry[Slice[Byte], Memory.SegmentResponse]) ++
                MapEntry.Put[Slice[Byte], Memory.Remove](toKey, Memory.Remove(toKey, Some(at), timer.next))
          }
      }
    }

  def update(key: Slice[Byte], value: Slice[Byte]): IO[Core.Error.Level, IO.Done] =
    assertKey(key) {
      maps.write(timer => MapEntry.Put(key, Memory.Update(key, Some(value), None, timer.next)))
    }

  def update(key: Slice[Byte], value: Option[Slice[Byte]]): IO[Core.Error.Level, IO.Done] =
    assertKey(key) {
      maps.write(timer => MapEntry.Put(key, Memory.Update(key, value, None, timer.next)))
    }

  def update(fromKey: Slice[Byte], toKey: Slice[Byte], value: Slice[Byte]): IO[Core.Error.Level, IO.Done] =
    update(fromKey, toKey, Some(value))

  def update(fromKey: Slice[Byte], toKey: Slice[Byte], value: Option[Slice[Byte]]): IO[Core.Error.Level, IO.Done] =
    assertKey(fromKey) {
      assertKey(toKey) {
        if (fromKey equiv toKey)
          update(fromKey, value)
        else if (fromKey >= toKey)
          IO.failed(new Exception("fromKey should be less than or equal to toKey"))
        else
          maps.write {
            timer =>
              (MapEntry.Put[Slice[Byte], Memory.Range](
                key = fromKey,
                value = Memory.Range(
                  fromKey = fromKey,
                  toKey = toKey,
                  fromValue = None,
                  rangeValue = Value.Update(value, None, timer.next)
                )
              ): MapEntry[Slice[Byte], Memory.SegmentResponse]) ++ MapEntry.Put[Slice[Byte], Memory.Update](toKey, Memory.Update(toKey, value, None, timer.next))
          }
      }
    }

  def clear(): IO.Defer[Core.Error.Level, IO.Done] =
    headKey flatMap {
      case Some(headKey) =>
        lastKey flatMap {
          case Some(lastKey) =>
            remove(headKey, lastKey).asDeferred

          case None =>
            IO.done //might have been removed by another thread?
        }

      case None =>
        IO.done
    }

  def registerFunction(functionID: Slice[Byte], function: SwayFunction): SwayFunction =
    functionStore.put(functionID, function)

  def applyFunction(key: Slice[Byte], function: Slice[Byte]): IO[Core.Error.Level, IO.Done] =
    if (!functionStore.exists(function))
      IO.failed(new Exception("Function does not exists in function store."))
    else
      assertKey(key) {
        maps.write(timer => MapEntry.Put[Slice[Byte], Memory.Function](key, Memory.Function(key, function, timer.next)))
      }

  def applyFunction(fromKey: Slice[Byte], toKey: Slice[Byte], function: Slice[Byte]): IO[Core.Error.Level, IO.Done] =
    if (!functionStore.exists(function))
      IO.failed(new Exception("Function does not exists in function store."))
    else
      assertKey(fromKey) {
        assertKey(toKey) {
          if (fromKey equiv toKey)
            applyFunction(fromKey, function)
          else if (fromKey >= toKey)
            IO.failed(new Exception("fromKey should be less than or equal to toKey"))
          else
            maps.write {
              timer =>
                (MapEntry.Put[Slice[Byte], Memory.Range](fromKey, Memory.Range(fromKey, toKey, None, Value.Function(function, timer.next))): MapEntry[Slice[Byte], Memory.SegmentResponse]) ++
                  MapEntry.Put[Slice[Byte], Memory.Function](toKey, Memory.Function(toKey, function, timer.next))
            }
        }
      }

  @tailrec
  private def getFromMap(key: Slice[Byte],
                         currentMap: map.Map[Slice[Byte], Memory.SegmentResponse],
                         preFetched: Option[Memory.SegmentResponse] = None): Option[Memory.SegmentResponse] =
    if (currentMap.hasRange)
      preFetched orElse currentMap.floor(key) match {
        case floor @ Some(floorRange: Memory.Range) if key < floorRange.toKey =>
          floor

        case floor @ Some(keyValue) if keyValue.key equiv key =>
          floor

        case Some(range: Memory.Range) => //if it's still a range then check if the Map is performing concurrent updates and retry.
          //This is a temporary solution to atomic writes issue in LevelZero.
          //If a Map contains a Range key-value, inserting new Fixed key-values for the Range
          //is not returning the previously inserted Range key-value (on floor) and is returning an invalid floor entry. This could be
          //due to concurrent changes to the Map are also concurrently changing the level hierarchy of this skipList which is routing
          //searching to key-values to invalid range entry or there is an issue with skipList merger.
          //Temporary solution is to retry read. If the retried read returns a different result to existing that means that
          //the current map is going through concurrent range updates and the read is retried.
          val reFetched = currentMap.floor(key)
          //          val fetchedRange = reFetched.map(_.asInstanceOf[Memory.Range])
          //          println(s"Key: ${key.readInt()}")
          //          println(s"Existing floor: fromKey : ${range.fromKey.readInt()} -> fromKey: ${range.toKey.readInt()}")
          //          println(s"Re-fetch floor: fromKey : ${fetchedRange.map(_.fromKey.readInt())} -> fromKey: ${fetchedRange.map(_.toKey.readInt())}")
          //          println
          //if the re-fetched key-value is different to existing key-value retry else return None.
          if (!reFetched.exists(_.key equiv range.key))
            getFromMap(key, currentMap, reFetched)
          else
            None

        case _ =>
          None
      }
    else
      currentMap.get(key)(keyOrder)

  private def getFromNextLevel(key: Slice[Byte],
                               mapsIterator: util.Iterator[map.Map[Slice[Byte], Memory.SegmentResponse]]): IO.Defer[Core.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    if (mapsIterator.hasNext)
      find(key, mapsIterator.next(), mapsIterator)
    else
      nextLevel.map(_ get key) getOrElse IO.none

  def currentGetter(currentMap: map.Map[Slice[Byte], Memory.SegmentResponse]) =
    new CurrentGetter {
      override def get(key: Slice[Byte]): IO[Core.Error.Level, Option[ReadOnly.SegmentResponse]] =
        IO(getFromMap(key, currentMap))
    }

  def nextGetter(mapsIterator: util.Iterator[map.Map[Slice[Byte], Memory.SegmentResponse]]) =
    new NextGetter {
      override def get(key: Slice[Byte]): IO.Defer[Core.Error.Level, Option[ReadOnly.Put]] =
        getFromNextLevel(key, mapsIterator)
    }

  private def find(key: Slice[Byte],
                   currentMap: map.Map[Slice[Byte], Memory.SegmentResponse],
                   mapsIterator: util.Iterator[map.Map[Slice[Byte], Memory.SegmentResponse]]): IO.Defer[Core.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    Get.seek(
      key = key,
      currentGetter = currentGetter(currentMap),
      nextGetter = nextGetter(mapsIterator)
    )

  def get(key: Slice[Byte]): IO.Defer[Core.Error.Level, Option[ReadOnly.Put]] =
    find(
      key = key,
      currentMap = maps.map,
      mapsIterator = maps.iterator
    )

  def getKey(key: Slice[Byte]): IO.Defer[Core.Error.Level, Option[Slice[Byte]]] =
    get(key).mapDeferred(_.map(_.key))

  def firstKeyFromMaps =
    maps.reduce[Slice[Byte]](_.firstKey, MinMax.min(_, _)(keyOrder))

  def lastKeyFromMaps =
    maps.reduce[Slice[Byte]](
      matcher =
        map =>
          map.lastValue() map {
            case fixed: KeyValue.ReadOnly.Fixed =>
              fixed.key
            case range: KeyValue.ReadOnly.Range =>
              range.toKey
          },
      reduce = MinMax.max(_, _)(keyOrder)
    )

  def lastKey: IO.Defer[Core.Error.Level, Option[Slice[Byte]]] =
    last.mapDeferred(_.map(_.key))

  override def headKey: IO.Defer[Core.Error.Level, Option[Slice[Byte]]] =
    head.mapDeferred(_.map(_.key))

  def head: IO.Defer[Core.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    nextLevel map {
      nextLevel =>
        nextLevel.headKey flatMap {
          nextLevelFirstKey =>
            MinMax.min(firstKeyFromMaps, nextLevelFirstKey)(keyOrder).map(ceiling) getOrElse IO.none
        }
    } getOrElse {
      firstKeyFromMaps.map(ceiling) getOrElse IO.none
    }

  def last: IO.Defer[Core.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    nextLevel map {
      nextLevel =>
        nextLevel.lastKey flatMap {
          nextLevelLastKey =>
            MinMax.max(lastKeyFromMaps, nextLevelLastKey)(keyOrder).map(floor) getOrElse IO.none
        }
    } getOrElse {
      lastKeyFromMaps.map(floor) getOrElse IO.none
    }

  def ceiling(key: Slice[Byte]): IO.Defer[Core.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    ceiling(key, maps.map, maps.iterator.asScala.toList)

  def ceiling(key: Slice[Byte],
              currentMap: map.Map[Slice[Byte], Memory.SegmentResponse],
              otherMaps: List[map.Map[Slice[Byte], Memory.SegmentResponse]]): IO.Defer[Core.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    find(key, currentMap, otherMaps.iterator.asJava) flatMap {
      found =>
        if (found.isDefined)
          IO.Success(found)
        else
          findHigher(key, currentMap, otherMaps)
    }

  def floor(key: Slice[Byte]): IO.Defer[Core.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    floor(key, maps.map, maps.iterator.asScala.toList)

  def floor(key: Slice[Byte],
            currentMap: map.Map[Slice[Byte], Memory.SegmentResponse],
            otherMaps: List[map.Map[Slice[Byte], Memory.SegmentResponse]]): IO.Defer[Core.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    find(key, currentMap, otherMaps.iterator.asJava) flatMap {
      found =>
        if (found.isDefined)
          IO.Success(found)
        else
          findLower(key, currentMap, otherMaps)
    }

  @tailrec
  private def higherFromMap(key: Slice[Byte],
                            currentMap: map.Map[Slice[Byte], Memory.SegmentResponse],
                            preFetched: Option[Memory] = None): Option[Memory.SegmentResponse] =
    if (currentMap.hasRange)
      preFetched orElse currentMap.floor(key) match {
        case Some(floorRange: Memory.Range) if key >= floorRange.fromKey && key < floorRange.toKey =>
          Some(floorRange)

        case Some(range: Memory.Range) =>
          val reFetched = currentMap.floor(key)
          if (!reFetched.exists(_.key equiv range.key))
            higherFromMap(key, currentMap, reFetched)
          else
            currentMap.higherValue(key)

        case _ =>
          currentMap.higherValue(key)
      }
    else
      currentMap.higher(key).map(_._2)

  def findHigherInNextLevel(key: Slice[Byte],
                            otherMaps: List[map.Map[Slice[Byte], Memory.SegmentResponse]]): IO.Defer[Core.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    otherMaps.headOption match {
      case Some(nextMap) =>
        //        println(s"Finding higher for key: ${key.readInt()} in Map: ${nextMap.pathOption}. Remaining map: ${otherMaps.size}")
        findHigher(key, nextMap, otherMaps.drop(1))
      case None =>
        //        println(s"Finding higher for key: ${key.readInt()} in ${nextLevel.rootPath}")
        nextLevel.map(_.higher(key)) getOrElse IO.none
    }

  def currentWalker(currentMap: map.Map[Slice[Byte], Memory.SegmentResponse],
                    otherMaps: List[map.Map[Slice[Byte], Memory.SegmentResponse]]) =
    new CurrentWalker {
      override def get(key: Slice[Byte]): IO.Defer[Core.Error.Level, Option[ReadOnly.Put]] =
        find(key, currentMap, otherMaps.asJava.iterator())

      override def higher(key: Slice[Byte]): IO[Core.Error.Level, Option[ReadOnly.SegmentResponse]] =
        IO(higherFromMap(key, currentMap))

      override def lower(key: Slice[Byte]): IO[Core.Error.Level, Option[ReadOnly.SegmentResponse]] =
        IO(lowerFromMap(key, currentMap))

      override def levelNumber: String =
        "current"
    }

  def nextWalker(otherMaps: List[map.Map[Slice[Byte], Memory.SegmentResponse]]) =
    new NextWalker {
      override def higher(key: Slice[Byte]): IO.Defer[Core.Error.Level, Option[ReadOnly.Put]] =
        findHigherInNextLevel(key, otherMaps)

      override def lower(key: Slice[Byte]): IO.Defer[Core.Error.Level, Option[ReadOnly.Put]] =
        findLowerInNextLevel(key, otherMaps)

      override def get(key: Slice[Byte]): IO.Defer[Core.Error.Level, Option[ReadOnly.Put]] =
        getFromNextLevel(key, otherMaps.iterator.asJava)

      override def hasStateChanged(previousState: Long): Boolean =
        false

      override def stateID: Long =
        -1

      override def levelNumber: String =
        "map"
    }

  def findHigher(key: Slice[Byte],
                 currentMap: map.Map[Slice[Byte], Memory.SegmentResponse],
                 otherMaps: List[map.Map[Slice[Byte], Memory.SegmentResponse]]): IO.Defer[Core.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    Higher.seek(
      key = key,
      currentSeek = Seek.Read,
      nextSeek = Seek.Read,
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
  def higher(key: Slice[Byte]): IO.Defer[Core.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    findHigher(
      key = key,
      currentMap = maps.map,
      otherMaps = maps.queuedMaps.toList
    )

  @tailrec
  private def lowerFromMap(key: Slice[Byte],
                           currentMap: map.Map[Slice[Byte], Memory.SegmentResponse],
                           preFetched: Option[Memory] = None): Option[Memory.SegmentResponse] =
    if (currentMap.hasRange)
      preFetched orElse currentMap.floor(key) match {
        case Some(floorRange: Memory.Range) if key > floorRange.fromKey && key <= floorRange.toKey =>
          Some(floorRange)

        case Some(range: Memory.Range) =>
          val reFetched = currentMap.floor(key)
          if (!reFetched.exists(_.key equiv range.key))
            lowerFromMap(key, currentMap, reFetched)
          else
            currentMap.lowerValue(key)

        case _ =>
          currentMap.lowerValue(key)
      }
    else
      currentMap.lower(key).map(_._2)

  def findLowerInNextLevel(key: Slice[Byte],
                           otherMaps: List[map.Map[Slice[Byte], Memory.SegmentResponse]]): IO.Defer[Core.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    otherMaps.headOption match {
      case Some(nextMap) =>
        //println(s"Finding lower for key: ${key.readInt()} in ${nextMap.pathOption}")
        findLower(key, nextMap, otherMaps.drop(1))
      case None =>
        //println(s"Finding lower for key: ${key.readInt()} in ${nextLevel.rootPath}")
        nextLevel.map(_.lower(key)) getOrElse IO.none
    }

  def findLower(key: Slice[Byte],
                currentMap: map.Map[Slice[Byte], Memory.SegmentResponse],
                otherMaps: List[map.Map[Slice[Byte], Memory.SegmentResponse]]): IO.Defer[Core.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    Lower.seek(
      key = key,
      currentSeek = Seek.Read,
      nextSeek = Seek.Read,
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
  def lower(key: Slice[Byte]): IO.Defer[Core.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    findLower(
      key = key,
      currentMap = maps.map,
      otherMaps = maps.queuedMaps.toList
    )

  def contains(key: Slice[Byte]): IO.Defer[Core.Error.Level, Boolean] =
    get(key).mapDeferred(_.isDefined)

  def valueSize(key: Slice[Byte]): IO.Defer[Core.Error.Level, Option[Int]] =
    get(key) mapDeferred {
      result =>
        result map {
          response =>
            response.valueLength
        }
    }

  def bloomFilterKeyValueCount: IO[Core.Error.Level, Int] = {
    val keyValueCountInMaps = maps.keyValueCount.getOrElse(0)
    nextLevel.map(_.bloomFilterKeyValueCount.map(_ + keyValueCountInMaps)) getOrElse IO.Success(keyValueCountInMaps)
  }

  def deadline(key: Slice[Byte]): IO.Defer[Core.Error.Level, Option[Deadline]] =
    get(key) mapDeferred {
      result =>
        result flatMap {
          response =>
            response.deadline
        }
    }

  def sizeOfSegments: Long =
    nextLevel.map(_.sizeOfSegments) getOrElse 0L

  def existsOnDisk: Boolean =
    IOEffect.exists(path)

  def close: IO[Core.Error.Close, Unit] = {
    //    Delay.cancelTimer()
    maps.close onFailureSideEffect {
      exception =>
        logger.error(s"$path: Failed to close maps", exception)
    }
    releaseLocks
    nextLevel.map(_.close) getOrElse IO.unit
  }

  def closeSegments: IO[Core.Error.Level, Unit] =
    nextLevel.map(_.closeSegments()) getOrElse IO.unit

  def mightContainKey(key: Slice[Byte]): IO[Core.Error.Level, Boolean] =
    if (maps.contains(key))
      IO.`true`
    else
      nextLevel.map(_.mightContainKey(key)) getOrElse IO.`true`

  private def findFunctionInMaps(functionId: Slice[Byte]): Boolean =
    maps.find[Boolean] {
      map =>
        Option {
          map.values().asScala exists {
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
    maps.iterator.asScala.foldLeft(maps.map.writeCountStateId)(_ + _.writeCountStateId) >= 10000 ||
      findFunctionInMaps(functionId)

  def mightContainFunction(functionId: Slice[Byte]): IO[Core.Error.Level, Boolean] =
    if (mightContainFunctionInMaps(functionId))
      IO.`true`
    else
      nextLevel
        .map(_.mightContainFunction(functionId))
        .getOrElse(IO.`false`)

  override def hasNextLevel: Boolean =
    nextLevel.isDefined

  override def appendixPath: Path =
    nextLevel.map(_.appendixPath) getOrElse {
      throw new Exception("LevelZero does not have appendix.")
    }

  override def rootPath: Path =
    path

  override def isEmpty: Boolean =
    maps.isEmpty

  override def segmentsCount(): Int =
    nextLevel.map(_.segmentsCount()) getOrElse 0

  override def segmentFilesOnDisk: Seq[Path] =
    nextLevel.map(_.segmentFilesOnDisk) getOrElse Seq.empty

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

  override def stateID: Long =
    maps.stateID

  override def nextCompactionDelay: FiniteDuration =
    throttle(levelZeroMeter)

  override def delete: IO[Core.Error.Delete, Unit] =
    LevelZero.delete(this)
}
