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

package swaydb.core.level.zero

import java.nio.channels.{FileChannel, FileLock}
import java.nio.file.{Path, Paths, StandardOpenOption}
import java.util

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.KeyValue._
import swaydb.core.data.Memory.{Fixed, Range}
import swaydb.core.data._
import swaydb.core.finders.{Get, Higher, Lower}
import swaydb.core.io.file.IO
import swaydb.core.level.LevelRef
import swaydb.core.level.actor.LevelCommand.WakeUp
import swaydb.core.level.actor.LevelZeroAPI
import swaydb.core.map
import swaydb.core.map.{MapEntry, Maps, SkipListMerge}
import swaydb.core.retry.Retry
import swaydb.core.util.MinMax
import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice
import swaydb.data.storage.Level0Storage

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

private[core] object LevelZero extends LazyLogging {

  def apply(mapSize: Long,
            storage: Level0Storage,
            nextLevel: LevelRef,
            acceleration: Level0Meter => Accelerator,
            readRetryLimit: Int)(implicit ordering: Ordering[Slice[Byte]],
                                 ec: ExecutionContext): Try[LevelZero] = {
    import swaydb.core.map.serializer.LevelZeroMapEntryReader.Level0Reader
    import swaydb.core.map.serializer.LevelZeroMapEntryWriter._
    implicit val skipListMerger: SkipListMerge[Slice[Byte], Memory] = LevelZeroSkipListMerge
    implicit val memoryOrdering: Ordering[Memory] = ordering.on[Memory](_.key)
    val mapsAndPathAndLock =
      storage match {
        case Level0Storage.Persistent(mmap, databaseDirectory, recovery) =>
          val path = databaseDirectory.resolve(0.toString)
          IO createDirectoriesIfAbsent path
          logger.info("{}: Acquiring lock.", path)
          val lockFile = path.resolve("LOCK")
          IO createFileIfAbsent lockFile
          Try(FileChannel.open(lockFile, StandardOpenOption.WRITE).tryLock()) flatMap {
            lock =>
              logger.info("{}: Recovering Maps.", path)
              Maps.persistent[Slice[Byte], Memory](path, mmap, mapSize, acceleration, recovery) map {
                maps =>
                  (maps, path, Some(lock))
              }
          }

        case Level0Storage.Memory =>
          Success(Maps.memory[Slice[Byte], Memory](mapSize, acceleration), Paths.get("MEMORY_DB").resolve(0.toString), None)
      }
    mapsAndPathAndLock map {
      case (maps, path, lock: Option[FileLock]) =>
        new LevelZero(path, mapSize, readRetryLimit, maps, nextLevel, lock)
    }
  }
}

private[core] class LevelZero(val path: Path,
                              mapSize: Long,
                              readRetryLimit: Int,
                              val maps: Maps[Slice[Byte], Memory],
                              val nextLevel: LevelRef,
                              lock: Option[FileLock])(implicit ordering: Ordering[Slice[Byte]],
                                                      memoryOrdering: Ordering[Memory],
                                                      ec: ExecutionContext) extends LevelZeroRef with LazyLogging {

  logger.info("{}: Level0 started.", path)

  import ordering._

  implicit val orderOnReadOnly = ordering.on[KeyValue](_.key)

  import swaydb.core.map.serializer.LevelZeroMapEntryWriter._

  private val actor =
    LevelZeroActor(this)

  maps setOnFullListener {
    () =>
      Future(actor ! WakeUp)
  }

  def releaseLocks: Try[Unit] =
    Try(lock.foreach(_.release())) flatMap {
      _ =>
        nextLevel.releaseLocks
    }

  def withRetry[T](tryBlock: => Try[T]): Try[T] =
    Retry[T](resourceId = path.toString, maxRetryLimit = readRetryLimit, until = Retry.levelReadRetryUntil) {
      try
        tryBlock
      catch {
        case ex: Exception =>
          Failure(ex)
      }
    }

  def !(command: LevelZeroAPI): Unit =
    actor ! command

  def assertKey(key: Slice[Byte])(block: => Try[Level0Meter]): Try[Level0Meter] =
    if (key.isEmpty)
      Failure(new IllegalArgumentException("Input key(s) cannot be empty."))
    else
      block

  def put(key: Slice[Byte]): Try[Level0Meter] =
    assertKey(key) {
      maps.write(MapEntry.Put[Slice[Byte], Memory](key, Memory.Put(key, None)))
    }

  def put(key: Slice[Byte], value: Slice[Byte]): Try[Level0Meter] =
    assertKey(key) {
      maps.write(MapEntry.Put(key, Memory.Put(key, value)))
    }

  def put(key: Slice[Byte], value: Option[Slice[Byte]]): Try[Level0Meter] =
    assertKey(key) {
      maps.write(MapEntry.Put(key, Memory.Put(key, value)))
    }

  def put(entry: MapEntry[Slice[Byte], Memory]): Try[Level0Meter] =
    maps write entry

  def remove(key: Slice[Byte]): Try[Level0Meter] =
    assertKey(key) {
      maps.write(MapEntry.Put[Slice[Byte], Memory.Remove](key, Memory.Remove(key)))
    }

  def remove(fromKey: Slice[Byte], untilKey: Slice[Byte]): Try[Level0Meter] =
    assertKey(fromKey) {
      assertKey(untilKey) {
        maps.write(MapEntry.Put[Slice[Byte], Memory.Range](fromKey, Memory.Range(fromKey, untilKey, None, Value.Remove)))
      }
    }

  def update(fromKey: Slice[Byte], untilKey: Slice[Byte], value: Slice[Byte]): Try[Level0Meter] =
    update(fromKey, untilKey, Some(value))

  def update(fromKey: Slice[Byte], untilKey: Slice[Byte], value: Option[Slice[Byte]]): Try[Level0Meter] =
    assertKey(fromKey) {
      assertKey(untilKey) {
        maps.write(MapEntry.Put[Slice[Byte], Memory.Range](fromKey, Memory.Range(fromKey, untilKey, None, Value.Put(value))))
      }
    }

  private def getFromMap(key: Slice[Byte],
                         currentMap: map.Map[Slice[Byte], Memory]): Option[Memory] =
    if (currentMap.hasRange)
      currentMap.floor(key) match {
        case floor @ Some(floorRange: Memory.Range) if key >= floorRange.fromKey && key < floorRange.toKey =>
          floor

        case floor @ Some(floorFixed) if floorFixed.key equiv key =>
          floor

        case _ =>
          None
      }
    else
      currentMap.get(key)

  private def getFromNextLevel(key: Slice[Byte],
                               mapsIterator: util.Iterator[map.Map[Slice[Byte], Memory]]): Try[Option[KeyValue.FindResponse]] =
    if (mapsIterator.hasNext) {
      val next = mapsIterator.next()
      //println(s"Get for key: ${key.readInt()} in ${next.pathOption}")
      find(key, next, mapsIterator)
    } else {
      //println(s"Get for key: ${key.readInt()} in ${nextLevel.rootPath}")
      nextLevel get key
    }

  private def find(key: Slice[Byte],
                   currentMap: map.Map[Slice[Byte], Memory],
                   mapsIterator: util.Iterator[map.Map[Slice[Byte], Memory]]): Try[Option[KeyValue.FindResponse]] =
    Get(
      key = key,
      getInCurrentLevelOnly =
        key =>
          Try(getFromMap(key, currentMap)),
      getFromNextLevel =
        key =>
          getFromNextLevel(key, mapsIterator)
    )

  private def find(key: Slice[Byte]): Try[Option[KeyValue.FindResponse]] =
    find(
      key = key,
      currentMap = maps.map,
      mapsIterator = maps.iterator
    )

  def get(key: Slice[Byte]): Try[Option[Option[Slice[Byte]]]] =
    withRetry {
      find(key) flatMap {
        result =>
          result map {
            response =>
              response.getOrFetchValue map {
                result =>
                  Some(result)
              }
          } getOrElse Success(None)
      }
    }

  def getKey(key: Slice[Byte]): Try[Option[Slice[Byte]]] =
    withRetry {
      find(key).map(_.map(_.key))
    }

  def getKeyValue(key: Slice[Byte]): Try[Option[KeyValueTuple]] =
    withRetry {
      find(key) flatMap {
        result =>
          result map {
            response =>
              response.getOrFetchValue map {
                result =>
                  Some(response.key, result)
              }
          } getOrElse Success(None)
      }
    }

  def firstKeyFromMaps =
    maps.reduce[Slice[Byte]](_.firstKey, MinMax.min(_, _))

  def lastKeyFromMaps =
    maps.reduce[Slice[Byte]](
      matcher =
        map =>
          map.lastValue() map {
            case fixed: Fixed =>
              fixed.key
            case range: Range =>
              range.toKey
          },
      reduce = MinMax.max(_, _)
    )

  def head: Try[Option[KeyValueTuple]] =
    withRetry {
      findHead flatMap {
        result =>
          result map {
            response =>
              response.getOrFetchValue map {
                result =>
                  Some(response.key, result)
              }
          } getOrElse Success(None)
      }
    }

  def headKey: Try[Option[Slice[Byte]]] =
    withRetry {
      findHead.map(_.map(_.key))
    }

  def last: Try[Option[KeyValueTuple]] =
    withRetry {
      findLast flatMap {
        result =>
          result map {
            response =>
              response.getOrFetchValue map {
                result =>
                  Some(response.key, result)
              }
          } getOrElse Success(None)
      }
    }

  def lastKey: Try[Option[Slice[Byte]]] =
    withRetry {
      findLast.map(_.map(_.key))
    }

  def findHead: Try[Option[KeyValue.FindResponse]] =
    MinMax.min(firstKeyFromMaps, nextLevel.firstKey).map(ceiling) getOrElse Success(None)

  def findLast: Try[Option[KeyValue.FindResponse]] =
    MinMax.max(lastKeyFromMaps, nextLevel.lastKey).map(floor) getOrElse Success(None)

  def ceiling(key: Slice[Byte]): Try[Option[FindResponse]] =
    ceiling(key, maps.map, maps.iterator.asScala.toList)

  def ceiling(key: Slice[Byte],
              currentMap: map.Map[Slice[Byte], Memory],
              otherMaps: List[map.Map[Slice[Byte], Memory]]): Try[Option[FindResponse]] =
    find(key, currentMap, otherMaps.iterator.asJava) flatMap {
      found =>
        if (found.isDefined)
          Success(found)
        else
          findHigher(key, currentMap, otherMaps)
    }

  def floor(key: Slice[Byte]): Try[Option[FindResponse]] =
    floor(key, maps.map, maps.iterator.asScala.toList)

  def floor(key: Slice[Byte],
            currentMap: map.Map[Slice[Byte], Memory],
            otherMaps: List[map.Map[Slice[Byte], Memory]]): Try[Option[FindResponse]] =
    find(key, currentMap, otherMaps.iterator.asJava) flatMap {
      found =>
        if (found.isDefined)
          Success(found)
        else
          findLower(key, currentMap, otherMaps)
    }

  private def higherFromMap(key: Slice[Byte],
                            currentMap: map.Map[Slice[Byte], Memory]): Option[Memory] =
    if (currentMap.hasRange)
      currentMap.floor(key) match {
        case floor @ Some(floorRange: Memory.Range) if key >= floorRange.fromKey && key < floorRange.toKey =>
          floor

        case _ =>
          currentMap.higherValue(key)
      }
    else
      currentMap.higher(key).map(_._2)

  def findHigherInNextLevel(key: Slice[Byte],
                            otherMaps: List[map.Map[Slice[Byte], Memory]]): Try[Option[KeyValue.FindResponse]] =
    otherMaps.headOption match {
      case Some(nextMap) =>
        //println(s"Finding higher for key: ${key.readInt()} in ${nextMap.pathOption}")
        findHigher(key, nextMap, otherMaps.drop(1))
      case None =>
        //println(s"Finding higher for key: ${key.readInt()} in ${nextLevel.rootPath}")
        nextLevel higher key
    }

  def findHigher(key: Slice[Byte],
                 currentMap: map.Map[Slice[Byte], Memory],
                 otherMaps: List[map.Map[Slice[Byte], Memory]]): Try[Option[KeyValue.FindResponse]] =
    Higher(
      key = key,
      higherInCurrentLevelOnly =
        key =>
          Try(higherFromMap(key, currentMap)),
      ceiling =
        key =>
          ceiling(key, currentMap, otherMaps),
      higherInNextLevel =
        key =>
          findHigherInNextLevel(key, otherMaps)
    )

  /**
    * Higher cannot use an iterator because a single Map can get read requests multiple times for cases where a Map contains a range
    * to fetch ceiling key.
    *
    * Higher queries require iteration of all maps anyway so a full initial conversion to a List is acceptable.
    */
  def findHigher(key: Slice[Byte]): Try[Option[KeyValue.FindResponse]] =
    findHigher(
      key = key,
      currentMap = maps.map,
      otherMaps = maps.iterator.asScala.toList
    )

  def higher(key: Slice[Byte]): Try[Option[KeyValueTuple]] =
    withRetry {
      findHigher(key) flatMap {
        result =>
          result map {
            response =>
              response.getOrFetchValue map {
                result =>
                  Some(response.key, result)
              }
          } getOrElse Success(None)
      }
    }

  def higherKey(key: Slice[Byte]): Try[Option[Slice[Byte]]] =
    withRetry {
      findHigher(key).map(_.map(_.key))
    }

  private def lowerFromMap(key: Slice[Byte],
                           currentMap: map.Map[Slice[Byte], Memory]): Option[Memory] =
    currentMap.lower(key).map(_._2)

  def findLowerInNextLevel(key: Slice[Byte],
                           otherMaps: List[map.Map[Slice[Byte], Memory]]): Try[Option[KeyValue.FindResponse]] =
    otherMaps.headOption match {
      case Some(nextMap) =>
        //println(s"Finding lower for key: ${key.readInt()} in ${nextMap.pathOption}")
        findLower(key, nextMap, otherMaps.drop(1))
      case None =>
        //println(s"Finding lower for key: ${key.readInt()} in ${nextLevel.rootPath}")
        nextLevel lower key
    }

  def findLower(key: Slice[Byte],
                currentMap: map.Map[Slice[Byte], Memory],
                otherMaps: List[map.Map[Slice[Byte], Memory]]): Try[Option[KeyValue.FindResponse]] =
    Lower(
      key = key,
      lowerInCurrentLevelOnly =
        key =>
          Try(lowerFromMap(key, currentMap)),
      lowerInNextLevel =
        key =>
          findLowerInNextLevel(key, otherMaps)
    )

  /**
    * Lower cannot use an iterator because a single Map can get read requests multiple times for cases where a Map contains a range
    * to fetch ceiling key.
    *
    * Lower queries require iteration of all maps anyway so a full initial conversion to a List is acceptable.
    */
  def findLower(key: Slice[Byte]): Try[Option[KeyValue.FindResponse]] =
    findLower(
      key = key,
      currentMap = maps.map,
      otherMaps = maps.iterator.asScala.toList
    )

  def lower(key: Slice[Byte]): Try[Option[KeyValueTuple]] =
    withRetry {
      findLower(key) flatMap {
        result =>
          result map {
            response =>
              response.getOrFetchValue map {
                result =>
                  Some(response.key, result)
              }
          } getOrElse Success(None)
      }
    }

  def lowerKey(key: Slice[Byte]): Try[Option[Slice[Byte]]] =
    withRetry {
      findLower(key).map(_.map(_.key))
    }

  def contains(key: Slice[Byte]): Try[Boolean] =
    withRetry {
      find(key).map(_.isDefined)
    }

  def valueSize(key: Slice[Byte]): Try[Option[Int]] =
    find(key) map {
      result =>
        result map {
          response =>
            response.valueLength
        }
    }

  def keyValueCount: Try[Int] =
    withRetry {
      val keyValueCountInMaps = maps.keyValueCount.getOrElse(0)
      nextLevel.keyValueCount.map(_ + keyValueCountInMaps)
    }

  override def sizeOfSegments: Long =
    nextLevel.sizeOfSegments

  override def beforeKey(key: Slice[Byte]): Try[Option[Slice[Byte]]] =
    lowerKey(key)

  override def before(key: Slice[Byte]): Try[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    lower(key)

  override def afterKey(key: Slice[Byte]): Try[Option[Slice[Byte]]] =
    higherKey(key)

  override def after(key: Slice[Byte]): Try[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    higher(key)

  def existsOnDisk: Boolean =
    IO.exists(path)

  def close: Try[Unit] = {
    //    Delay.cancelTimer()
    maps.close.failed foreach {
      exception =>
        logger.error(s"$path: Failed to close maps", exception)
    }
    nextLevel.close
  }

  def closeSegments: Try[Unit] =
    nextLevel.closeSegments()

  override def level0Meter: Level0Meter =
    maps.getMeter

  override def level1Meter: LevelMeter =
    nextLevel.meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    nextLevel.meterFor(levelNumber)

  override def mightContain(key: Slice[Byte]): Try[Boolean] =
    withRetry {
      if (maps.contains(key))
        Success(true)
      else
        nextLevel mightContain key
    }
}