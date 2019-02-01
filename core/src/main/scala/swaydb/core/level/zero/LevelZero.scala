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

package swaydb.core.level.zero

import com.typesafe.scalalogging.LazyLogging
import java.nio.channels.{FileChannel, FileLock}
import java.nio.file.{Path, Paths, StandardOpenOption}
import java.util
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.{Deadline, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import swaydb.core.data.KeyValue._
import swaydb.core.data._
import swaydb.core.seek._
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.IOOps
import swaydb.core.level.actor.LevelCommand.WakeUp
import swaydb.core.level.actor.{LevelAPI, LevelZeroAPI}
import swaydb.core.level.{LevelRef, PathsDistributor}
import swaydb.core.map
import swaydb.core.map.{MapEntry, Maps, SkipListMerger}
import swaydb.core.queue.FileLimiter
import swaydb.core.retry.Retry
import swaydb.core.segment.Segment
import swaydb.core.util.{MinMax, TryUtil}
import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.storage.Level0Storage

private[core] object LevelZero extends LazyLogging {

  def withRetry[T](resourceId: String, readRetryLimit: Int)(tryBlock: => Try[T]): Try[T] =
    Retry[T](resourceId = resourceId, maxRetryLimit = readRetryLimit, until = Retry.levelReadRetryUntil) {
      try
        tryBlock
      catch {
        case ex: Exception =>
          Failure(ex)
      }
    }

  def apply(mapSize: Long,
            storage: Level0Storage,
            nextLevel: Option[LevelRef],
            acceleration: Level0Meter => Accelerator,
            readRetryLimit: Int,
            throttleOn: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                 timeOrder: TimeOrder[Slice[Byte]],
                                 limiter: FileLimiter,
                                 functionStore: FunctionStore,
                                 ec: ExecutionContext): Try[LevelZero] = {
    import swaydb.core.map.serializer.LevelZeroMapEntryReader.Level0Reader
    import swaydb.core.map.serializer.LevelZeroMapEntryWriter._
    implicit val skipListMerger: SkipListMerger[Slice[Byte], Memory.SegmentResponse] = LevelZeroSkipListMerger
    implicit val memoryOrdering: Ordering[Memory] = keyOrder.on[Memory](_.key)
    val mapsAndPathAndLock =
      storage match {
        case Level0Storage.Persistent(mmap, databaseDirectory, recovery) =>
          val path = databaseDirectory.resolve(0.toString)
          IOOps createDirectoriesIfAbsent path
          logger.info("{}: Acquiring lock.", path)
          val lockFile = path.resolve("LOCK")
          IOOps createFileIfAbsent lockFile
          Try(FileChannel.open(lockFile, StandardOpenOption.WRITE).tryLock()) flatMap {
            lock =>
              logger.info("{}: Recovering Maps.", path)
              Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap, mapSize, acceleration, recovery) map {
                maps =>
                  (maps, path, Some(lock))
              }
          }

        case Level0Storage.Memory =>
          Success(Maps.memory[Slice[Byte], Memory.SegmentResponse](mapSize, acceleration), Paths.get("MEMORY_DB").resolve(0.toString), None)
      }
    mapsAndPathAndLock map {
      case (maps, path, lock: Option[FileLock]) =>
        new LevelZero(
          path = path,
          mapSize = mapSize,
          readRetryLimit = readRetryLimit,
          maps = maps,
          throttleOn = throttleOn,
          nextLevel = nextLevel,
          lock = lock
        )
    }
  }
}

private[core] class LevelZero(val path: Path,
                              mapSize: Long,
                              val readRetryLimit: Int,
                              val maps: Maps[Slice[Byte], Memory.SegmentResponse],
                              val throttleOn: Boolean,
                              val nextLevel: Option[LevelRef],
                              lock: Option[FileLock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                      functionStore: FunctionStore,
                                                      memoryOrdering: Ordering[Memory],
                                                      ec: ExecutionContext) extends LevelRef with LazyLogging {

  logger.info("{}: Level0 started.", path)

  import keyOrder._
  import swaydb.core.map.serializer.LevelZeroMapEntryWriter._

  //LevelZero can also implement PathsDistributor to spread the Maps over to multiple paths.
  override def paths: PathsDistributor =
    nextLevel.map(_.paths) getOrElse PathsDistributor.empty

  //Currently not used for Level0.
  override def throttle: LevelMeter => Throttle =
    nextLevel.map(_.throttle) getOrElse {
      _: LevelMeter => Throttle(0.second, 0)
    }

  private val actor: Option[LevelZeroActor] =
    if (!throttleOn)
      None
    else
      nextLevel map {
        nextLevel =>
          LevelZeroActor(this, nextLevel)
      }

  actor foreach {
    actor =>
      maps setOnFullListener {
        () =>
          Future(actor ! WakeUp)
      }
  }

  def releaseLocks: Try[Unit] =
    IOOps.release(lock) flatMap {
      _ =>
        nextLevel.map(_.releaseLocks) getOrElse TryUtil.successUnit
    }

  def withRetry[T](tryBlock: => Try[T]): Try[T] =
    LevelZero.withRetry(resourceId = path.toString, readRetryLimit = readRetryLimit)(tryBlock = tryBlock)

  def !(command: LevelZeroAPI): Unit =
    actor.foreach(_ ! command)

  override def !(request: LevelAPI): Unit =
    nextLevel.foreach(_ ! request)

  def assertKey(key: Slice[Byte])(block: => Try[Level0Meter]): Try[Level0Meter] =
    if (key.isEmpty)
      Failure(new IllegalArgumentException("Input key(s) cannot be empty."))
    else
      block

  def put(key: Slice[Byte]): Try[Level0Meter] =
    assertKey(key) {
      maps.write(MapEntry.Put[Slice[Byte], Memory.SegmentResponse](key, Memory.Put(key, None, None, Time.empty)))
    }

  def put(key: Slice[Byte], value: Slice[Byte]): Try[Level0Meter] =
    assertKey(key) {
      maps.write(MapEntry.Put(key, Memory.Put(key, Some(value), None, Time.empty)))
    }

  def put(key: Slice[Byte], value: Option[Slice[Byte]], removeAt: Deadline): Try[Level0Meter] =
    assertKey(key) {
      maps.write(MapEntry.Put(key, Memory.Put(key, value, Some(removeAt), Time.empty)))
    }

  def put(key: Slice[Byte], value: Option[Slice[Byte]]): Try[Level0Meter] =
    assertKey(key) {
      maps.write(MapEntry.Put(key, Memory.Put(key, value, None, Time.empty)))
    }

  def put(entry: MapEntry[Slice[Byte], Memory.SegmentResponse]): Try[Level0Meter] =
    maps write entry

  def remove(key: Slice[Byte]): Try[Level0Meter] =
    assertKey(key) {
      maps.write(MapEntry.Put[Slice[Byte], Memory.Remove](key, Memory.Remove(key, None, Time.empty)))
    }

  def remove(key: Slice[Byte], at: Deadline): Try[Level0Meter] =
    assertKey(key) {
      maps.write(MapEntry.Put[Slice[Byte], Memory.Remove](key, Memory.Remove(key, Some(at), Time.empty)))
    }

  def remove(fromKey: Slice[Byte], to: Slice[Byte]): Try[Level0Meter] =
    assertKey(fromKey) {
      assertKey(to) {
        if (fromKey >= to)
          Failure(new Exception("fromKey should be less than toKey"))
        else
          maps.write {
            (MapEntry.Put[Slice[Byte], Memory.Range](fromKey, Memory.Range(fromKey, to, None, Value.Remove(None, Time.empty))): MapEntry[Slice[Byte], Memory.SegmentResponse]) ++
              MapEntry.Put[Slice[Byte], Memory.Remove](to, Memory.Remove(to, None, Time.empty))
          }
      }
    }

  def remove(fromKey: Slice[Byte], to: Slice[Byte], at: Deadline): Try[Level0Meter] =
    assertKey(fromKey) {
      assertKey(to) {
        if (fromKey >= to)
          Failure(new Exception("fromKey should be less than toKey"))
        else
          maps.write {
            (MapEntry.Put[Slice[Byte], Memory.Range](fromKey, Memory.Range(fromKey, to, None, Value.Remove(Some(at), Time.empty))): MapEntry[Slice[Byte], Memory.SegmentResponse]) ++
              MapEntry.Put[Slice[Byte], Memory.Remove](to, Memory.Remove(to, Some(at), Time.empty))
          }
      }
    }

  def update(key: Slice[Byte], value: Slice[Byte]): Try[Level0Meter] =
    assertKey(key) {
      maps.write(MapEntry.Put(key, Memory.Update(key, Some(value), None, Time.empty)))
    }

  def update(key: Slice[Byte], value: Option[Slice[Byte]]): Try[Level0Meter] =
    assertKey(key) {
      maps.write(MapEntry.Put(key, Memory.Update(key, value, None, Time.empty)))
    }

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Slice[Byte]): Try[Level0Meter] =
    update(fromKey, to, Some(value))

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Option[Slice[Byte]]): Try[Level0Meter] =
    assertKey(fromKey) {
      assertKey(to) {
        if (fromKey >= to)
          Failure(new Exception("fromKey should be less than toKey"))
        else
          maps.write {
            (MapEntry.Put[Slice[Byte], Memory.Range](
              key = fromKey,
              value = Memory.Range(
                fromKey = fromKey,
                toKey = to,
                fromValue = None,
                rangeValue = Value.Update(value, None, Time.empty)
              )
            ): MapEntry[Slice[Byte], Memory.SegmentResponse]) ++ MapEntry.Put[Slice[Byte], Memory.Update](to, Memory.Update(to, value, None, Time.empty))
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
                               mapsIterator: util.Iterator[map.Map[Slice[Byte], Memory.SegmentResponse]]): Try[Option[KeyValue.ReadOnly.Put]] =
    if (mapsIterator.hasNext)
      find(key, mapsIterator.next(), mapsIterator)
    else
      nextLevel.map(_ get key) getOrElse TryUtil.successNone

  def currentGetter(currentMap: map.Map[Slice[Byte], Memory.SegmentResponse]) =
    new CurrentGetter {
      override def get(key: Slice[Byte]): Try[Option[ReadOnly.SegmentResponse]] =
        Try(getFromMap(key, currentMap))
    }

  def newGetter(mapsIterator: util.Iterator[map.Map[Slice[Byte], Memory.SegmentResponse]]) =
    new NextGetter {
      override def get(key: Slice[Byte]): Try[Option[ReadOnly.Put]] =
        getFromNextLevel(key, mapsIterator)
    }

  private def find(key: Slice[Byte],
                   currentMap: map.Map[Slice[Byte], Memory.SegmentResponse],
                   mapsIterator: util.Iterator[map.Map[Slice[Byte], Memory.SegmentResponse]]): Try[Option[KeyValue.ReadOnly.Put]] =
    Get.seek(
      key = key,
      currentGetter = currentGetter(currentMap),
      nextGetter = newGetter(mapsIterator)
    )

  def get(key: Slice[Byte]): Try[Option[ReadOnly.Put]] =
    withRetry {
      find(
        key = key,
        currentMap = maps.map,
        mapsIterator = maps.iterator
      )
    }

  def getKey(key: Slice[Byte]): Try[Option[Slice[Byte]]] =
    withRetry {
      get(key).map(_.map(_.key))
    }

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

  def head: Try[Option[ReadOnly.Put]] =
    withRetry {
      findHead
    }

  def last: Try[Option[ReadOnly.Put]] =
    withRetry {
      findLast
    }

  def lastKey: Try[Option[Slice[Byte]]] =
    last.map(_.map(_.key))

  override def headKey: Try[Option[Slice[Byte]]] =
    head.map(_.map(_.key))

  def findHead: Try[Option[KeyValue.ReadOnly.Put]] =
    nextLevel map {
      nextLevel =>
        nextLevel.headKey flatMap {
          nextLevelFirstKey =>
            MinMax.min(firstKeyFromMaps, nextLevelFirstKey)(keyOrder).map(ceiling) getOrElse TryUtil.successNone
        }
    } getOrElse TryUtil.successNone

  def findLast: Try[Option[KeyValue.ReadOnly.Put]] =
    nextLevel map {
      nextLevel =>
        nextLevel.lastKey flatMap {
          nextLevelLastKey =>
            MinMax.max(lastKeyFromMaps, nextLevelLastKey)(keyOrder).map(floor) getOrElse TryUtil.successNone
        }

    } getOrElse TryUtil.successNone

  def ceiling(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly.Put]] =
    ceiling(key, maps.map, maps.iterator.asScala.toList)

  def ceiling(key: Slice[Byte],
              currentMap: map.Map[Slice[Byte], Memory.SegmentResponse],
              otherMaps: List[map.Map[Slice[Byte], Memory.SegmentResponse]]): Try[Option[KeyValue.ReadOnly.Put]] =
    find(key, currentMap, otherMaps.iterator.asJava) flatMap {
      found =>
        if (found.isDefined)
          Success(found)
        else
          findHigher(key, currentMap, otherMaps)
    }

  def floor(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly.Put]] =
    floor(key, maps.map, maps.iterator.asScala.toList)

  def floor(key: Slice[Byte],
            currentMap: map.Map[Slice[Byte], Memory.SegmentResponse],
            otherMaps: List[map.Map[Slice[Byte], Memory.SegmentResponse]]): Try[Option[KeyValue.ReadOnly.Put]] =
    find(key, currentMap, otherMaps.iterator.asJava) flatMap {
      found =>
        if (found.isDefined)
          Success(found)
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
                            otherMaps: List[map.Map[Slice[Byte], Memory.SegmentResponse]]): Try[Option[KeyValue.ReadOnly.Put]] =
    otherMaps.headOption match {
      case Some(nextMap) =>
        //        println(s"Finding higher for key: ${key.readInt()} in Map: ${nextMap.pathOption}. Remaining map: ${otherMaps.size}")
        findHigher(key, nextMap, otherMaps.drop(1))
      case None =>
        //        println(s"Finding higher for key: ${key.readInt()} in ${nextLevel.rootPath}")
        nextLevel.map(_.higher(key)) getOrElse TryUtil.successNone
    }

  def currentWalker(currentMap: map.Map[Slice[Byte], Memory.SegmentResponse],
                    otherMaps: List[map.Map[Slice[Byte], Memory.SegmentResponse]]) =
    new CurrentWalker {
      override def get(key: Slice[Byte]): Try[Option[ReadOnly.Put]] =
        find(key, currentMap, otherMaps.asJava.iterator())

      override def higher(key: Slice[Byte]): Try[Option[ReadOnly.SegmentResponse]] =
        Try(higherFromMap(key, currentMap))

      override def lower(key: Slice[Byte]): Try[Option[ReadOnly.SegmentResponse]] =
        Try(lowerFromMap(key, currentMap))
    }

  def nextWalker(otherMaps: List[map.Map[Slice[Byte], Memory.SegmentResponse]]) =
    new NextWalker {
      override def higher(key: Slice[Byte]): Try[Option[ReadOnly.Put]] =
        findHigherInNextLevel(key, otherMaps)

      override def lower(key: Slice[Byte]): Try[Option[ReadOnly.Put]] =
        findLowerInNextLevel(key, otherMaps)

      override def get(key: Slice[Byte]): Try[Option[ReadOnly.Put]] =
        getFromNextLevel(key, otherMaps.iterator.asJava)
    }

  def findHigher(key: Slice[Byte],
                 currentMap: map.Map[Slice[Byte], Memory.SegmentResponse],
                 otherMaps: List[map.Map[Slice[Byte], Memory.SegmentResponse]]): Try[Option[KeyValue.ReadOnly.Put]] =
    Higher.seek(
      key = key,
      currentSeek = Seek.Next,
      nextSeek = Seek.Next,
      currentWalker = currentWalker(currentMap, otherMaps),
      nextWalker = nextWalker(otherMaps),
      keyOrder = keyOrder,
      timeOrder = timeOrder,
      functionStore = functionStore
    )

  /**
    * Higher cannot use an iterator because a single Map can get read requests multiple times for cases where a Map contains a range
    * to fetch ceiling key.
    *
    * Higher queries require iteration of all maps anyway so a full initial conversion to a List is acceptable.
    */
  def findHigher(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly.Put]] =
    findHigher(
      key = key,
      currentMap = maps.map,
      otherMaps = maps.queuedMaps.toList
    )

  def higher(key: Slice[Byte]): Try[Option[ReadOnly.Put]] =
    withRetry {
      findHigher(key)
    }

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
                           otherMaps: List[map.Map[Slice[Byte], Memory.SegmentResponse]]): Try[Option[KeyValue.ReadOnly.Put]] =
    otherMaps.headOption match {
      case Some(nextMap) =>
        //println(s"Finding lower for key: ${key.readInt()} in ${nextMap.pathOption}")
        findLower(key, nextMap, otherMaps.drop(1))
      case None =>
        //println(s"Finding lower for key: ${key.readInt()} in ${nextLevel.rootPath}")
        nextLevel.map(_.lower(key)) getOrElse TryUtil.successNone
    }

  def findLower(key: Slice[Byte],
                currentMap: map.Map[Slice[Byte], Memory.SegmentResponse],
                otherMaps: List[map.Map[Slice[Byte], Memory.SegmentResponse]]): Try[Option[KeyValue.ReadOnly.Put]] =
    Lower.seek(
      key = key,
      currentSeek = Seek.Next,
      nextSeek = Seek.Next,
      currentWalker = currentWalker(currentMap, otherMaps),
      nextWalker = nextWalker(otherMaps),
      keyOrder = keyOrder,
      timeOrder = timeOrder,
      functionStore = functionStore
    )

  /**
    * Lower cannot use an iterator because a single Map can get read requests multiple times for cases where a Map contains a range
    * to fetch ceiling key.
    *
    * Lower queries require iteration of all maps anyway so a full initial conversion to a List is acceptable.
    */
  def findLower(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly.Put]] =
    findLower(
      key = key,
      currentMap = maps.map,
      otherMaps = maps.queuedMaps.toList
    )

  def lower(key: Slice[Byte]): Try[Option[ReadOnly.Put]] =
    withRetry {
      findLower(key)
    }

  def contains(key: Slice[Byte]): Try[Boolean] =
    withRetry {
      get(key).map(_.isDefined)
    }

  def valueSize(key: Slice[Byte]): Try[Option[Int]] =
    withRetry {
      get(key) map {
        result =>
          result map {
            response =>
              response.valueLength
          }
      }
    }

  def bloomFilterKeyValueCount: Try[Int] =
    withRetry {
      val keyValueCountInMaps = maps.keyValueCount.getOrElse(0)
      nextLevel.map(_.bloomFilterKeyValueCount.map(_ + keyValueCountInMaps)) getOrElse Success(keyValueCountInMaps)
    }

  def deadline(key: Slice[Byte]): Try[Option[Deadline]] =
    withRetry {
      get(key) map {
        result =>
          result flatMap {
            response =>
              response.deadline
          }
      }
    }

  def sizeOfSegments: Long =
    nextLevel.map(_.sizeOfSegments) getOrElse 0L

  def existsOnDisk: Boolean =
    IOOps.exists(path)

  def close: Try[Unit] = {
    //    Delay.cancelTimer()
    maps.close.failed foreach {
      exception =>
        logger.error(s"$path: Failed to close maps", exception)
    }
    releaseLocks
    nextLevel.map(_.close) getOrElse TryUtil.successUnit map {
      _ =>
        actor.foreach(_.terminate())
    }
  }

  def closeSegments: Try[Unit] =
    nextLevel.map(_.closeSegments()) getOrElse TryUtil.successUnit

  def level0Meter: Level0Meter =
    maps.getMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    nextLevel.flatMap(_.meterFor(levelNumber))

  def mightContain(key: Slice[Byte]): Try[Boolean] =
    withRetry {
      if (maps.contains(key))
        Success(true)
      else
        nextLevel.map(_.mightContain(key)) getOrElse TryUtil.successTrue
    }

  override def segmentsInLevel(): Iterable[Segment] =
    nextLevel.map(_.segmentsInLevel()) getOrElse Iterable.empty

  override def hasNextLevel: Boolean =
    nextLevel.isDefined

  override def appendixPath: Path =
    nextLevel.map(_.appendixPath) getOrElse {
      throw new Exception("LevelZero does not have appendix.")
    }

  override def rootPath: Path =
    path

  override def takeSegments(size: Int, condition: Segment => Boolean): Iterable[Segment] =
    nextLevel.map(_.takeSegments(size, condition)) getOrElse Iterable.empty

  override def isEmpty: Boolean =
    maps.isEmpty

  override def segmentsCount(): Int =
    nextLevel.map(_.segmentsCount()) getOrElse 0

  override def segmentFilesOnDisk: Seq[Path] =
    nextLevel.map(_.segmentFilesOnDisk) getOrElse Seq.empty

  override def take(count: Int): Slice[Segment] =
    nextLevel.map(_.take(count)) getOrElse Slice.empty

  override def foreach[T](f: (Slice[Byte], Segment) => T): Unit =
    nextLevel.foreach(_.foreach(f))

  override def containsSegmentWithMinKey(minKey: Slice[Byte]): Boolean =
    nextLevel.exists(_.containsSegmentWithMinKey(minKey))

  override def getSegment(minKey: Slice[Byte]): Option[Segment] =
    nextLevel.flatMap(_.getSegment(minKey))

  override def getBusySegments(): List[Segment] =
    nextLevel.map(_.getBusySegments()) getOrElse List.empty

  override def takeSmallSegments(size: Int): Iterable[Segment] =
    nextLevel.map(_.takeSmallSegments(size)) getOrElse List.empty

  override def takeLargeSegments(size: Int): Iterable[Segment] =
    nextLevel.map(_.takeLargeSegments(size)) getOrElse List.empty

  override def levelSize: Long =
    nextLevel.map(_.levelSize) getOrElse 0

  override def segmentCountAndLevelSize: (Int, Long) =
    nextLevel.map(_.segmentCountAndLevelSize) getOrElse ((0, 0))

  override def meter: LevelMeter =
    nextLevel.map(_.meter) getOrElse LevelMeter(0, 0)

  override def meterFor(levelNumber: Int): Option[LevelMeter] =
    nextLevel.flatMap(_.meterFor(levelNumber))

  override def isTrash: Boolean =
    false

  override def levelNumber: Long = 0
}
