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

package swaydb.core.map

import java.nio.file.Path
import java.util.concurrent.ConcurrentLinkedDeque
import com.typesafe.scalalogging.LazyLogging
import swaydb.core.brake.BrakePedal
import swaydb.core.map.serializer.{MapEntryReader, MapEntryWriter}
import swaydb.core.io.file.IOEffect._
import swaydb.data.io.IO._
import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.config.RecoveryMode
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import swaydb.core.data.Time
import swaydb.data.io.IO
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.IOEffect
import swaydb.core.queue.FileLimiter
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

private[core] object Maps extends LazyLogging {

  def memory[K, V: ClassTag](fileSize: Long,
                             acceleration: Level0Meter => Accelerator)(implicit keyOrder: KeyOrder[K],
                                                                       timeOrder: TimeOrder[Slice[Byte]],
                                                                       limiter: FileLimiter,
                                                                       functionStore: FunctionStore,
                                                                       mapReader: MapEntryReader[MapEntry[K, V]],
                                                                       writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                       skipListMerger: SkipListMerger[K, V],
                                                                       ec: ExecutionContext): Maps[K, V] =
    new Maps[K, V](
      maps = new ConcurrentLinkedDeque[Map[K, V]](),
      fileSize = fileSize,
      acceleration = acceleration,
      currentMap = Map.memory[K, V](fileSize, flushOnOverflow = false)
    )

  def persistent[K, V: ClassTag](path: Path,
                                 mmap: Boolean,
                                 fileSize: Long,
                                 acceleration: Level0Meter => Accelerator,
                                 recovery: RecoveryMode)(implicit keyOrder: KeyOrder[K],
                                                         timeOrder: TimeOrder[Slice[Byte]],
                                                         limiter: FileLimiter,
                                                         functionStore: FunctionStore,
                                                         writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                         reader: MapEntryReader[MapEntry[K, V]],
                                                         skipListMerger: SkipListMerger[K, V],
                                                         ec: ExecutionContext): IO[Maps[K, V]] = {
    logger.debug("{}: Maps persistent started. Initialising recovery.", path)
    //reverse to keep the newest maps at the top.
    recover[K, V](path, mmap, fileSize, recovery).map(_.reverse) flatMap {
      recoveredMapsReversed =>
        logger.info(s"{}: Recovered {} maps.", path, recoveredMapsReversed.size)
        val nextMapId =
          recoveredMapsReversed.headOption match {
            case Some(lastMaps) =>
              lastMaps match {
                case PersistentMap(path, _, _, _, _, _) =>
                  path.incrementFolderId
                case _ =>
                  path.resolve(0.toFolderId)
              }
            case None =>
              path.resolve(0.toFolderId)
          }
        //delete maps that are empty.
        val (emptyMaps, otherMaps) = recoveredMapsReversed.partition(_.isEmpty)
        if (emptyMaps.nonEmpty) logger.info(s"{}: Deleting empty {} maps {}.", path, emptyMaps.size, emptyMaps.flatMap(_.pathOption).map(_.toString).mkString(", "))
        emptyMaps foreachIO (_.delete) match {
          case Some(IO.Failure(error)) =>
            logger.error(s"{}: Failed to delete empty maps {}", path, emptyMaps.flatMap(_.pathOption).map(_.toString).mkString(", "))
            IO.Failure(error)

          case None =>
            logger.debug(s"{}: Creating next map with ID {} maps.", path, nextMapId)
            val queue = new ConcurrentLinkedDeque[Map[K, V]](otherMaps.asJavaCollection)
            Map.persistent[K, V](nextMapId, mmap, flushOnOverflow = false, fileSize, recovery.drop) map {
              nextMap =>
                logger.debug(s"{}: Next map created with ID {}.", path, nextMapId)
                new Maps[K, V](queue, fileSize, acceleration, nextMap.item)
            }
        }
    }
  }

  private def recover[K, V: ClassTag](folder: Path,
                                      mmap: Boolean,
                                      fileSize: Long,
                                      recovery: RecoveryMode)(implicit keyOrder: KeyOrder[K],
                                                              timeOrder: TimeOrder[Slice[Byte]],
                                                              fileOpenLimiter: FileLimiter,
                                                              functionStore: FunctionStore,
                                                              writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                              mapReader: MapEntryReader[MapEntry[K, V]],
                                                              skipListMerger: SkipListMerger[K, V],
                                                              ec: ExecutionContext): IO[Seq[Map[K, V]]] = {
    /**
      * Performs corruption handling based on the the value set for [[RecoveryMode]].
      */
    def applyRecoveryMode(exception: Throwable,
                          mapPath: Path,
                          otherMapsPaths: List[Path],
                          recoveredMaps: ListBuffer[Map[K, V]]): IO[Seq[Map[K, V]]] =
      exception match {
        case exception: IllegalStateException =>
          recovery match {
            case RecoveryMode.ReportFailure =>
              //return failure immediately without effecting the current state of Level0
              IO.Failure(exception)

            case RecoveryMode.DropCorruptedTailEntries =>
              //Ignore the corrupted file and jump to to the next Map.
              //The recovery itself should've read most of the non-corrupted head entries on best effort basis
              //and added to the recoveredMaps.
              doRecovery(otherMapsPaths, recoveredMaps)

            case RecoveryMode.DropCorruptedTailEntriesAndMaps =>
              //skip and delete all the files after the corruption file and return the successfully recovered maps
              //if the files were deleted successfully.
              logger.info(s"{}: Skipping files after corrupted file. Recovery mode: {}", mapPath, recovery.name)
              otherMapsPaths foreachIO { //delete Maps after the corruption.
                mapPath =>
                  IOEffect.walkDelete(mapPath) match {
                    case IO.Success(_) =>
                      logger.info(s"{}: Deleted file after corruption. Recovery mode: {}", mapPath, recovery.name)
                      IO.unit

                    case IO.Failure(error) =>
                      logger.error(s"{}: IO.Failure to delete file after corruption file. Recovery mode: {}", mapPath, recovery.name)
                      IO.Failure(error)
                  }
              } match {
                case Some(IO.Failure(error)) =>
                  IO.Failure(error)

                case None =>
                  IO.Success(recoveredMaps)
              }
          }

        case exception =>
          IO.Failure(exception)
      }

    /**
      * Start recovery for all the input maps.
      */
    @tailrec
    def doRecovery(maps: List[Path],
                   recoveredMaps: ListBuffer[Map[K, V]]): IO[Seq[Map[K, V]]] =
      maps match {
        case Nil =>
          IO.Success(recoveredMaps)

        case mapPath :: otherMapsPaths =>
          logger.info(s"{}: Recovering.", mapPath)

          Map.persistent[K, V](mapPath, mmap, flushOnOverflow = false, fileSize, recovery.drop) match {
            case IO.Success(recoveredMap) =>
              //recovered immutable memory map's files should be closed after load as they are always read from in memory
              // and does not require the files to be opened.
              recoveredMap.item.close() match {
                case IO.Success(_) =>
                  recoveredMaps += recoveredMap.item //recoveredMap.item can also be partially recovered file based on RecoveryMode set.
                  //if the recoveredMap's recovery result is a failure (partially recovered file),
                  //apply corruption handling based on the value set for RecoveryMode.
                  recoveredMap.result match {
                    case IO.Success(_) => //Recovery was successful. Recover next map.
                      doRecovery(otherMapsPaths, recoveredMaps)

                    case IO.Failure(error) =>
                      applyRecoveryMode(error.exception, mapPath, otherMapsPaths, recoveredMaps)
                  }

                case IO.Failure(error) => //failed to close the file.
                  IO.Failure(error)
              }

            case IO.Failure(error) =>
              //if there was a full failure perform failure handling based on the value set for RecoveryMode.
              applyRecoveryMode(error.exception, mapPath, otherMapsPaths, recoveredMaps)
          }
      }

    doRecovery(folder.folders, ListBuffer.empty)
  }

  def nextMap[K, V: ClassTag](nextMapSize: Long,
                              currentMap: Map[K, V])(implicit keyOrder: KeyOrder[K],
                                                     timeOrder: TimeOrder[Slice[Byte]],
                                                     limiter: FileLimiter,
                                                     functionStore: FunctionStore,
                                                     mapReader: MapEntryReader[MapEntry[K, V]],
                                                     writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                     skipListMerger: SkipListMerger[K, V],
                                                     ec: ExecutionContext): IO[Map[K, V]] =
    currentMap match {
      case currentMap @ PersistentMap(path, mmap, _, _, _, _) =>
        currentMap.close() flatMap {
          _ =>
            Map.persistent[K, V](path.incrementFolderId, mmap, flushOnOverflow = false, fileSize = nextMapSize)
        }

      case _ =>
        IO.Success(Map.memory[K, V](nextMapSize, flushOnOverflow = false))
    }
}

private[core] class Maps[K, V: ClassTag](val maps: ConcurrentLinkedDeque[Map[K, V]],
                                         fileSize: Long,
                                         acceleration: Level0Meter => Accelerator,
                                         @volatile private var currentMap: Map[K, V])(implicit keyOrder: KeyOrder[K],
                                                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                                                      limiter: FileLimiter,
                                                                                      functionStore: FunctionStore,
                                                                                      mapReader: MapEntryReader[MapEntry[K, V]],
                                                                                      writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                                      skipListMerger: SkipListMerger[K, V],
                                                                                      ec: ExecutionContext) extends LazyLogging {

  private var meter = Level0Meter(fileSize, currentMap.fileSize, maps.size() + 1)
  //this listener is invoked when currentMap is full.
  private var onFullListener: () => Unit = () => ()
  // This is crucial for write performance use null instead of Option.
  private var brakePedal: BrakePedal = _

  private val timer = Timer(System.nanoTime())

  def setOnFullListener(event: () => Unit) =
    onFullListener = event

  def write(mapEntry: Timer => MapEntry[K, V]): IO[Level0Meter] =
    synchronized {
      if (brakePedal != null && brakePedal.applyBrakes()) brakePedal = null
      persist(mapEntry(timer))
    }

  /**
    * @param entry entry to add
    * @return IO.Success(true) when new map gets added to maps. This return value is currently used
    *         in LevelZero to determine if there is a map that should be converted Segment.
    */
  @tailrec
  private def persist(entry: MapEntry[K, V]): IO[Level0Meter] =
    currentMap.write(entry) match {
      case IO.Success(writeSuccessful) =>
        if (writeSuccessful)
          IO.Success(meter)
        else {
          val mapsSize = maps.size() + 1
          IO(acceleration(Level0Meter(fileSize, currentMap.fileSize, mapsSize))) match {
            case IO.Success(accelerate) =>
              accelerate.brake match {
                case Some(brake) =>
                  brakePedal = new BrakePedal(brake.brakeFor, brake.releaseRate)
                case None =>
                  brakePedal = null
              }

              val nextMapSize = accelerate.nextMapSize max entry.totalByteSize
              logger.debug(s"Next map size: {}.bytes", nextMapSize)
              Maps.nextMap(nextMapSize, currentMap) match {
                case IO.Success(nextMap) =>
                  maps addFirst currentMap
                  currentMap = nextMap
                  meter = Level0Meter(fileSize, nextMapSize, mapsSize + 1)
                  onFullListener()
                  persist(entry)

                case IO.Failure(error) =>
                  IO.Failure(error)
              }
            case IO.Failure(error) =>
              IO.Failure(error)
          }
        }

      //If there is a failure writing an Entry to the Map. Start a new Map immediately! This ensures that
      //if the failure was due to a corruption in the current Map, all the new Entries do not get submitted
      //to the same Map file. They SHOULD be added to a new Map file that is not already unreadable.
      case IO.Failure(writeException) =>
        logger.error("IO.Failure to write Map entry. Starting a new Map.", writeException.exception)
        Maps.nextMap(fileSize, currentMap) match {
          case IO.Success(nextMap) =>
            maps addFirst currentMap
            currentMap = nextMap
            onFullListener()
            IO.Failure(writeException)

          case IO.Failure(newMapException) =>
            logger.error("Failed to create a new map on failure to write", newMapException.exception)
            IO.Failure(writeException)
        }
    }

  private def findFirst[R](f: Map[K, V] => Option[R]): Option[R] = {
    val iterator = maps.iterator()

    def getNext() = if (iterator.hasNext) Some(iterator.next()) else None

    @tailrec
    def find(nextMayBe: Option[Map[K, V]]): Option[R] = {
      nextMayBe match {
        case Some(next) =>
          f(next) match {
            case found @ Some(_) =>
              found

            case None =>
              find(getNext())
          }
        case _ =>
          None
      }
    }

    if (iterator.hasNext)
      find(Some(iterator.next()))
    else
      None
  }

  private def findAndReduce[R](f: Map[K, V] => Option[R],
                               reduce: (Option[R], Option[R]) => Option[R]): Option[R] = {
    val iterator = maps.iterator()

    //iterator.next() could throw an exception if the map was removed and converted to Segment as this read occurred.
    def getNext() = if (iterator.hasNext) Option(iterator.next()) else None

    @tailrec
    def find(nextMayBe: Option[Map[K, V]],
             previousResult: Option[R]): Option[R] =
      nextMayBe match {
        case Some(next) =>
          f(next) match {
            case nextResult @ Some(_) =>
              val result = reduce(previousResult, nextResult)
              find(getNext(), result)

            case None =>
              find(getNext(), previousResult)
          }
        case _ =>
          previousResult
      }

    if (iterator.hasNext)
      find(Some(iterator.next()), None)
    else
      None
  }

  private def find[R](matcher: Map[K, V] => Option[R]): Option[R] =
    matcher(currentMap) orElse findFirst(matcher)

  def contains(key: K): Boolean =
    get(key).exists(_ => true)

  def get(key: K): Option[V] =
    find(_.get(key)(keyOrder))

  def reduce[R](matcher: Map[K, V] => Option[R],
                reduce: (Option[R], Option[R]) => Option[R]): Option[R] =
    reduce(matcher(currentMap), findAndReduce(matcher, reduce))

  def last(): Option[Map[K, V]] =
    IO.tryOrNone(maps.getLast)

  def removeLast(): Option[IO[Unit]] =
    Option(maps.pollLast()) map {
      removedMap =>
        removedMap.delete match {
          case IO.Failure(error) =>
            //failed to delete file. Add it back to the queue.
            val mapPath: String = removedMap.pathOption.map(_.toString).getOrElse("No path")
            logger.error(s"Failed to delete map '$mapPath;. Adding it back to the queue.")
            maps.addLast(removedMap)
            IO.Failure(error)

          case IO.Success(_) =>
            IO.unit
        }
    }

  def keyValueCount: Option[Int] =
    reduce[Int](map => Some(map.count), (one, two) => Some(one.getOrElse(0) + two.getOrElse(0)))

  def queuedMapsCount =
    maps.size()

  def queuedMapsCountWithCurrent =
    maps.size() + Option(currentMap).map(_ => 1).getOrElse(0)

  def isEmpty: Boolean =
    maps.isEmpty

  def map: Map[K, V] =
    currentMap

  def close: IO[Unit] =
    (Seq(currentMap) ++ maps.asScala)
      .foreachIO(f = _.close(), failFast = false)
      .getOrElse(IO.unit)

  def getMeter =
    Level0Meter(fileSize, currentMap.fileSize, maps.size() + 1)

  def iterator =
    maps.iterator()

  def queuedMaps =
    maps.asScala
}
