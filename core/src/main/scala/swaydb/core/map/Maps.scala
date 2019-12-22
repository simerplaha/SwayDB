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

package swaydb.core.map

import java.nio.file.Path
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.function.Consumer

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Map.ExceptionHandler
import swaydb.IO
import swaydb.IO._
import swaydb.core.actor.FileSweeper
import swaydb.core.brake.BrakePedal
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.Effect
import swaydb.core.io.file.Effect._
import swaydb.core.map.serializer.{MapEntryReader, MapEntryWriter}
import swaydb.core.map.timer.Timer
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.config.RecoveryMode
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

private[core] object Maps extends LazyLogging {

  def memory[OK, OV, K <: OK, V <: OV : ClassTag](fileSize: Long,
                                                  acceleration: LevelZeroMeter => Accelerator,
                                                  nullKey: OK,
                                                  nullValue: OV)(implicit keyOrder: KeyOrder[K],
                                                                 timeOrder: TimeOrder[Slice[Byte]],
                                                                 fileSweeper: FileSweeper,
                                                                 functionStore: FunctionStore,
                                                                 mapReader: MapEntryReader[MapEntry[K, V]],
                                                                 writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                 skipListMerger: SkipListMerger[OK, OV, K, V],
                                                                 timer: Timer): Maps[OK, OV, K, V] =
    new Maps[OK, OV, K, V](
      maps = new ConcurrentLinkedDeque[Map[OK, OV, K, V]](),
      fileSize = fileSize,
      acceleration = acceleration,
      currentMap =
        Map.memory[OK, OV, K, V](
          nullKey = nullKey,
          nullValue = nullValue,
          fileSize = fileSize,
          flushOnOverflow = false
        )
    )

  def persistent[OK, OV, K <: OK, V <: OV : ClassTag](path: Path,
                                                      mmap: Boolean,
                                                      fileSize: Long,
                                                      acceleration: LevelZeroMeter => Accelerator,
                                                      recovery: RecoveryMode,
                                                      nullKey: OK,
                                                      nullValue: OV)(implicit keyOrder: KeyOrder[K],
                                                                     timeOrder: TimeOrder[Slice[Byte]],
                                                                     fileSweeper: FileSweeper,
                                                                     functionStore: FunctionStore,
                                                                     writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                     reader: MapEntryReader[MapEntry[K, V]],
                                                                     skipListMerger: SkipListMerger[OK, OV, K, V],
                                                                     timer: Timer): IO[swaydb.Error.Map, Maps[OK, OV, K, V]] = {
    logger.debug("{}: Maps persistent started. Initialising recovery.", path)
    //reverse to keep the newest maps at the top.
    recover[OK, OV, K, V](
      folder = path,
      mmap = mmap,
      fileSize = fileSize,
      recovery = recovery,
      nullKey = nullKey,
      nullValue = nullValue
    ).map(_.reverse) flatMap {
      recoveredMapsReversed =>
        logger.info(s"{}: Recovered {} maps.", path, recoveredMapsReversed.size)
        val nextMapId =
          recoveredMapsReversed.headOption match {
            case Some(lastMaps) =>
              lastMaps match {
                case PersistentMap(path, _, _, _, _, _, _) =>
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
        emptyMaps foreachIO (map => IO(map.delete)) match {
          case Some(IO.Left(error)) =>
            logger.error(s"{}: Failed to delete empty maps {}", path, emptyMaps.flatMap(_.pathOption).map(_.toString).mkString(", "))
            IO.Left(error)

          case None =>
            logger.debug(s"{}: Creating next map with ID {} maps.", path, nextMapId)
            val queue = new ConcurrentLinkedDeque[Map[OK, OV, K, V]](otherMaps.asJavaCollection)
            IO {
              Map.persistent[OK, OV, K, V](
                nullKey = nullKey,
                nullValue = nullValue,
                folder = nextMapId,
                mmap = mmap,
                flushOnOverflow = false,
                fileSize = fileSize,
                dropCorruptedTailEntries = recovery.drop
              )
            } map {
              nextMap =>
                logger.debug(s"{}: Next map created with ID {}.", path, nextMapId)
                new Maps[OK, OV, K, V](queue, fileSize, acceleration, nextMap.item)
            }
        }
    }
  }

  private def recover[OK, OV, K <: OK, V <: OV : ClassTag](folder: Path,
                                                           mmap: Boolean,
                                                           fileSize: Long,
                                                           recovery: RecoveryMode,
                                                           nullKey: OK,
                                                           nullValue: OV)(implicit keyOrder: KeyOrder[K],
                                                                          timeOrder: TimeOrder[Slice[Byte]],
                                                                          fileSweeper: FileSweeper,
                                                                          functionStore: FunctionStore,
                                                                          writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                          mapReader: MapEntryReader[MapEntry[K, V]],
                                                                          skipListMerger: SkipListMerger[OK, OV, K, V]): IO[swaydb.Error.Map, ListBuffer[Map[OK, OV, K, V]]] = {
    /**
     * Performs corruption handling based on the the value set for [[RecoveryMode]].
     */
    def applyRecoveryMode(exception: Throwable,
                          mapPath: Path,
                          otherMapsPaths: List[Path],
                          recoveredMaps: ListBuffer[Map[OK, OV, K, V]]): IO[swaydb.Error.Map, ListBuffer[Map[OK, OV, K, V]]] =
      exception match {
        case exception: IllegalStateException =>
          recovery match {
            case RecoveryMode.ReportFailure =>
              //return failure immediately without effecting the current state of Level0
              IO.failed(exception)

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
                  IO(Effect.walkDelete(mapPath)) match {
                    case IO.Right(_) =>
                      logger.info(s"{}: Deleted file after corruption. Recovery mode: {}", mapPath, recovery.name)
                      IO.unit

                    case IO.Left(error) =>
                      logger.error(s"{}: IO.Left to delete file after corruption file. Recovery mode: {}", mapPath, recovery.name)
                      IO.Left(error)
                  }
              } match {
                case Some(IO.Left(error)) =>
                  IO.Left(error)

                case None =>
                  IO.Right(recoveredMaps)
              }
          }

        case exception: Throwable =>
          IO.failed(exception)
      }

    /**
     * Start recovery for all the input maps.
     */
    @tailrec
    def doRecovery(maps: List[Path],
                   recoveredMaps: ListBuffer[Map[OK, OV, K, V]]): IO[swaydb.Error.Map, ListBuffer[Map[OK, OV, K, V]]] =
      maps match {
        case Nil =>
          IO.Right(recoveredMaps)

        case mapPath :: otherMapsPaths =>
          logger.info(s"{}: Recovering.", mapPath)

          IO {
            Map.persistent[OK, OV, K, V](
              nullKey = nullKey,
              nullValue = nullValue,
              folder = mapPath,
              mmap = mmap,
              flushOnOverflow = false,
              fileSize = fileSize,
              dropCorruptedTailEntries = recovery.drop
            )
          } match {
            case IO.Right(recoveredMap) =>
              //recovered immutable memory map's files should be closed after load as they are always read from in memory
              // and does not require the files to be opened.
              IO(recoveredMap.item.close()) match {
                case IO.Right(_) =>
                  recoveredMaps += recoveredMap.item //recoveredMap.item can also be partially recovered file based on RecoveryMode set.
                  //if the recoveredMap's recovery result is a failure (partially recovered file),
                  //apply corruption handling based on the value set for RecoveryMode.
                  recoveredMap.result match {
                    case IO.Right(_) => //Recovery was successful. Recover next map.
                      doRecovery(
                        maps = otherMapsPaths,
                        recoveredMaps = recoveredMaps
                      )

                    case IO.Left(error) =>
                      applyRecoveryMode(
                        exception = error.exception,
                        mapPath = mapPath,
                        otherMapsPaths = otherMapsPaths,
                        recoveredMaps = recoveredMaps
                      )
                  }

                case IO.Left(error) => //failed to close the file.
                  IO.Left(error)
              }

            case IO.Left(error) =>
              //if there was a full failure perform failure handling based on the value set for RecoveryMode.
              applyRecoveryMode(
                exception = error.exception,
                mapPath = mapPath,
                otherMapsPaths = otherMapsPaths,
                recoveredMaps = recoveredMaps
              )
          }
      }

    doRecovery(
      maps = folder.folders,
      recoveredMaps = ListBuffer.empty
    )
  }

  def nextMapUnsafe[OK, OV, K <: OK, V <: OV : ClassTag](nextMapSize: Long,
                                                         currentMap: Map[OK, OV, K, V])(implicit keyOrder: KeyOrder[K],
                                                                                        timeOrder: TimeOrder[Slice[Byte]],
                                                                                        fileSweeper: FileSweeper,
                                                                                        functionStore: FunctionStore,
                                                                                        mapReader: MapEntryReader[MapEntry[K, V]],
                                                                                        writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                                        skipListMerger: SkipListMerger[OK, OV, K, V]): Map[OK, OV, K, V] =
    currentMap match {
      case currentMap @ PersistentMap(_, _, _, _, _, _, _) =>
        currentMap.close()
        Map.persistent[OK, OV, K, V](
          folder = currentMap.path.incrementFolderId,
          mmap = currentMap.mmap,
          flushOnOverflow = false,
          fileSize = nextMapSize,
          nullKey = currentMap.skipList.nullKey,
          nullValue = currentMap.skipList.nullValue
        )

      case _ =>
        Map.memory[OK, OV, K, V](
          nullKey = currentMap.skipList.nullKey,
          nullValue = currentMap.skipList.nullValue,
          fileSize = nextMapSize,
          flushOnOverflow = false
        )
    }
}

private[core] class Maps[OK, OV, K <: OK, V <: OV : ClassTag](val maps: ConcurrentLinkedDeque[Map[OK, OV, K, V]],
                                                              fileSize: Long,
                                                              acceleration: LevelZeroMeter => Accelerator,
                                                              @volatile private var currentMap: Map[OK, OV, K, V])(implicit keyOrder: KeyOrder[K],
                                                                                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                                                                                   fileSweeper: FileSweeper,
                                                                                                                   functionStore: FunctionStore,
                                                                                                                   mapReader: MapEntryReader[MapEntry[K, V]],
                                                                                                                   writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                                                                   skipListMerger: SkipListMerger[OK, OV, K, V],
                                                                                                                   timer: Timer) extends LazyLogging { self =>

  //this listener is invoked when currentMap is full.
  private var onNextMapListener: () => Unit = () => ()
  // This is crucial for write performance use null instead of Option.
  private var brakePedal: BrakePedal = _

  @volatile private var totalMapsCount: Int = maps.size() + 1
  @volatile private var currentMapsCount: Int = maps.size() + 1

  @volatile var currentMapsSlice: Slice[Map[OK, OV, K, V]] = toMapsSlice()

  val meter =
    new LevelZeroMeter {
      override def defaultMapSize: Long = fileSize
      override def currentMapSize: Long = currentMap.fileSize
      override def mapsCount: Int = self.currentMapsCount
    }

  private[core] def onNextMapCallback(event: () => Unit): Unit =
    onNextMapListener = event

  def write(mapEntry: Timer => MapEntry[K, V]): Unit =
    synchronized {
      if (brakePedal != null && brakePedal.applyBrakes()) brakePedal = null
      persist(mapEntry(timer))
    }

  def toMapsSlice(): Slice[Map[OK, OV, K, V]] = {
    val slice = Slice.create[Map[OK, OV, K, V]](totalMapsCount)

    slice add currentMap
    maps forEach {
      new Consumer[Map[OK, OV, K, V]] {
        override def accept(map: Map[OK, OV, K, V]): Unit =
          slice add map
      }
    }
    slice
  }

  def updateMapsSlice(): Unit =
    this.currentMapsSlice = toMapsSlice()

  private def initNextMap(mapSize: Long) = {
    val nextMap = Maps.nextMapUnsafe(mapSize, currentMap)
    maps addFirst currentMap
    currentMap = nextMap
    totalMapsCount += 1
    currentMapsCount += 1
    updateMapsSlice()
    onNextMapListener()
  }

  /**
   * @param entry entry to add
   * @return IO.Right(true) when new map gets added to maps. This return value is currently used
   *         in LevelZero to determine if there is a map that should be converted Segment.
   */
  @tailrec
  private def persist(entry: MapEntry[K, V]): Unit = {
    val persisted =
      try
        currentMap write entry
      catch {
        case throwable: Throwable =>
          //If there is a failure writing an Entry to the Map. Start a new Map immediately! This ensures that
          //if the failure was due to a corruption in the current Map, all the new Entries do not value submitted
          //to the same Map file. They SHOULD be added to a new Map file that is not already unreadable.
          logger.error(s"FATAL: Failed to write Map entry of size ${entry.entryBytesSize}.byte(s). Initialising a new Map.", throwable)
          initNextMap(fileSize)
          throw throwable
      }

    if (!persisted) {
      val accelerate = acceleration(meter)

      accelerate.brake match {
        case None =>
          brakePedal = null

        case Some(brake) =>
          brakePedal = new BrakePedal(brake.brakeFor, brake.releaseRate)
      }

      val nextMapSize = accelerate.nextMapSize max entry.totalByteSize
      logger.debug(s"Map full. Initialising next map of size: $nextMapSize.bytes.")
      initNextMap(nextMapSize)
      persist(entry)
    }
  }

  private def findFirst[R](f: Map[OK, OV, K, V] => Option[R]): Option[R] = {
    val iterator = maps.iterator()

    def getNext() = if (iterator.hasNext) iterator.next() else null

    @tailrec
    def find(next: Map[OK, OV, K, V]): Option[R] =
      f(next) match {
        case found @ Some(_) =>
          found

        case None =>
          val next = getNext()
          if (next == null)
            None
          else
            find(next)
      }

    val next = getNext()
    if (next == null)
      None
    else
      find(next)
  }

  private def findAndReduce[R](f: Map[OK, OV, K, V] => Option[R],
                               reduce: (Option[R], Option[R]) => Option[R]): Option[R] = {
    val iterator = maps.iterator()

    def getNext() = if (iterator.hasNext) Option(iterator.next()) else None

    @tailrec
    def find(nextMayBe: Option[Map[OK, OV, K, V]],
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

        case None =>
          previousResult
      }

    find(getNext(), None)
  }

  def find[R](matcher: Map[OK, OV, K, V] => Option[R]): Option[R] =
    matcher(currentMap) orElse findFirst(matcher)

  def contains(key: K): Boolean =
    get(key).isDefined

  def get(key: K): Option[V] =
  //    find(_.skipList.get(key))
    ???

  def reduce[R](matcher: Map[OK, OV, K, V] => Option[R],
                reduce: (Option[R], Option[R]) => Option[R]): Option[R] =
    reduce(matcher(currentMap), findAndReduce(matcher, reduce))

  def lastOption(): Option[Map[OK, OV, K, V]] =
    IO.tryOrNone(maps.getLast)

  def removeLast(): Option[IO[swaydb.Error.Map, Unit]] =
    Option(maps.pollLast()) map {
      removedMap =>
        IO(removedMap.delete) match {
          case IO.Right(_) =>
            currentMapsCount -= 1
            updateMapsSlice()
            IO.unit

          case IO.Left(error) =>
            //failed to delete file. Add it back to the queue.
            val mapPath: String = removedMap.pathOption.map(_.toString).getOrElse("No path")
            logger.error(s"Failed to delete map '$mapPath;. Adding it back to the queue.", error.exception)
            maps.addLast(removedMap)
            updateMapsSlice()
            IO.Left(error)
        }
    }

  def keyValueCount: Option[Int] =
    reduce[Int](map => Some(map.size), (one, two) => Some(one.getOrElse(0) + two.getOrElse(0)))

  def queuedMapsCount =
    maps.size()

  def queuedMapsCountWithCurrent =
    maps.size() + Option(currentMap).map(_ => 1).getOrElse(0)

  def isEmpty: Boolean =
    maps.isEmpty

  def map: Map[OK, OV, K, V] =
    currentMap

  def close: IO[swaydb.Error.Map, Unit] = {
    IO(timer.close) onLeftSideEffect {
      failure =>
        logger.error("Failed to close timer file", failure.exception)
    }

    (Seq(currentMap) ++ maps.asScala)
      .foreachIO(f = map => IO(map.close()), failFast = false)
      .getOrElse(IO.unit)
  }

  def queuedMapsIterator =
    maps.iterator()

  def stateId: Long =
    totalMapsCount

  def queuedMaps =
    maps.asScala
}
