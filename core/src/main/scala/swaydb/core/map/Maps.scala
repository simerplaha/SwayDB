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

package swaydb.core.map

import java.nio.file.Path
import java.util.concurrent.ConcurrentLinkedDeque

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Map.ExceptionHandler
import swaydb.IO._
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.brake.BrakePedal
import swaydb.core.io.file.Effect._
import swaydb.core.io.file.{Effect, ForceSaveApplier}
import swaydb.core.map.serializer.{MapEntryReader, MapEntryWriter}
import swaydb.core.map.timer.Timer
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.config.{MMAP, RecoveryMode}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.{Error, IO}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

private[core] object Maps extends LazyLogging {

  val closeErrorMessage = "Cannot perform write on a closed instance."

  def memory[K, V, C <: MapCache[K, V]](fileSize: Long,
                                        enableHashIndex: Boolean,
                                        acceleration: LevelZeroMeter => Accelerator)(implicit keyOrder: KeyOrder[K],
                                                                                     fileSweeper: FileSweeperActor,
                                                                                     bufferCleaner: ByteBufferSweeperActor,
                                                                                     writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                                     cacheBuilder: MapCacheBuilder[C],
                                                                                     timer: Timer,
                                                                                     forceSaveApplier: ForceSaveApplier): Maps[K, V, C] =
    new Maps[K, V, C](
      maps = new ConcurrentLinkedDeque[Map[K, V, C]](),
      fileSize = fileSize,
      acceleration = acceleration,
      enableHashIndex = enableHashIndex,
      currentMap =
        Map.memory[K, V, C](
          fileSize = fileSize,
          flushOnOverflow = false,
          enableHashIndex = enableHashIndex
        )
    )

  def persistent[K, V, C <: MapCache[K, V]](path: Path,
                                            mmap: MMAP.Map,
                                            fileSize: Long,
                                            acceleration: LevelZeroMeter => Accelerator,
                                            recovery: RecoveryMode,
                                            enableHashIndex: Boolean)(implicit keyOrder: KeyOrder[K],
                                                                      fileSweeper: FileSweeperActor,
                                                                      bufferCleaner: ByteBufferSweeperActor,
                                                                      writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                      reader: MapEntryReader[MapEntry[K, V]],
                                                                      cacheBuilder: MapCacheBuilder[C],
                                                                      timer: Timer,
                                                                      forceSaveApplier: ForceSaveApplier): IO[swaydb.Error.Map, Maps[K, V, C]] = {
    logger.debug("{}: Maps persistent started. Initialising recovery.", path)
    //reverse to keep the newest maps at the top.
    recover[K, V, C](
      folder = path,
      mmap = mmap,
      fileSize = fileSize,
      recovery = recovery,
      enableHashIndex = enableHashIndex
    ).map(_.reverse) flatMap {
      recoveredMapsReversed =>
        logger.info(s"{}: Recovered {} ${if (recoveredMapsReversed.isEmpty || recoveredMapsReversed.size > 1) "logs" else "log"}.", path, recoveredMapsReversed.size)
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
        val (emptyMaps, otherMaps) = recoveredMapsReversed.partition(_.cache.isEmpty)
        if (emptyMaps.nonEmpty) logger.info(s"{}: Deleting empty {} maps {}.", path, emptyMaps.size, emptyMaps.flatMap(_.pathOption).map(_.toString).mkString(", "))
        emptyMaps foreachIO (map => IO(map.delete)) match {
          case Some(IO.Left(error)) =>
            logger.error(s"{}: Failed to delete empty maps {}", path, emptyMaps.flatMap(_.pathOption).map(_.toString).mkString(", "))
            IO.Left(error)

          case None =>
            logger.debug(s"{}: Creating next map with ID {} maps.", path, nextMapId)
            val queue = new ConcurrentLinkedDeque[Map[K, V, C]](otherMaps.asJavaCollection)
            IO {
              Map.persistent[K, V, C](
                folder = nextMapId,
                mmap = mmap,
                flushOnOverflow = false,
                fileSize = fileSize,
                dropCorruptedTailEntries = recovery.drop,
                enableHashIndex = enableHashIndex
              )
            } map {
              nextMap =>
                logger.debug(s"{}: Next map created with ID {}.", path, nextMapId)
                new Maps[K, V, C](
                  maps = queue,
                  fileSize = fileSize,
                  acceleration = acceleration,
                  enableHashIndex = enableHashIndex,
                  currentMap = nextMap.item
                )
            }
        }
    }
  }

  private def recover[K, V, C <: MapCache[K, V]](folder: Path,
                                                 mmap: MMAP.Map,
                                                 fileSize: Long,
                                                 enableHashIndex: Boolean,
                                                 recovery: RecoveryMode)(implicit keyOrder: KeyOrder[K],
                                                                         fileSweeper: FileSweeperActor,
                                                                         bufferCleaner: ByteBufferSweeperActor,
                                                                         writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                         mapReader: MapEntryReader[MapEntry[K, V]],
                                                                         cacheBuilder: MapCacheBuilder[C],
                                                                         forceSaveApplier: ForceSaveApplier): IO[swaydb.Error.Map, ListBuffer[Map[K, V, C]]] = {
    /**
     * Performs corruption handling based on the the value set for [[RecoveryMode]].
     */
    def applyRecoveryMode(exception: Throwable,
                          mapPath: Path,
                          otherMapsPaths: List[Path],
                          recoveredMaps: ListBuffer[Map[K, V, C]]): IO[swaydb.Error.Map, ListBuffer[Map[K, V, C]]] =
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
                   recoveredMaps: ListBuffer[Map[K, V, C]]): IO[swaydb.Error.Map, ListBuffer[Map[K, V, C]]] =
      maps match {
        case Nil =>
          IO.Right(recoveredMaps)

        case mapPath :: otherMapsPaths =>
          logger.debug(s"{}: Recovering.", mapPath)

          IO {
            Map.persistent[K, V, C](
              folder = mapPath,
              mmap = mmap,
              flushOnOverflow = false,
              fileSize = fileSize,
              enableHashIndex = enableHashIndex,
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

  def nextMapUnsafe[K, V, C <: MapCache[K, V]](nextMapSize: Long,
                                               enableHashIndex: Boolean,
                                               currentMap: Map[K, V, C])(implicit keyOrder: KeyOrder[K],
                                                                         fileSweeper: FileSweeperActor,
                                                                         bufferCleaner: ByteBufferSweeperActor,
                                                                         writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                         skipListMerger: MapCacheBuilder[C],
                                                                         forceSaveApplier: ForceSaveApplier): Map[K, V, C] =
    currentMap match {
      case currentMap: PersistentMap[K, V, C] =>
        currentMap.close()
        Map.persistent[K, V, C](
          folder = currentMap.path.incrementFolderId,
          mmap = currentMap.mmap,
          flushOnOverflow = false,
          fileSize = nextMapSize,
          enableHashIndex = enableHashIndex
        )

      case _ =>
        Map.memory[K, V, C](
          fileSize = nextMapSize,
          flushOnOverflow = false,
          enableHashIndex = enableHashIndex
        )
    }

  /**
   * Returns a snapshot of [[Maps]] state to execute reads.
   *
   * @note Why? - [[Maps.maps]] is being concurrently updated by writes (inserts) and compaction (remove)
   *       there is no easy was to tell how reads to access the queues maps and we cannot the Iterator
   *       directly because of the current read architecture could access the same Iterator multiple times.
   *       This cannot be mutable either due to performance cost. So a Slice is needed.
   *
   *       There will be rare cases where [[maps]] could contain [[currentMap]]
   *       which would result in [[currentMap]] being twice but this would only
   *       happen if [[fileSize]] is too small < 10.bytes otherwise the performance
   *       cost is negligible.
   */
  @inline def flatMap[K, V, C <: MapCache[K, V], R: ClassTag](minimumSize: Int,
                                                              currentMap: Map[K, V, C],
                                                              queue: ConcurrentLinkedDeque[Map[K, V, C]])(flatten: Map[K, V, C] => Iterable[R]): Slice[R] = {
    val initial = flatten(currentMap)
    var slice = Slice.of[R]((minimumSize + initial.size) * 2)
    slice addAll initial

    //if currentMap is already added the queue then drop head.
    var staleCurrentMap = false

    queue forEach {
      (queuedMap: Map[K, V, C]) => {
        // As the writes are in progress currentMap could get added to the
        // queue and the more maps could also possibly get added to the map.
        // For those cases we need see if currentMap is already in the Map.
        // If it is then we drop the currentMap and work off the queue directly.

        if (queuedMap.uniqueFileNumber == currentMap.uniqueFileNumber)
          staleCurrentMap = true

        if (slice.isFull) {
          //overflow - extend the map.
          val newSlice = Slice.of[R](slice.size * 2)
          newSlice addAll slice
          newSlice addAll flatten(queuedMap)
          slice = newSlice
        } else {
          slice addAll flatten(queuedMap)
        }
      }
    }

    if (staleCurrentMap)
      slice.dropHead()
    else
      slice
  }
}

private[core] class Maps[K, V, C <: MapCache[K, V]](val maps: ConcurrentLinkedDeque[Map[K, V, C]],
                                                    fileSize: Long,
                                                    acceleration: LevelZeroMeter => Accelerator,
                                                    enableHashIndex: Boolean,
                                                    @volatile private var currentMap: Map[K, V, C])(implicit keyOrder: KeyOrder[K],
                                                                                                    fileSweeper: FileSweeperActor,
                                                                                                    val bufferCleaner: ByteBufferSweeperActor,
                                                                                                    writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                                                    cacheBuilder: MapCacheBuilder[C],
                                                                                                    val timer: Timer,
                                                                                                    forceSaveApplier: ForceSaveApplier) extends LazyLogging { self =>

  @volatile private var closed: Boolean = false

  //this listener is invoked when currentMap is full.
  private var onNextMapListener: () => Unit = () => ()
  // This is crucial for write performance use null instead of Option.
  private var brakePedal: BrakePedal = _

  @volatile private var totalMapsCount: Int = maps.size() + 1
  @volatile private var currentMapsCount: Int = maps.size() + 1

  val meter =
    new LevelZeroMeter {
      override def defaultMapSize: Long = fileSize

      override def currentMapSize: Long = currentMap.fileSize

      override def mapsCount: Int = self.currentMapsCount
    }

  private[core] def onNextMapCallback(event: () => Unit): Unit =
    onNextMapListener = event

  @inline def flatMap[R: ClassTag](f: Map[K, V, C] => Iterable[R]): Slice[R] =
    Maps.flatMap(
      minimumSize = currentMapsCount,
      currentMap = currentMap,
      queue = maps
    )(f)

  def write(mapEntry: Timer => MapEntry[K, V]): Unit =
    synchronized {
      if (brakePedal != null && brakePedal.applyBrakes()) brakePedal = null
      persist(mapEntry(timer))
    }

  private def initNextMap(mapSize: Long) = {
    val nextMap =
      Maps.nextMapUnsafe(
        nextMapSize = mapSize,
        enableHashIndex = enableHashIndex,
        currentMap = currentMap
      )

    maps addFirst currentMap
    currentMap = nextMap
    totalMapsCount += 1
    currentMapsCount += 1
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
        currentMap writeNoSync entry
      catch {
        case exception: Throwable if self.closed =>
          throw swaydb.Exception.InvalidAccessException(Maps.closeErrorMessage, exception)

        case throwable: Throwable =>
          //If there is a failure writing an Entry to the Map. Start a new Map immediately! This ensures that
          //if the failure was due to a corruption in the current Map, all the new Entries do not value submitted
          //to the same Map file. They SHOULD be added to a new Map file that is not already unreadable.
          logger.error(s"FATAL: Failed to write Map entry of size ${entry.entryBytesSize}.byte(s). Initialising a new Map.", throwable)
          initNextMap(fileSize)
          throw throwable
      }

    val accelerate = acceleration(meter)
    if (accelerate.brake.isEmpty) {
      if (brakePedal != null && brakePedal.isReleased())
        brakePedal = null
    } else if (brakePedal == null) {
      val brake = accelerate.brake.get
      brakePedal =
        new BrakePedal(
          brakeFor = brake.brakeFor,
          releaseRate = brake.releaseRate,
          logAsWarning = brake.logAsWarning
        )
    }

    if (!persisted) {
      val nextMapSize = accelerate.nextMapSize max entry.totalByteSize
      logger.debug(s"Map full. Initialising next map of size: $nextMapSize.bytes.")
      initNextMap(nextMapSize)
      persist(entry)
    }
  }

  @inline final private def findFirst[A >: Null, B](nullResult: B,
                                                    flatMap: Map[K, V, C] => Iterable[A],
                                                    finder: A => B): B = {
    val iterator = flatMap(currentMap).iterator ++ maps.iterator().asScala.flatMap(flatMap)

    def getNext() = if (iterator.hasNext) iterator.next() else null

    @tailrec
    def find(next: A): B = {
      val foundOrNullResult = finder(next)
      if (foundOrNullResult == nullResult) {
        val next = getNext()
        if (next == null)
          nullResult
        else
          find(next)
      } else {
        foundOrNullResult
      }
    }

    val next = getNext()
    if (next == null)
      nullResult
    else
      find(next)
  }

  @inline final def reduce[A >: Null, B](nullResult: B,
                                         flatMap: Map[K, V, C] => Iterable[A],
                                         applier: A => B,
                                         reducer: (B, B) => B): B = {

    val iterator = flatMap(currentMap).iterator ++ maps.iterator().asScala.flatMap(map => flatMap(map))

    def getNextOrNull(): A = if (iterator.hasNext) iterator.next() else null

    @tailrec
    def find(nextOrNull: A,
             previousResult: B): B =
      if (nextOrNull == null) {
        previousResult
      } else {
        val nextResult = applier(nextOrNull)
        if (nextResult == nullResult) {
          find(getNextOrNull(), previousResult)
        } else if (previousResult == nullResult) {
          find(getNextOrNull(), nextResult)
        } else {
          val result = reducer(previousResult, nextResult)
          find(getNextOrNull(), result)
        }
      }

    find(getNextOrNull(), nullResult)
  }

  def find[A >: Null, B](nullResult: B,
                         flatMap: Map[K, V, C] => Iterable[A],
                         matcher: A => B): B =
    findFirst[A, B](
      nullResult = nullResult,
      flatMap = flatMap,
      finder = matcher
    )

  def lastOption(): Option[Map[K, V, C]] =
    IO.tryOrNone(maps.getLast)

  def removeLast(): Option[IO[swaydb.Error.Map, Unit]] =
    Option(maps.pollLast()) map {
      removedMap =>
        IO(removedMap.delete) match {
          case IO.Right(_) =>
            currentMapsCount -= 1
            IO.unit

          case IO.Left(error) =>
            //failed to delete file. Add it back to the queue.
            val mapPath: String = removedMap.pathOption.map(_.toString).getOrElse("No path")
            logger.error(s"Failed to delete map '$mapPath;. Adding it back to the queue.", error.exception)
            maps.addLast(removedMap)
            IO.Left(error)
        }
    }

  def keyValueCount: Int =
    reduce[Integer, Int](
      nullResult = 0,
      flatMap = map => Seq(map.cache.maxKeyValueCount),
      applier = size => size,
      reducer = _ + _
    )

  def queuedMapsCount =
    maps.size()

  def queuedMapsCountWithCurrent =
    maps.size() + Option(currentMap).map(_ => 1).getOrElse(0)

  def isEmpty: Boolean =
    maps.isEmpty

  def map: Map[K, V, C] =
    currentMap

  def close(): IO[swaydb.Error.Map, Unit] =
    IO {
      closed = true
      timer.close
    }.onLeftSideEffect {
      failure =>
        logger.error("Failed to close timer file", failure.exception)
    }.and {
      self
        .flatMap(Array(_))
        .foreachIO(map => IO(map.close()), failFast = false)
        .getOrElse(IO.unit)
    }

  def delete(): IO[Error.Map, Unit] =
    close()
      .and {
        self
          .flatMap(Array(_))
          .foreachIO(map => IO(map.delete))
          .getOrElse(IO.unit)
      }

  def queuedMapsIterator =
    maps.iterator()

  def mmap: MMAP =
    currentMap.mmap

  def stateId: Long =
    totalMapsCount

  def isClosed =
    closed

  def queuedMaps =
    maps.asScala
}
