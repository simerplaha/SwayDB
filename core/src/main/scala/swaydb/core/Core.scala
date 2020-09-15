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

package swaydb.core

import java.nio.file.Path
import java.util.function.Supplier

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.build.BuildValidator
import swaydb.core.data.{Memory, SwayFunction, Value}
import swaydb.core.function.FunctionStore
import swaydb.core.level.compaction.Compactor
import swaydb.core.level.compaction.throttle.ThrottleState
import swaydb.core.level.zero.LevelZero
import swaydb.core.map.MapEntry
import swaydb.core.map.serializer.LevelZeroMapEntryWriter
import swaydb.core.map.timer.Timer
import swaydb.core.segment.ThreadReadState
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.config._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.util.Futures.FutureImplicits
import swaydb.data.util.TupleOrNone
import swaydb.{ActorWire, Bag, IO, OK, Prepare}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
 * Core defines the interface to SwayDB's internals. User level APIs interact with SwayDB via this instance only
 * and do directly invoke any function on [[LevelZero]] or any other [[swaydb.core.level.Level]].
 */
private[swaydb] object Core {

  val closedMessage = "This SwayDB instance was closed."

  def apply(enableTimer: Boolean,
            cacheKeyValueIds: Boolean,
            fileCache: FileCache.Enable,
            memoryCache: MemoryCache,
            shutdownTimeout: FiniteDuration,
            threadStateCache: ThreadStateCache,
            config: SwayDBPersistentConfig)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                            timeOrder: TimeOrder[Slice[Byte]],
                                            functionStore: FunctionStore,
                                            buildValidator: BuildValidator): IO[swaydb.Error.Boot, Core[Bag.Less]] =
    CoreInitializer(
      config = config,
      enableTimer = enableTimer,
      cacheKeyValueIds = cacheKeyValueIds,
      fileCache = fileCache,
      threadStateCache = threadStateCache,
      memoryCache = memoryCache,
      shutdownTimeout = shutdownTimeout
    )

  def apply(enableTimer: Boolean,
            cacheKeyValueIds: Boolean,
            fileCache: FileCache.Enable,
            memoryCache: MemoryCache,
            shutdownTimeout: FiniteDuration,
            threadStateCache: ThreadStateCache,
            config: SwayDBMemoryConfig)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]],
                                        functionStore: FunctionStore,
                                        buildValidator: BuildValidator): IO[swaydb.Error.Boot, Core[Bag.Less]] =
    CoreInitializer(
      config = config,
      enableTimer = enableTimer,
      cacheKeyValueIds = cacheKeyValueIds,
      fileCache = fileCache,
      threadStateCache = threadStateCache,
      memoryCache = memoryCache,
      shutdownTimeout = shutdownTimeout
    )

  /**
   * Converts all prepare statement to a single transactional commit entry ([[MapEntry]]).
   */
  private def prepareToMapEntry(entries: Iterator[Prepare[Slice[Byte], SliceOption[Byte], Slice[Byte]]])(timer: Timer): Option[MapEntry[Slice[Byte], Memory]] =
    entries.foldLeft(Option.empty[MapEntry[Slice[Byte], Memory]]) {
      case (mapEntry, prepare) =>
        val nextEntry =
          prepare match {
            case Prepare.Put(key, value, expire) =>
              if (key.isEmpty) throw new IllegalArgumentException("Key cannot be empty.")

              MapEntry.Put[Slice[Byte], Memory.Put](key, Memory.Put(key, value, expire, timer.next))(LevelZeroMapEntryWriter.Level0PutWriter)

            case Prepare.Add(key, expire) =>
              if (key.isEmpty) throw new IllegalArgumentException("Key cannot be empty.")

              MapEntry.Put[Slice[Byte], Memory.Put](key, Memory.Put(key, Slice.Null, expire, timer.next))(LevelZeroMapEntryWriter.Level0PutWriter)

            case Prepare.Remove(key, toKey, expire) =>
              if (key.isEmpty) throw new IllegalArgumentException("Key cannot be empty.")
              if (toKey.exists(_.isEmpty)) throw new IllegalArgumentException("toKey cannot be empty.")

              toKey map {
                toKey =>
                  (MapEntry.Put[Slice[Byte], Memory.Range](key, Memory.Range(key, toKey, Value.FromValue.Null, Value.Remove(expire, timer.next)))(LevelZeroMapEntryWriter.Level0RangeWriter): MapEntry[Slice[Byte], Memory]) ++
                    MapEntry.Put[Slice[Byte], Memory.Remove](toKey, Memory.Remove(toKey, expire, timer.next))(LevelZeroMapEntryWriter.Level0RemoveWriter)
              } getOrElse {
                MapEntry.Put[Slice[Byte], Memory.Remove](key, Memory.Remove(key, expire, timer.next))(LevelZeroMapEntryWriter.Level0RemoveWriter)
              }

            case Prepare.Update(key, toKey, value) =>
              if (key.isEmpty) throw new IllegalArgumentException("Key cannot be empty.")
              if (toKey.exists(_.isEmpty)) throw new IllegalArgumentException("toKey cannot be empty.")

              toKey map {
                toKey =>
                  (MapEntry.Put[Slice[Byte], Memory.Range](key, Memory.Range(key, toKey, Value.FromValue.Null, Value.Update(value, None, timer.next)))(LevelZeroMapEntryWriter.Level0RangeWriter): MapEntry[Slice[Byte], Memory]) ++
                    MapEntry.Put[Slice[Byte], Memory.Update](toKey, Memory.Update(toKey, value, None, timer.next))(LevelZeroMapEntryWriter.Level0UpdateWriter)
              } getOrElse {
                MapEntry.Put[Slice[Byte], Memory.Update](key, Memory.Update(key, value, None, timer.next))(LevelZeroMapEntryWriter.Level0UpdateWriter)
              }

            case Prepare.ApplyFunction(key, toKey, function) =>
              if (key.isEmpty) throw new IllegalArgumentException("Key cannot be empty.")
              if (toKey.exists(_.isEmpty)) throw new IllegalArgumentException("toKey cannot be empty.")

              toKey map {
                toKey =>
                  (MapEntry.Put[Slice[Byte], Memory.Range](key, Memory.Range(key, toKey, Value.FromValue.Null, Value.Function(function, timer.next)))(LevelZeroMapEntryWriter.Level0RangeWriter): MapEntry[Slice[Byte], Memory]) ++
                    MapEntry.Put[Slice[Byte], Memory.Function](toKey, Memory.Function(toKey, function, timer.next))(LevelZeroMapEntryWriter.Level0FunctionWriter)
              } getOrElse {
                MapEntry.Put[Slice[Byte], Memory.Function](key, Memory.Function(key, function, timer.next))(LevelZeroMapEntryWriter.Level0FunctionWriter)
              }
          }
        Some(mapEntry.map(_ ++ nextEntry) getOrElse nextEntry)
    }
}

private[swaydb] class Core[BAG[_]](zero: LevelZero,
                                   coreState: CoreState,
                                   threadStateCache: ThreadStateCache)(implicit bag: Bag[BAG],
                                                                       compactor: ActorWire[Compactor[ThrottleState], ThrottleState],
                                                                       private[swaydb] val bufferSweeper: ByteBufferSweeperActor,
                                                                       shutdownExecutionContext: ExecutionContext) extends LazyLogging {

  private val serial = bag.createSerial()

  protected[swaydb] val readStates =
    ThreadLocal.withInitial[ThreadReadState] {
      new Supplier[ThreadReadState] {
        override def get(): ThreadReadState =
          threadStateCache match {
            case ThreadStateCache.Limit(hashMapMaxSize, maxProbe) =>
              ThreadReadState.limitHashMap(
                maxSize = hashMapMaxSize,
                probe = maxProbe
              )

            case ThreadStateCache.NoLimit =>
              ThreadReadState.hashMap()

            case ThreadStateCache.Disable =>
              ThreadReadState.limitHashMap(
                maxSize = 0,
                probe = 0
              )
          }
      }
    }

  def zeroPath: Path =
    zero.path

  @inline private def assertTerminated[T, BAG2[_]](f: => BAG2[T])(implicit bag: Bag[BAG2]): BAG2[T] =
    if (coreState.isRunning)
      f
    else
      bag.failure(new IllegalAccessException(Core.closedMessage))

  @inline private def execute[R, BAG[_]](thunk: LevelZero => R)(implicit bag: Bag[BAG]): BAG[R] =
    if (coreState.isRunning)
      try
        bag.success(thunk(zero))
      catch {
        case throwable: Throwable =>
          val error = IO.ExceptionHandler.toError[swaydb.Error.Level](throwable)
          IO.Defer[swaydb.Error.Level, R](thunk(zero), error).run(1)
      }
    else
      bag.failure(new IllegalAccessException(Core.closedMessage))

  def put(key: Slice[Byte]): BAG[OK] =
    assertTerminated(serial.execute(zero.put(key)))

  def put(key: Slice[Byte], value: Slice[Byte]): BAG[OK] =
    assertTerminated(serial.execute(zero.put(key, value)))

  def put(key: Slice[Byte], value: SliceOption[Byte]): BAG[OK] =
    assertTerminated(serial.execute(zero.put(key, value)))

  def put(key: Slice[Byte], value: SliceOption[Byte], removeAt: Deadline): BAG[OK] =
    assertTerminated(serial.execute(zero.put(key, value, removeAt)))

  /**
   * Each [[Prepare]] requires a new next [[swaydb.core.data.Time]] for cases where a batch contains overriding keys.
   *
   * Same time indicates that the later Prepare in this batch with the same time as newer Prepare has already applied
   * to the newer prepare therefore ignoring the newer prepare.
   *
   * @note If the default time order [[TimeOrder.long]] is used
   *       Times should always be unique and in incremental order for *ALL* key values.
   */
  def commit(entries: Iterator[Prepare[Slice[Byte], SliceOption[Byte], Slice[Byte]]]): BAG[OK] =
    assertTerminated {
      if (entries.isEmpty)
        bag.failure(new IllegalArgumentException("Cannot write empty batch"))
      else
        serial.execute(zero.put(Core.prepareToMapEntry(entries)(_).get)) //Gah .get!
    }

  def remove(key: Slice[Byte]): BAG[OK] =
    assertTerminated(serial.execute(zero.remove(key)))

  def expire(key: Slice[Byte], at: Deadline): BAG[OK] =
    assertTerminated(serial.execute(zero.remove(key, at)))

  def remove(from: Slice[Byte], to: Slice[Byte]): BAG[OK] =
    assertTerminated(serial.execute(zero.remove(from, to)))

  def expire(from: Slice[Byte], to: Slice[Byte], at: Deadline): BAG[OK] =
    assertTerminated(serial.execute(zero.remove(from, to, at)))

  def update(key: Slice[Byte], value: Slice[Byte]): BAG[OK] =
    assertTerminated(serial.execute(zero.update(key, value)))

  def update(key: Slice[Byte], value: SliceOption[Byte]): BAG[OK] =
    assertTerminated(serial.execute(zero.update(key, value)))

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Slice[Byte]): BAG[OK] =
    assertTerminated(serial.execute(zero.update(fromKey, to, value)))

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: SliceOption[Byte]): BAG[OK] =
    assertTerminated(serial.execute(zero.update(fromKey, to, value)))

  def applyFunction(key: Slice[Byte], function: Slice[Byte]): BAG[OK] =
    assertTerminated(serial.execute(zero.applyFunction(key, function)))

  def applyFunction(from: Slice[Byte], to: Slice[Byte], function: Slice[Byte]): BAG[OK] =
    assertTerminated(serial.execute(zero.applyFunction(from, to, function)))

  def registerFunction(functionId: Slice[Byte], function: SwayFunction): BAG[OK] =
    execute(_.registerFunction(functionId, function))

  def head[BAG[_]](readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[TupleOrNone[Slice[Byte], SliceOption[Byte]]] =
    execute(_.head(readState).toTupleOrNone)

  def headKey[BAG[_]](readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[SliceOption[Byte]] =
    execute(_.headKey(readState))

  def last[BAG[_]](readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[TupleOrNone[Slice[Byte], SliceOption[Byte]]] =
    execute(_.last(readState).toTupleOrNone)

  def lastKey[BAG[_]](readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[SliceOption[Byte]] =
    execute(_.lastKey(readState))

  def bloomFilterKeyValueCount: BAG[Int] =
    execute(_.keyValueCount)

  def deadline(key: Slice[Byte],
               readState: ThreadReadState): BAG[Option[Deadline]] =
    execute(_.deadline(key, readState))

  def sizeOfSegments: Long =
    zero.sizeOfSegments

  def contains(key: Slice[Byte],
               readState: ThreadReadState): BAG[Boolean] =
    execute(_.contains(key, readState))

  def mightContainKey(key: Slice[Byte]): BAG[Boolean] =
    execute(_.mightContainKey(key))

  def mightContainFunction(functionId: Slice[Byte]): BAG[Boolean] =
    execute(_.mightContainFunction(functionId))

  def get(key: Slice[Byte],
          readState: ThreadReadState): BAG[Option[SliceOption[Byte]]] =
    execute(_.get(key, readState).getValue)

  def getKey[BAG[_]](key: Slice[Byte],
                     readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[SliceOption[Byte]] =
    execute(_.getKey(key, readState))

  def getKeyValue[BAG[_]](key: Slice[Byte],
                          readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[TupleOrNone[Slice[Byte], SliceOption[Byte]]] =
    execute(_.get(key, readState).toTupleOrNone)

  def getKeyValueDeadline[BAG[_]](key: Slice[Byte],
                                  readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[TupleOrNone[(Slice[Byte], SliceOption[Byte]), Option[Deadline]]] =
    execute(_.get(key, readState).toKeyValueDeadlineOrNone)

  def getKeyDeadline[BAG[_]](key: Slice[Byte],
                             readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[TupleOrNone[Slice[Byte], Option[Deadline]]] =
    execute(_.get(key, readState).toDeadlineOrNone)

  def before[BAG[_]](key: Slice[Byte],
                     readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[TupleOrNone[Slice[Byte], SliceOption[Byte]]] =
    execute(_.lower(key, readState).toTupleOrNone)

  def beforeKey[BAG[_]](key: Slice[Byte],
                        readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[SliceOption[Byte]] =
    execute(_.lower(key, readState).getKey)

  def after[BAG[_]](key: Slice[Byte],
                    readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[TupleOrNone[Slice[Byte], SliceOption[Byte]]] =
    execute(_.higher(key, readState).toTupleOrNone)

  def afterKey[BAG[_]](key: Slice[Byte],
                       readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[SliceOption[Byte]] =
    execute(_.higher(key, readState).getKey)

  def valueSize(key: Slice[Byte],
                readState: ThreadReadState): BAG[Option[Int]] =
    execute(_.valueSize(key, readState))

  def levelZeroMeter: LevelZeroMeter =
    zero.levelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    zero.meterFor(levelNumber)

  def clear(readState: ThreadReadState): BAG[OK] =
    execute(_.clear(readState))

  def clearAppliedFunctions(): BAG[Iterable[String]] =
    execute(_.clearAppliedFunctions())

  def clearAppliedAndRegisteredFunctions(): BAG[Iterable[String]] =
    execute(_.clearAppliedAndRegisteredFunctions())

  def isFunctionApplied(functionId: Slice[Byte]): Boolean =
    zero.isFunctionApplied(functionId)

  /**
   * This does the final clean up on all Levels.
   *
   * - Releases all locks on all system folders
   * - Flushes all files and persists them to disk for persistent databases.
   */

  private def shutdownCompaction(): Future[Unit] = {
    implicit val futureBag = Bag.future(shutdownExecutionContext)

    logger.info("Stopping compaction ...")
    compactor
      .ask
      .flatMap {
        (impl, state, self) =>
          impl.terminate(state, self)
      }
      .recoverWith {
        case exception =>
          logger.error("Failed compaction shutdown.", exception)
          Future.failed(exception)
      }
  }

  def close(): BAG[Unit] =
    closeWithBag()(this.bag)

  def closeWithBag[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] =
    if (coreState.isNotRunning) {
      bag.unit
    } else {
      val closeResult =
        bag
          .and {
            coreState setState CoreState.Closing
            serial.terminateBag()
          } {
            IO
              .fromFuture(shutdownCompaction().and(zero.close()))
              .run(0)
          }

      bag.transform(closeResult) {
        unit =>
          coreState setState CoreState.Closed
          unit
      }
    }

  def delete(): BAG[Unit] =
    bag.and(close()) {
      IO.fromFuture(zero.delete()).run(0)
    }

  def state: CoreState.State =
    coreState.getState

  def toBag[BAG[_]](implicit bag: Bag[BAG]): Core[BAG] =
    new Core[BAG](
      zero = zero,
      coreState = coreState,
      threadStateCache = threadStateCache,
    )(bag = bag,
      compactor = compactor,
      bufferSweeper = bufferSweeper,
      shutdownExecutionContext = shutdownExecutionContext)
}
