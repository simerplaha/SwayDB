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
 */

package swaydb.core

import java.util.function.Supplier

import swaydb.core.data.{Memory, SwayFunction, Value}
import swaydb.core.function.FunctionStore
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
import swaydb.data.util.TupleOrNone
import swaydb.{Bag, IO, OK, Prepare}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Deadline

private[swaydb] object Core {

  def apply(config: SwayDBPersistentConfig,
            enableTimer: Boolean,
            cacheKeyValueIds: Boolean,
            fileCache: FileCache.Enable,
            memoryCache: MemoryCache,
            threadStateCache: ThreadStateCache)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                timeOrder: TimeOrder[Slice[Byte]],
                                                functionStore: FunctionStore): IO[swaydb.Error.Boot, Core[Bag.Less]] =
    CoreInitializer(
      config = config,
      enableTimer = enableTimer,
      cacheKeyValueIds = cacheKeyValueIds,
      fileCache = fileCache,
      threadStateCache = threadStateCache,
      memoryCache = memoryCache
    )

  def apply(config: SwayDBMemoryConfig,
            enableTimer: Boolean,
            cacheKeyValueIds: Boolean,
            fileCache: FileCache.Enable,
            memoryCache: MemoryCache,
            threadStateCache: ThreadStateCache)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                timeOrder: TimeOrder[Slice[Byte]],
                                                functionStore: FunctionStore): IO[swaydb.Error.Boot, Core[Bag.Less]] =
    CoreInitializer(
      config = config,
      enableTimer = enableTimer,
      cacheKeyValueIds = cacheKeyValueIds,
      fileCache = fileCache,
      threadStateCache = threadStateCache,
      memoryCache = memoryCache
    )

  def apply(enableTimer: Boolean,
            config: LevelZeroPersistentConfig)(implicit mmapCleanerEC: Option[ExecutionContext],
                                               keyOrder: KeyOrder[Slice[Byte]],
                                               timeOrder: TimeOrder[Slice[Byte]],
                                               functionStore: FunctionStore): IO[swaydb.Error.Boot, Core[Bag.Less]] =
    CoreInitializer(
      config = config,
      enableTimer = enableTimer
    )

  def apply(config: LevelZeroMemoryConfig,
            enableTimer: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  timeOrder: TimeOrder[Slice[Byte]],
                                  functionStore: FunctionStore): IO[swaydb.Error.Boot, Core[Bag.Less]] =
    CoreInitializer(
      config = config,
      enableTimer = enableTimer
    )

  private def prepareToMapEntry(entries: Iterator[Prepare[Slice[Byte], SliceOption[Byte], Slice[Byte]]])(timer: Timer): Option[MapEntry[Slice[Byte], Memory]] =
    entries.foldLeft(Option.empty[MapEntry[Slice[Byte], Memory]]) {
      case (mapEntry, prepare) =>
        val nextEntry =
          prepare match {
            case Prepare.Put(key, value, expire) =>
              if (key.isEmpty) throw new Exception("Key cannot be empty.")

              MapEntry.Put[Slice[Byte], Memory.Put](key, Memory.Put(key, value, expire, timer.next))(LevelZeroMapEntryWriter.Level0PutWriter)

            case Prepare.Add(key, expire) =>
              if (key.isEmpty) throw new Exception("Key cannot be empty.")

              MapEntry.Put[Slice[Byte], Memory.Put](key, Memory.Put(key, Slice.Null, expire, timer.next))(LevelZeroMapEntryWriter.Level0PutWriter)

            case Prepare.Remove(key, toKey, expire) =>
              if (key.isEmpty) throw new Exception("Key cannot be empty.")
              if (toKey.exists(_.isEmpty)) throw new Exception("toKey cannot be empty.")

              toKey map {
                toKey =>
                  (MapEntry.Put[Slice[Byte], Memory.Range](key, Memory.Range(key, toKey, Value.FromValue.Null, Value.Remove(expire, timer.next)))(LevelZeroMapEntryWriter.Level0RangeWriter): MapEntry[Slice[Byte], Memory]) ++
                    MapEntry.Put[Slice[Byte], Memory.Remove](toKey, Memory.Remove(toKey, expire, timer.next))(LevelZeroMapEntryWriter.Level0RemoveWriter)
              } getOrElse {
                MapEntry.Put[Slice[Byte], Memory.Remove](key, Memory.Remove(key, expire, timer.next))(LevelZeroMapEntryWriter.Level0RemoveWriter)
              }

            case Prepare.Update(key, toKey, value) =>
              if (key.isEmpty) throw new Exception("Key cannot be empty.")
              if (toKey.exists(_.isEmpty)) throw new Exception("toKey cannot be empty.")

              toKey map {
                toKey =>
                  (MapEntry.Put[Slice[Byte], Memory.Range](key, Memory.Range(key, toKey, Value.FromValue.Null, Value.Update(value, None, timer.next)))(LevelZeroMapEntryWriter.Level0RangeWriter): MapEntry[Slice[Byte], Memory]) ++
                    MapEntry.Put[Slice[Byte], Memory.Update](toKey, Memory.Update(toKey, value, None, timer.next))(LevelZeroMapEntryWriter.Level0UpdateWriter)
              } getOrElse {
                MapEntry.Put[Slice[Byte], Memory.Update](key, Memory.Update(key, value, None, timer.next))(LevelZeroMapEntryWriter.Level0UpdateWriter)
              }

            case Prepare.ApplyFunction(key, toKey, function) =>
              if (key.isEmpty) throw new Exception("Key cannot be empty.")
              if (toKey.exists(_.isEmpty)) throw new Exception("toKey cannot be empty.")

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

private[swaydb] class Core[BAG[_]](val zero: LevelZero,
                                   threadStateCache: ThreadStateCache,
                                   onClose: => IO.Defer[swaydb.Error.Close, Unit])(implicit bag: Bag[BAG]) {

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

  def put(key: Slice[Byte]): BAG[OK] =
    serial.execute(zero.put(key))

  def put(key: Slice[Byte], value: Slice[Byte]): BAG[OK] =
    serial.execute(zero.put(key, value))

  def put(key: Slice[Byte], value: SliceOption[Byte]): BAG[OK] =
    serial.execute(zero.put(key, value))

  def put(key: Slice[Byte], value: SliceOption[Byte], removeAt: Deadline): BAG[OK] =
    serial.execute(zero.put(key, value, removeAt))

  /**
   * Each [[Prepare]] requires a new next [[swaydb.core.data.Time]] for cases where a batch contains overriding keys.
   *
   * Same time indicates that the later Prepare in this batch with the same time as newer Prepare has already applied
   * to the newer prepare therefore ignoring the newer prepare.
   *
   * @note If the default time order [[TimeOrder.long]] is used
   *       Times should always be unique and in incremental order for *ALL* key values.
   */
  def put(entries: Iterator[Prepare[Slice[Byte], SliceOption[Byte], Slice[Byte]]]): BAG[OK] =
    if (entries.isEmpty)
      bag.failure(new IllegalArgumentException("Cannot write empty batch"))
    else
      serial.execute(zero.put(Core.prepareToMapEntry(entries)(_).get)) //Gah .get!

  def remove(key: Slice[Byte]): BAG[OK] =
    serial.execute(zero.remove(key))

  def remove(key: Slice[Byte], at: Deadline): BAG[OK] =
    serial.execute(zero.remove(key, at))

  def remove(from: Slice[Byte], to: Slice[Byte]): BAG[OK] =
    serial.execute(zero.remove(from, to))

  def remove(from: Slice[Byte], to: Slice[Byte], at: Deadline): BAG[OK] =
    serial.execute(zero.remove(from, to, at))

  def update(key: Slice[Byte], value: Slice[Byte]): BAG[OK] =
    serial.execute(zero.update(key, value))

  def update(key: Slice[Byte], value: SliceOption[Byte]): BAG[OK] =
    serial.execute(zero.update(key, value))

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Slice[Byte]): BAG[OK] =
    serial.execute(zero.update(fromKey, to, value))

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: SliceOption[Byte]): BAG[OK] =
    serial.execute(zero.update(fromKey, to, value))

  def function(key: Slice[Byte], function: Slice[Byte]): BAG[OK] =
    serial.execute(zero.applyFunction(key, function))

  def function(from: Slice[Byte], to: Slice[Byte], function: Slice[Byte]): BAG[OK] =
    serial.execute(zero.applyFunction(from, to, function))

  def registerFunction(functionId: Slice[Byte], function: SwayFunction): BAG[OK] =
    zero.run(_.registerFunction(functionId, function))

  def head[BAG[_]](readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[TupleOrNone[Slice[Byte], SliceOption[Byte]]] =
    zero.run(_.head(readState).toTupleOrNone)

  def headKey[BAG[_]](readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[SliceOption[Byte]] =
    zero.run(_.headKey(readState))

  def last[BAG[_]](readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[TupleOrNone[Slice[Byte], SliceOption[Byte]]] =
    zero.run(_.last(readState).toTupleOrNone)

  def lastKey[BAG[_]](readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[SliceOption[Byte]] =
    zero.run(_.lastKey(readState))

  def bloomFilterKeyValueCount: BAG[Int] =
    zero.run(_.keyValueCount)

  def deadline(key: Slice[Byte],
               readState: ThreadReadState): BAG[Option[Deadline]] =
    zero.run(_.deadline(key, readState))

  def sizeOfSegments: Long =
    zero.sizeOfSegments

  def contains(key: Slice[Byte],
               readState: ThreadReadState): BAG[Boolean] =
    zero.run(_.contains(key, readState))

  def mightContainKey(key: Slice[Byte]): BAG[Boolean] =
    zero.run(_.mightContainKey(key))

  def mightContainFunction(functionId: Slice[Byte]): BAG[Boolean] =
    zero.run(_.mightContainFunction(functionId))

  def get(key: Slice[Byte],
          readState: ThreadReadState): BAG[Option[SliceOption[Byte]]] =
    zero.run(_.get(key, readState).getValue)

  def getKey[BAG[_]](key: Slice[Byte],
                     readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[SliceOption[Byte]] =
    zero.run(_.getKey(key, readState))

  def getKeyValue[BAG[_]](key: Slice[Byte],
                          readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[TupleOrNone[Slice[Byte], SliceOption[Byte]]] =
    zero.run(_.get(key, readState).toTupleOrNone)

  def before[BAG[_]](key: Slice[Byte],
                     readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[TupleOrNone[Slice[Byte], SliceOption[Byte]]] =
    zero.run(_.lower(key, readState).toTupleOrNone)

  def beforeKey[BAG[_]](key: Slice[Byte],
                        readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[SliceOption[Byte]] =
    zero.run(_.lower(key, readState).getKey)

  def after[BAG[_]](key: Slice[Byte],
                    readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[TupleOrNone[Slice[Byte], SliceOption[Byte]]] =
    zero.run(_.higher(key, readState).toTupleOrNone)

  def afterKey[BAG[_]](key: Slice[Byte],
                       readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[SliceOption[Byte]] =
    zero.run(_.higher(key, readState).getKey)

  def valueSize(key: Slice[Byte],
                readState: ThreadReadState): BAG[Option[Int]] =
    zero.run(_.valueSize(key, readState))

  def levelZeroMeter: LevelZeroMeter =
    zero.levelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    zero.meterFor(levelNumber)

  def close(): BAG[Unit] =
    onClose.run(0)

  def delete(): BAG[Unit] =
    onClose.flatMapIO(_ => zero.delete).run(0)

  def clear(readState: ThreadReadState): BAG[OK] =
    zero.run(_.clear(readState))

  def toBag[BAG[_]](implicit bag: Bag[BAG]): Core[BAG] =
    new Core[BAG](
      zero = zero,
      threadStateCache = threadStateCache,
      onClose = onClose
    )(bag)
}
