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

package swaydb.core

import java.util.function.Supplier

import swaydb.Error.Level.ExceptionHandler
import swaydb.core.data.KeyValue._
import swaydb.core.data.{Memory, SwayFunction, Time, Value}
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
import swaydb.data.slice.Slice
import swaydb.{Done, IO, Prepare, Tag}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Deadline

private[swaydb] object Core {

  def apply(config: SwayDBPersistentConfig,
            enableTimer: Boolean,
            fileCache: FileCache.Enable,
            memoryCache: MemoryCache)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                      timeOrder: TimeOrder[Slice[Byte]],
                                      functionStore: FunctionStore): IO[swaydb.Error.Boot, Core[IO.ApiIO]] =
    CoreInitializer(
      config = config,
      enableTimer = enableTimer,
      fileCache = fileCache,
      memoryCache = memoryCache
    )

  def apply(config: SwayDBMemoryConfig,
            enableTimer: Boolean,
            fileCache: FileCache.Enable,
            memoryCache: MemoryCache)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                      timeOrder: TimeOrder[Slice[Byte]],
                                      functionStore: FunctionStore): IO[swaydb.Error.Boot, Core[IO.ApiIO]] =
    CoreInitializer(
      config = config,
      enableTimer = enableTimer,
      fileCache = fileCache,
      memoryCache = memoryCache
    )

  def apply(enableTimer: Boolean,
            config: LevelZeroPersistentConfig)(implicit mmapCleanerEC: Option[ExecutionContext],
                                               keyOrder: KeyOrder[Slice[Byte]],
                                               timeOrder: TimeOrder[Slice[Byte]],
                                               functionStore: FunctionStore): IO[swaydb.Error.Boot, Core[IO.ApiIO]] =
    CoreInitializer(
      config = config,
      enableTimer = enableTimer
    )

  def apply(config: LevelZeroMemoryConfig,
            enableTimer: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  timeOrder: TimeOrder[Slice[Byte]],
                                  functionStore: FunctionStore): IO[swaydb.Error.Boot, Core[IO.ApiIO]] =
    CoreInitializer(
      config = config,
      enableTimer = enableTimer
    )

  private def prepareToMapEntry(entries: Iterator[Prepare[Slice[Byte], Option[Slice[Byte]], Slice[Byte]]])(timer: Timer): Option[MapEntry[Slice[Byte], Memory]] =
    entries.foldLeft(Option.empty[MapEntry[Slice[Byte], Memory]]) {
      case (mapEntry, prepare) =>
        val nextEntry =
          prepare match {
            case Prepare.Put(key, value, expire) =>
              if (key.isEmpty) throw new Exception("Key cannot be empty.")

              MapEntry.Put[Slice[Byte], Memory.Put](key, Memory.Put(key, value.getOrElse(Slice.Null), expire, timer.next))(LevelZeroMapEntryWriter.Level0PutWriter)

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
                  (MapEntry.Put[Slice[Byte], Memory.Range](key, Memory.Range(key, toKey, Value.FromValue.Null, Value.Update(value.getOrElse(Slice.Null), None, timer.next)))(LevelZeroMapEntryWriter.Level0RangeWriter): MapEntry[Slice[Byte], Memory]) ++
                    MapEntry.Put[Slice[Byte], Memory.Update](toKey, Memory.Update(toKey, value.getOrElse(Slice.Null), None, timer.next))(LevelZeroMapEntryWriter.Level0UpdateWriter)
              } getOrElse {
                MapEntry.Put[Slice[Byte], Memory.Update](key, Memory.Update(key, value.getOrElse(Slice.Null), None, timer.next))(LevelZeroMapEntryWriter.Level0UpdateWriter)
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

private[swaydb] class Core[T[_]](val zero: LevelZero,
                                 onClose: => IO.Defer[swaydb.Error.Close, Unit])(implicit tag: Tag[T]) {

  private val serial = tag.createSerial()

  protected[swaydb] val readStates =
    ThreadLocal.withInitial[ThreadReadState] {
      new Supplier[ThreadReadState] {
        override def get(): ThreadReadState = ThreadReadState.limitHashMap(10, 2)
      }
    }

  def put(key: Slice[Byte]): T[Done] =
    serial.execute(zero.put(key))

  def put(key: Slice[Byte], value: Slice[Byte]): T[Done] =
    serial.execute(zero.put(key, value))

  def put(key: Slice[Byte], value: Option[Slice[Byte]]): T[Done] =
    serial.execute(zero.put(key, value))

  def put(key: Slice[Byte], value: Option[Slice[Byte]], removeAt: Deadline): T[Done] =
    serial.execute(zero.put(key, value, removeAt))

  /**
   * Each [[Prepare]] requires a new next [[Time]] for cases where a batch contains overriding keys.
   *
   * Same time indicates that the later Prepare in this batch with the same time as newer Prepare has already applied
   * to the newer prepare therefore ignoring the newer prepare.
   *
   * @note If the default time order [[TimeOrder.long]] is used
   *       Times should always be unique and in incremental order for *ALL* key values.
   */
  def put(entries: Iterator[Prepare[Slice[Byte], Option[Slice[Byte]], Slice[Byte]]]): T[Done] =
    if (entries.isEmpty)
      tag.failure(new IllegalArgumentException("Cannot write empty batch"))
    else
      serial.execute(zero.put(Core.prepareToMapEntry(entries)(_).get)) //Gah .get!

  def remove(key: Slice[Byte]): T[Done] =
    serial.execute(zero.remove(key))

  def remove(key: Slice[Byte], at: Deadline): T[Done] =
    serial.execute(zero.remove(key, at))

  def remove(from: Slice[Byte], to: Slice[Byte]): T[Done] =
    serial.execute(zero.remove(from, to))

  def remove(from: Slice[Byte], to: Slice[Byte], at: Deadline): T[Done] =
    serial.execute(zero.remove(from, to, at))

  def update(key: Slice[Byte], value: Slice[Byte]): T[Done] =
    serial.execute(zero.update(key, value))

  def update(key: Slice[Byte], value: Option[Slice[Byte]]): T[Done] =
    serial.execute(zero.update(key, value))

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Slice[Byte]): T[Done] =
    serial.execute(zero.update(fromKey, to, value))

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Option[Slice[Byte]]): T[Done] =
    serial.execute(zero.update(fromKey, to, value))

  def function(key: Slice[Byte], function: Slice[Byte]): T[Done] =
    serial.execute(zero.applyFunction(key, function))

  def function(from: Slice[Byte], to: Slice[Byte], function: Slice[Byte]): T[Done] =
    serial.execute(zero.applyFunction(from, to, function))

  def registerFunction(functionId: Slice[Byte], function: SwayFunction): T[Done] =
    zero.registerFunction(functionId, function).run

  def head(readState: ThreadReadState): T[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    zero.run(_.head(readState))

  def headKey(readState: ThreadReadState): T[Option[Slice[Byte]]] =
    zero.headKey(readState).run

  def last(readState: ThreadReadState): T[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    zero.run(_.last(readState))

  def lastKey(readState: ThreadReadState): T[Option[Slice[Byte]]] =
    zero.lastKey(readState).run

  def bloomFilterKeyValueCount: T[Int] =
    IO.Defer(zero.keyValueCount).run

  def deadline(key: Slice[Byte],
               readState: ThreadReadState): T[Option[Deadline]] =
    zero.deadline(key, readState).run

  def sizeOfSegments: Long =
    zero.sizeOfSegments

  def contains(key: Slice[Byte],
               readState: ThreadReadState): T[Boolean] =
    zero.contains(key, readState).run

  def mightContainKey(key: Slice[Byte]): T[Boolean] =
    IO.Defer(zero.mightContainKey(key)).run

  def mightContainFunction(functionId: Slice[Byte]): T[Boolean] =
    IO.Defer(zero.mightContainFunction(functionId)).run

  def get(key: Slice[Byte],
          readState: ThreadReadState): T[Option[Option[Slice[Byte]]]] =
    tag.map(zero.run(_.get(key, readState)))(_.map(_._2))

  def getKey(key: Slice[Byte],
             readState: ThreadReadState): T[Option[Slice[Byte]]] =
    zero.getKey(key, readState).run

  def getKeyValue(key: Slice[Byte],
                  readState: ThreadReadState): T[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    zero.run(_.get(key, readState))

  def before(key: Slice[Byte],
             readState: ThreadReadState): T[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    zero.run(_.lower(key, readState))

  def beforeKey(key: Slice[Byte],
                readState: ThreadReadState): T[Option[Slice[Byte]]] =
    tag.map(zero.lower(key, readState).run)(_.map(_.key))

  def after(key: Slice[Byte],
            readState: ThreadReadState): T[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    zero.run(_.higher(key, readState))

  def afterKey(key: Slice[Byte],
               readState: ThreadReadState): T[Option[Slice[Byte]]] =
    tag.map(zero.higher(key, readState).run)(_.map(_.key))

  def valueSize(key: Slice[Byte],
                readState: ThreadReadState): T[Option[Int]] =
    zero.valueSize(key, readState).run

  def levelZeroMeter: LevelZeroMeter =
    zero.levelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    zero.meterFor(levelNumber)

  def close(): T[Unit] =
    onClose.run

  def delete(): T[Unit] =
    onClose.flatMapIO(_ => zero.delete).run

  def clear(readState: ThreadReadState): T[Done] =
    zero.clear(readState).run

  def toTag[X[_]](implicit tag: Tag[X]): Core[X] =
    new Core[X](zero, onClose)(tag)
}
