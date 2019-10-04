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
import swaydb.core.segment.ReadState
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.config._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.{IO, Prepare, Tag}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Deadline

private[swaydb] object Core {

  def apply(config: SwayDBPersistentConfig,
            fileCache: FileCache.Enable,
            memoryCache: MemoryCache)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                      timeOrder: TimeOrder[Slice[Byte]],
                                      functionStore: FunctionStore): IO[swaydb.Error.Boot, Core[IO.ApiIO]] =
    CoreInitializer(
      config = config,
      fileCache = fileCache,
      memoryCache = memoryCache
    )

  def apply(config: SwayDBMemoryConfig,
            fileCache: FileCache.Enable,
            memoryCache: MemoryCache)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                      timeOrder: TimeOrder[Slice[Byte]],
                                      functionStore: FunctionStore): IO[swaydb.Error.Boot, Core[IO.ApiIO]] =
    CoreInitializer(
      config = config,
      fileCache = fileCache,
      memoryCache = memoryCache
    )

  def apply(config: LevelZeroPersistentConfig)(implicit mmapCleanerEC: Option[ExecutionContext],
                                               keyOrder: KeyOrder[Slice[Byte]],
                                               timeOrder: TimeOrder[Slice[Byte]],
                                               functionStore: FunctionStore): IO[swaydb.Error.Boot, Core[IO.ApiIO]] =
    CoreInitializer(config = config)

  def apply(config: LevelZeroMemoryConfig)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                           timeOrder: TimeOrder[Slice[Byte]],
                                           functionStore: FunctionStore): IO[swaydb.Error.Boot, Core[IO.ApiIO]] =
    CoreInitializer(config = config)

  private def prepareToMapEntry(entries: Iterable[Prepare[Slice[Byte], Option[Slice[Byte]]]])(timer: Timer): Option[MapEntry[Slice[Byte], Memory]] =
    entries.foldLeft(Option.empty[MapEntry[Slice[Byte], Memory]]) {
      case (mapEntry, prepare) =>
        val nextEntry =
          prepare match {
            case Prepare.Put(key, value, expire) =>
              if (key.isEmpty) throw new Exception("Key cannot be empty.")

              MapEntry.Put[Slice[Byte], Memory.Put](key, Memory.Put(key, value, expire, timer.next))(LevelZeroMapEntryWriter.Level0PutWriter)

            case Prepare.Add(key, expire) =>
              if (key.isEmpty) throw new Exception("Key cannot be empty.")

              MapEntry.Put[Slice[Byte], Memory.Put](key, Memory.Put(key, None, expire, timer.next))(LevelZeroMapEntryWriter.Level0PutWriter)

            case Prepare.Remove(key, toKey, expire) =>
              if (key.isEmpty) throw new Exception("Key cannot be empty.")
              if (toKey.exists(_.isEmpty)) throw new Exception("toKey cannot be empty.")

              toKey map {
                toKey =>
                  (MapEntry.Put[Slice[Byte], Memory.Range](key, Memory.Range(key, toKey, None, Value.Remove(expire, timer.next)))(LevelZeroMapEntryWriter.Level0RangeWriter): MapEntry[Slice[Byte], Memory]) ++
                    MapEntry.Put[Slice[Byte], Memory.Remove](toKey, Memory.Remove(toKey, expire, timer.next))(LevelZeroMapEntryWriter.Level0RemoveWriter)
              } getOrElse {
                MapEntry.Put[Slice[Byte], Memory.Remove](key, Memory.Remove(key, expire, timer.next))(LevelZeroMapEntryWriter.Level0RemoveWriter)
              }

            case Prepare.Update(key, toKey, value) =>
              if (key.isEmpty) throw new Exception("Key cannot be empty.")
              if (toKey.exists(_.isEmpty)) throw new Exception("toKey cannot be empty.")

              toKey map {
                toKey =>
                  (MapEntry.Put[Slice[Byte], Memory.Range](key, Memory.Range(key, toKey, None, Value.Update(value, None, timer.next)))(LevelZeroMapEntryWriter.Level0RangeWriter): MapEntry[Slice[Byte], Memory]) ++
                    MapEntry.Put[Slice[Byte], Memory.Update](toKey, Memory.Update(toKey, None, None, timer.next))(LevelZeroMapEntryWriter.Level0UpdateWriter)
              } getOrElse {
                MapEntry.Put[Slice[Byte], Memory.Update](key, Memory.Update(key, value, None, timer.next))(LevelZeroMapEntryWriter.Level0UpdateWriter)
              }

            case Prepare.ApplyFunction(key, toKey, function) =>
              if (key.isEmpty) throw new Exception("Key cannot be empty.")
              if (toKey.exists(_.isEmpty)) throw new Exception("toKey cannot be empty.")

              toKey map {
                toKey =>
                  (MapEntry.Put[Slice[Byte], Memory.Range](key, Memory.Range(key, toKey, None, Value.Function(function, timer.next)))(LevelZeroMapEntryWriter.Level0RangeWriter): MapEntry[Slice[Byte], Memory]) ++
                    MapEntry.Put[Slice[Byte], Memory.Function](toKey, Memory.Function(toKey, function, timer.next))(LevelZeroMapEntryWriter.Level0FunctionWriter)
              } getOrElse {
                MapEntry.Put[Slice[Byte], Memory.Function](key, Memory.Function(key, function, timer.next))(LevelZeroMapEntryWriter.Level0FunctionWriter)
              }
          }
        Some(mapEntry.map(_ ++ nextEntry) getOrElse nextEntry)
    }
}

private[swaydb] class Core[T[_]](zero: LevelZero,
                                 onClose: => IO.Defer[swaydb.Error.Close, Unit])(implicit tag: Tag[T]) {

  import Tag.Implicits._

  private val serial = tag.applySerial()

  protected[swaydb] val readStates =
    ThreadLocal.withInitial[ReadState] {
      new Supplier[ReadState] {
        override def get(): ReadState = ReadState.limitHashMap(10, 2)
      }
    }

  def put(key: Slice[Byte]): T[IO.Done] =
    serial.execute(zero.put(key))

  def put(key: Slice[Byte], value: Slice[Byte]): T[IO.Done] =
    serial.execute(zero.put(key, value))

  def put(key: Slice[Byte], value: Option[Slice[Byte]]): T[IO.Done] =
    serial.execute(zero.put(key, value))

  def put(key: Slice[Byte], value: Option[Slice[Byte]], removeAt: Deadline): T[IO.Done] =
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
  def put(entries: Iterable[Prepare[Slice[Byte], Option[Slice[Byte]]]]): T[IO.Done] =
    if (entries.isEmpty)
      tag.failure(new IllegalArgumentException("Cannot write empty batch"))
    else
      serial.execute(zero.put(Core.prepareToMapEntry(entries)(_).get)) //Gah .get!

  def remove(key: Slice[Byte]): T[IO.Done] =
    serial.execute(zero.remove(key))

  def remove(key: Slice[Byte], at: Deadline): T[IO.Done] =
    serial.execute(zero.remove(key, at))

  def remove(from: Slice[Byte], to: Slice[Byte]): T[IO.Done] =
    serial.execute(zero.remove(from, to))

  def remove(from: Slice[Byte], to: Slice[Byte], at: Deadline): T[IO.Done] =
    serial.execute(zero.remove(from, to, at))

  def update(key: Slice[Byte], value: Slice[Byte]): T[IO.Done] =
    serial.execute(zero.update(key, value))

  def update(key: Slice[Byte], value: Option[Slice[Byte]]): T[IO.Done] =
    serial.execute(zero.update(key, value))

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Slice[Byte]): T[IO.Done] =
    serial.execute(zero.update(fromKey, to, value))

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Option[Slice[Byte]]): T[IO.Done] =
    serial.execute(zero.update(fromKey, to, value))

  def function(key: Slice[Byte], function: Slice[Byte]): T[IO.Done] =
    serial.execute(zero.applyFunction(key, function))

  def function(from: Slice[Byte], to: Slice[Byte], function: Slice[Byte]): T[IO.Done] =
    serial.execute(zero.applyFunction(from, to, function))

  def registerFunction(functionID: Slice[Byte], function: SwayFunction): SwayFunction =
    zero.registerFunction(functionID, function)

  def head(readState: ReadState): T[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    zero.run(_.head(readState))

  def headKey(readState: ReadState): T[Option[Slice[Byte]]] =
    zero.headKey(readState).run

  def last(readState: ReadState): T[Option[KeyValueTuple]] =
    zero.run(_.last(readState))

  def lastKey(readState: ReadState): T[Option[Slice[Byte]]] =
    zero.lastKey(readState).run

  def bloomFilterKeyValueCount: T[Int] =
    IO.Defer(zero.bloomFilterKeyValueCount).run

  def deadline(key: Slice[Byte],
               readState: ReadState): T[Option[Deadline]] =
    zero.deadline(key, readState).run

  def sizeOfSegments: Long =
    zero.sizeOfSegments

  def contains(key: Slice[Byte],
               readState: ReadState): T[Boolean] =
    zero.contains(key, readState).run

  def mightContainKey(key: Slice[Byte]): T[Boolean] =
    IO.Defer(zero.mightContainKey(key)).run

  def mightContainFunction(functionId: Slice[Byte]): T[Boolean] =
    IO.Defer(zero.mightContainFunction(functionId)).run

  def get(key: Slice[Byte],
          readState: ReadState): T[Option[Option[Slice[Byte]]]] =
    zero.run(_.get(key, readState)).map(_.map(_._2))

  def getKey(key: Slice[Byte],
             readState: ReadState): T[Option[Slice[Byte]]] =
    zero.getKey(key, readState).run

  def getKeyValue(key: Slice[Byte],
                  readState: ReadState): T[Option[KeyValueTuple]] =
    zero.run(_.get(key, readState))

  def before(key: Slice[Byte],
             readState: ReadState): T[Option[KeyValueTuple]] =
    zero.run(_.lower(key, readState))

  def beforeKey(key: Slice[Byte],
                readState: ReadState): T[Option[Slice[Byte]]] =
    zero.lower(key, readState).run.map(_.map(_.key))

  def after(key: Slice[Byte],
            readState: ReadState): T[Option[KeyValueTuple]] =
    zero.run(_.higher(key, readState))

  def afterKey(key: Slice[Byte],
               readState: ReadState): T[Option[Slice[Byte]]] =
    zero.higher(key, readState).run.map(_.map(_.key))

  def valueSize(key: Slice[Byte],
                readState: ReadState): T[Option[Int]] =
    zero.valueSize(key, readState).run

  def level0Meter: LevelZeroMeter =
    zero.levelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    zero.meterFor(levelNumber)

  def close(): T[Unit] =
    onClose.run

  def delete(): T[Unit] =
    onClose.flatMapIO(_ => zero.delete).run

  def clear(readState: ReadState): T[IO.Done] =
    zero.clear(readState).run

  def toTag[X[_]](implicit tag: Tag[X]): Core[X] =
    new Core[X](zero, onClose)(tag)
}
