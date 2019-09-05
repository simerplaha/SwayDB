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

import swaydb.Error.Level.ExceptionHandler
import swaydb.core.data.KeyValue._
import swaydb.core.data.{Memory, SwayFunction, Time, Value}
import swaydb.core.function.FunctionStore
import swaydb.core.level.zero.LevelZero
import swaydb.core.map.MapEntry
import swaydb.core.map.serializer.LevelZeroMapEntryWriter
import swaydb.core.map.timer.Timer
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

  private def prepareToMapEntry(entries: Iterable[Prepare[Slice[Byte], Option[Slice[Byte]]]])(timer: Timer): Option[MapEntry[Slice[Byte], Memory.SegmentResponse]] =
    entries.foldLeft(Option.empty[MapEntry[Slice[Byte], Memory.SegmentResponse]]) {
      case (mapEntry, prepare) =>
        val nextEntry =
          prepare match {
            case Prepare.Put(key, value, expire) =>
              MapEntry.Put[Slice[Byte], Memory.Put](key, Memory.Put(key, value, expire, timer.next))(LevelZeroMapEntryWriter.Level0PutWriter)

            case Prepare.Add(key, expire) =>
              MapEntry.Put[Slice[Byte], Memory.Put](key, Memory.Put(key, None, expire, timer.next))(LevelZeroMapEntryWriter.Level0PutWriter)

            case Prepare.Remove(key, toKey, expire) =>
              toKey map {
                toKey =>
                  (MapEntry.Put[Slice[Byte], Memory.Range](key, Memory.Range(key, toKey, None, Value.Remove(expire, timer.next)))(LevelZeroMapEntryWriter.Level0RangeWriter): MapEntry[Slice[Byte], Memory.SegmentResponse]) ++
                    MapEntry.Put[Slice[Byte], Memory.Remove](toKey, Memory.Remove(toKey, expire, timer.next))(LevelZeroMapEntryWriter.Level0RemoveWriter)
              } getOrElse {
                MapEntry.Put[Slice[Byte], Memory.Remove](key, Memory.Remove(key, expire, timer.next))(LevelZeroMapEntryWriter.Level0RemoveWriter)
              }

            case Prepare.Update(key, toKey, value) =>
              toKey map {
                toKey =>
                  (MapEntry.Put[Slice[Byte], Memory.Range](key, Memory.Range(key, toKey, None, Value.Update(value, None, timer.next)))(LevelZeroMapEntryWriter.Level0RangeWriter): MapEntry[Slice[Byte], Memory.SegmentResponse]) ++
                    MapEntry.Put[Slice[Byte], Memory.Update](toKey, Memory.Update(toKey, None, None, timer.next))(LevelZeroMapEntryWriter.Level0UpdateWriter)
              } getOrElse {
                MapEntry.Put[Slice[Byte], Memory.Update](key, Memory.Update(key, value, None, timer.next))(LevelZeroMapEntryWriter.Level0UpdateWriter)
              }

            case Prepare.ApplyFunction(key, toKey, function) =>
              toKey map {
                toKey =>
                  (MapEntry.Put[Slice[Byte], Memory.Range](key, Memory.Range(key, toKey, None, Value.Function(function, timer.next)))(LevelZeroMapEntryWriter.Level0RangeWriter): MapEntry[Slice[Byte], Memory.SegmentResponse]) ++
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

  def put(key: Slice[Byte]): T[IO.Done] =
    tag.point(tag.fromIO(zero.put(key)))

  def put(key: Slice[Byte], value: Slice[Byte]): T[IO.Done] =
    tag.point(tag.fromIO(zero.put(key, value)))

  def put(key: Slice[Byte], value: Option[Slice[Byte]]): T[IO.Done] =
    tag.point(tag.fromIO(zero.put(key, value)))

  def put(key: Slice[Byte], value: Option[Slice[Byte]], removeAt: Deadline): T[IO.Done] =
    tag.point(tag.fromIO(zero.put(key, value, removeAt)))

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
      tag.point(tag.fromIO(IO.failed("Cannot write empty batch")))
    else
      tag.point(tag.fromIO(zero.put(Core.prepareToMapEntry(entries)(_).get))) //Gah .get! hmm.

  def remove(key: Slice[Byte]): T[IO.Done] =
    tag.point(tag.fromIO(zero.remove(key)))

  def remove(key: Slice[Byte], at: Deadline): T[IO.Done] =
    tag.point(tag.fromIO(zero.remove(key, at)))

  def remove(from: Slice[Byte], to: Slice[Byte]): T[IO.Done] =
    tag.point(tag.fromIO(zero.remove(from, to)))

  def remove(from: Slice[Byte], to: Slice[Byte], at: Deadline): T[IO.Done] =
    tag.point(tag.fromIO(zero.remove(from, to, at)))

  def update(key: Slice[Byte], value: Slice[Byte]): T[IO.Done] =
    tag.point(tag.fromIO(zero.update(key, value)))

  def update(key: Slice[Byte], value: Option[Slice[Byte]]): T[IO.Done] =
    tag.point(tag.fromIO(zero.update(key, value)))

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Slice[Byte]): T[IO.Done] =
    tag.point(tag.fromIO(zero.update(fromKey, to, value)))

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Option[Slice[Byte]]): T[IO.Done] =
    tag.point(tag.fromIO(zero.update(fromKey, to, value)))

  def function(key: Slice[Byte], function: Slice[Byte]): T[IO.Done] =
    tag.point(tag.fromIO(zero.applyFunction(key, function)))

  def function(from: Slice[Byte], to: Slice[Byte], function: Slice[Byte]): T[IO.Done] =
    tag.point(tag.fromIO(zero.applyFunction(from, to, function)))

  def registerFunction(functionID: Slice[Byte], function: SwayFunction): SwayFunction =
    zero.registerFunction(functionID, function)

  def head: T[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    zero.run(_.head)

  def headKey: T[Option[Slice[Byte]]] =
    zero.headKey.run

  def last: T[Option[KeyValueTuple]] =
    zero.run(_.last)

  def lastKey: T[Option[Slice[Byte]]] =
    zero.lastKey.run

  def bloomFilterKeyValueCount: T[Int] =
    IO.Defer(zero.bloomFilterKeyValueCount.get).run

  def deadline(key: Slice[Byte]): T[Option[Deadline]] =
    zero.deadline(key).run

  def sizeOfSegments: Long =
    zero.sizeOfSegments

  def contains(key: Slice[Byte]): T[Boolean] =
    zero.contains(key).run

  def mightContainKey(key: Slice[Byte]): T[Boolean] =
    IO.Defer(zero.mightContainKey(key).get).run

  def mightContainFunction(functionId: Slice[Byte]): T[Boolean] =
    IO.Defer(zero.mightContainFunction(functionId).get).run

  def get(key: Slice[Byte]): T[Option[Option[Slice[Byte]]]] =
    zero.run(_.get(key)).map(_.map(_._2))

  def getKey(key: Slice[Byte]): T[Option[Slice[Byte]]] =
    zero.getKey(key).run

  def getKeyValue(key: Slice[Byte]): T[Option[KeyValueTuple]] =
    zero.run(_.get(key))

  def before(key: Slice[Byte]): T[Option[KeyValueTuple]] =
    zero.run(_.lower(key))

  def beforeKey(key: Slice[Byte]): T[Option[Slice[Byte]]] =
    zero.lower(key).run.map(_.map(_.key))

  def after(key: Slice[Byte]): T[Option[KeyValueTuple]] =
    zero.run(_.higher(key))

  def afterKey(key: Slice[Byte]): T[Option[Slice[Byte]]] =
    zero.higher(key).run.map(_.map(_.key))

  def valueSize(key: Slice[Byte]): T[Option[Int]] =
    zero.valueSize(key).run

  def level0Meter: LevelZeroMeter =
    zero.levelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    zero.meterFor(levelNumber)

  def close(): T[Unit] =
    onClose.run

  def delete(): T[Unit] =
    onClose.flatMapIO(_ => zero.delete).run

  def clear(): T[IO.Done] =
    zero.clear().run

  def toTag[X[_]](implicit tag: Tag[X]): Core[X] =
    new Core[X](zero, onClose)(tag)
}
