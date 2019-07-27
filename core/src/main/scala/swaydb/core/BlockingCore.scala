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

import swaydb.Error.Level.ErrorHandler
import swaydb.core.data.KeyValue._
import swaydb.core.data.{Memory, SwayFunction, Time, Value}
import swaydb.core.function.FunctionStore
import swaydb.core.level.zero.LevelZero
import swaydb.core.map.MapEntry
import swaydb.core.map.serializer.LevelZeroMapEntryWriter
import swaydb.core.map.timer.Timer
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.config.{LevelZeroConfig, SwayDBConfig}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.{IO, Prepare, Tag}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Deadline, FiniteDuration}

private[swaydb] object BlockingCore {

  def apply(config: SwayDBConfig,
            maxOpenSegments: Int,
            cacheSize: Long,
            cacheCheckDelay: FiniteDuration,
            segmentsOpenCheckDelay: FiniteDuration,
            fileOpenLimiterEC: ExecutionContext,
            cacheLimiterEC: ExecutionContext)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                              timeOrder: TimeOrder[Slice[Byte]],
                                              functionStore: FunctionStore): IO[swaydb.Error.BootUp, BlockingCore[Tag.API]] =
    CoreInitializer(
      config = config,
      maxSegmentsOpen = maxOpenSegments,
      cacheSize = cacheSize,
      keyValueQueueDelay = cacheCheckDelay,
      segmentCloserDelay = segmentsOpenCheckDelay,
      fileOpenLimiterEC = fileOpenLimiterEC,
      cacheLimiterEC = cacheLimiterEC
    )

  def apply(config: LevelZeroConfig)(implicit mmapCleanerEC: ExecutionContext,
                                     keyOrder: KeyOrder[Slice[Byte]],
                                     timeOrder: TimeOrder[Slice[Byte]],
                                     functionStore: FunctionStore): IO[swaydb.Error.BootUp, BlockingCore[Tag.API]] =
    CoreInitializer(
      config = config,
      bufferCleanerEC = mmapCleanerEC
    )

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

private[swaydb] case class BlockingCore[T[_]](zero: LevelZero, onClose: () => IO[swaydb.Error.Close, Unit])(implicit tag: Tag[T]) extends Core[T] {

  def put(key: Slice[Byte]): T[IO.Done] =
    tag.fromIO(zero.put(key))

  def put(key: Slice[Byte], value: Slice[Byte]): T[IO.Done] =
    tag.fromIO(zero.put(key, value))

  def put(key: Slice[Byte], value: Option[Slice[Byte]]): T[IO.Done] =
    tag.fromIO(zero.put(key, value))

  def put(key: Slice[Byte], value: Option[Slice[Byte]], removeAt: Deadline): T[IO.Done] =
    tag.fromIO(zero.put(key, value, removeAt))

  /**
    * Each [[Prepare]] requires a new next [[Time]] for cases where a batch contains overriding keys.
    *
    * Same time indicates that the later Prepare in this batch with the same time as newer Prepare has already applied
    * to the newer prepare therefore ignoring the newer prepare.
    *
    * NOTE: If the default time order [[TimeOrder.long]] is used
    * Times should always be unique and in incremental order for *ALL* key values.
    */
  def put(entries: Iterable[Prepare[Slice[Byte], Option[Slice[Byte]]]]): T[IO.Done] =
    if (entries.isEmpty)
      tag.fromIO(IO.failed("Cannot write empty batch"))
    else
      tag.fromIO(zero.put(BlockingCore.prepareToMapEntry(entries)(_).get)) //Gah .get! hmm.

  def remove(key: Slice[Byte]): T[IO.Done] =
    tag.fromIO(zero.remove(key))

  def remove(key: Slice[Byte], at: Deadline): T[IO.Done] =
    tag.fromIO(zero.remove(key, at))

  def remove(from: Slice[Byte], to: Slice[Byte]): T[IO.Done] =
    tag.fromIO(zero.remove(from, to))

  def remove(from: Slice[Byte], to: Slice[Byte], at: Deadline): T[IO.Done] =
    tag.fromIO(zero.remove(from, to, at))

  def update(key: Slice[Byte], value: Slice[Byte]): T[IO.Done] =
    tag.fromIO(zero.update(key, value))

  def update(key: Slice[Byte], value: Option[Slice[Byte]]): T[IO.Done] =
    tag.fromIO(zero.update(key, value))

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Slice[Byte]): T[IO.Done] =
    tag.fromIO(zero.update(fromKey, to, value))

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Option[Slice[Byte]]): T[IO.Done] =
    tag.fromIO(zero.update(fromKey, to, value))

  override def clear(): T[IO.Done] =
    tag.fromIO(zero.clear().runBlocking)

  def function(key: Slice[Byte], function: Slice[Byte]): T[IO.Done] =
    tag.fromIO(zero.applyFunction(key, function))

  def function(from: Slice[Byte], to: Slice[Byte], function: Slice[Byte]): T[IO.Done] =
    tag.fromIO(zero.applyFunction(from, to, function))

  def registerFunction(functionID: Slice[Byte], function: SwayFunction): SwayFunction =
    zero.registerFunction(functionID, function)

  private def headIO: IO[swaydb.Error.Level, Option[KeyValueTuple]] =
    zero.head.runBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Defer.recover(response.getOrFetchValue.get).runBlocking map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: swaydb.Error.ReservedIO =>
                    headIO

                  case failure =>
                    IO.Failure(error = failure)
                }
            }
        } getOrElse IO.none
    }

  def head: T[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    tag.fromIO(headIO)

  def headKey: T[Option[Slice[Byte]]] =
    tag.fromIO(zero.headKey.runBlocking)

  private def lastIO: IO[swaydb.Error.Level, Option[KeyValueTuple]] =
    zero.last.runBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Defer.recover(response.getOrFetchValue.get).runBlocking map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: swaydb.Error.ReservedIO =>
                    lastIO

                  case failure =>
                    IO.Failure(error = failure)
                }
            }
        } getOrElse IO.none
    }

  def last: T[Option[KeyValueTuple]] =
    tag.fromIO(lastIO)

  def lastKey: T[Option[Slice[Byte]]] =
    tag.fromIO(zero.lastKey.runBlocking)

  def bloomFilterKeyValueCount: T[Int] =
    tag.fromIO(IO.Defer.recover(zero.bloomFilterKeyValueCount.get).runBlocking)

  def deadline(key: Slice[Byte]): T[Option[Deadline]] =
    tag.fromIO(zero.deadline(key).runBlocking)

  def sizeOfSegments: Long =
    zero.sizeOfSegments

  def contains(key: Slice[Byte]): T[Boolean] =
    tag.fromIO(zero.contains(key).runBlocking)

  def mightContainKey(key: Slice[Byte]): T[Boolean] =
    tag.fromIO(IO.Defer.recover(zero.mightContainKey(key).get).runBlocking)

  def mightContainFunction(functionId: Slice[Byte]): T[Boolean] =
    tag.fromIO(IO.Defer.recover(zero.mightContainFunction(functionId).get).runBlocking)

  private def getIO(key: Slice[Byte]): IO[swaydb.Error.Level, Option[Option[Slice[Byte]]]] =
    zero.get(key).runBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Defer.recover(response.getOrFetchValue.get).runBlocking map {
              result =>
                Some(result)
            } recoverWith[swaydb.Error.Level, Option[Option[Slice[Byte]]]] {
              case error =>
                error match {
                  case _: swaydb.Error.ReservedIO =>
                    getIO(key)

                  case failure =>
                    IO.Failure(error = failure)
                }
            }
        } getOrElse IO.none
    }

  def get(key: Slice[Byte]): T[Option[Option[Slice[Byte]]]] =
    tag.fromIO(getIO(key))

  def getKey(key: Slice[Byte]): T[Option[Slice[Byte]]] =
    tag.fromIO(zero.getKey(key).runBlocking)

  private def getKeyValueIO(key: Slice[Byte]): IO[swaydb.Error.Level, Option[KeyValueTuple]] =
    zero.get(key).runBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Defer.recover(response.getOrFetchValue.get).runBlocking map {
              result =>
                Some(response.key, result)
            } recoverWith[swaydb.Error.Level, Option[KeyValueTuple]] {
              case error =>
                error match {
                  case _: swaydb.Error.ReservedIO =>
                    getKeyValueIO(key)

                  case failure =>
                    IO.Failure(error = failure)
                }
            }
        } getOrElse IO.none
    }

  def getKeyValue(key: Slice[Byte]): T[Option[KeyValueTuple]] =
    tag.fromIO(getKeyValueIO(key))

  private def beforeIO(key: Slice[Byte]): IO[swaydb.Error.Level, Option[KeyValueTuple]] =
    zero.lower(key).runBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Defer.recover(response.getOrFetchValue.get).runBlocking map {
              result =>
                Some(response.key, result)
            } recoverWith[swaydb.Error.Level, Option[KeyValueTuple]] {
              case error =>
                error match {
                  case _: swaydb.Error.ReservedIO =>
                    beforeIO(key)

                  case failure =>
                    IO.Failure(error = failure)
                }
            }
        } getOrElse IO.none
    }

  def before(key: Slice[Byte]): T[Option[KeyValueTuple]] =
    tag.fromIO(beforeIO(key))

  def beforeKey(key: Slice[Byte]): T[Option[Slice[Byte]]] =
    tag.fromIO(zero.lower(key).runBlocking.map(_.map(_.key)))

  private def afterIO(key: Slice[Byte]): IO[swaydb.Error.Level, Option[KeyValueTuple]] =
    zero.higher(key).runBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Defer.recover(response.getOrFetchValue.get).runBlocking map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: swaydb.Error.ReservedIO =>
                    afterIO(key)

                  case failure =>
                    IO.Failure(error = failure)
                }
            }
        } getOrElse IO.none
    }

  def after(key: Slice[Byte]): T[Option[KeyValueTuple]] =
    tag.fromIO(afterIO(key))

  def afterKey(key: Slice[Byte]): T[Option[Slice[Byte]]] =
    tag.fromIO(zero.higher(key).runBlocking.map(_.map(_.key)))

  def valueSize(key: Slice[Byte]): T[Option[Int]] =
    tag.fromIO(zero.valueSize(key).runBlocking)

  def level0Meter: LevelZeroMeter =
    zero.levelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    zero.meterFor(levelNumber)

  def close(): T[Unit] =
    tag.fromIO(onClose().flatMap(_ => zero.close))

  def delete(): T[Unit] =
    tag.fromIO(onClose().flatMap(_ => zero.delete))

  override def tagAsync[T[_]](implicit ec: ExecutionContext, tag: Tag.Async[T]): Core[T] =
    AsyncCore(zero, onClose)

  override def tagBlocking[T[_]](implicit tag: Tag[T]): BlockingCore[T] =
    BlockingCore(zero, onClose)
}
