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

package swaydb.core

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Deadline, FiniteDuration}
import swaydb.Prepare
import swaydb.core.data.KeyValue._
import swaydb.core.data.{Memory, SwayFunction, Time, Value}
import swaydb.core.function.FunctionStore
import swaydb.core.level.zero.LevelZero
import swaydb.core.map.MapEntry
import swaydb.core.map.serializer.LevelZeroMapEntryWriter
import swaydb.core.map.timer.Timer
import swaydb.data.IO
import swaydb.data.IO.Error
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.config.{LevelZeroConfig, SwayDBConfig}
import swaydb.data.io.{FutureTransformer, IOTransformer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

private[swaydb] object BlockingCore {

  def apply(config: SwayDBConfig,
            maxOpenSegments: Int,
            cacheSize: Long,
            cacheCheckDelay: FiniteDuration,
            segmentsOpenCheckDelay: FiniteDuration)(implicit ec: ExecutionContext,
                                                    keyOrder: KeyOrder[Slice[Byte]],
                                                    timeOrder: TimeOrder[Slice[Byte]],
                                                    functionStore: FunctionStore): IO[BlockingCore[IO]] =
    CoreInitializer(
      config = config,
      maxSegmentsOpen = maxOpenSegments,
      cacheSize = cacheSize,
      keyValueQueueDelay = cacheCheckDelay,
      segmentCloserDelay = segmentsOpenCheckDelay
    )

  def apply(config: LevelZeroConfig)(implicit ec: ExecutionContext,
                                     keyOrder: KeyOrder[Slice[Byte]],
                                     timeOrder: TimeOrder[Slice[Byte]],
                                     functionStore: FunctionStore): IO[BlockingCore[IO]] =
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

private[swaydb] case class BlockingCore[W[_]](zero: LevelZero)(implicit transform: IOTransformer[W]) extends Core[W] {

  def put(key: Slice[Byte]): W[Level0Meter] =
    transform.toOther(zero.put(key))

  def put(key: Slice[Byte], value: Slice[Byte]): W[Level0Meter] =
    transform.toOther(zero.put(key, value))

  def put(key: Slice[Byte], value: Option[Slice[Byte]]): W[Level0Meter] =
    transform.toOther(zero.put(key, value))

  def put(key: Slice[Byte], value: Option[Slice[Byte]], removeAt: Deadline): W[Level0Meter] =
    transform.toOther(zero.put(key, value, removeAt))

  /**
    * Each [[Prepare]] requires a new next [[Time]] for cases where a batch contains overriding keys.
    *
    * Same time indicates that the later Prepare in this batch with the same time as newer Prepare has already applied
    * to the newer prepare therefore ignoring the newer prepare.
    *
    * NOTE: If the default time order [[TimeOrder.long]] is used
    * Times should always be unique and in incremental order for *ALL* key values.
    */
  def put(entries: Iterable[Prepare[Slice[Byte], Option[Slice[Byte]]]]): W[Level0Meter] =
    if (entries.isEmpty)
      transform.toOther(IO.Failure(new Exception("Cannot write empty batch")))
    else
      transform.toOther(zero.put(BlockingCore.prepareToMapEntry(entries)(_).get)) //Gah .get! hmm.

  def remove(key: Slice[Byte]): W[Level0Meter] =
    transform.toOther(zero.remove(key))

  def remove(key: Slice[Byte], at: Deadline): W[Level0Meter] =
    transform.toOther(zero.remove(key, at))

  def remove(from: Slice[Byte], to: Slice[Byte]): W[Level0Meter] =
    transform.toOther(zero.remove(from, to))

  def remove(from: Slice[Byte], to: Slice[Byte], at: Deadline): W[Level0Meter] =
    transform.toOther(zero.remove(from, to, at))

  def update(key: Slice[Byte], value: Slice[Byte]): W[Level0Meter] =
    transform.toOther(zero.update(key, value))

  def update(key: Slice[Byte], value: Option[Slice[Byte]]): W[Level0Meter] =
    transform.toOther(zero.update(key, value))

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Slice[Byte]): W[Level0Meter] =
    transform.toOther(zero.update(fromKey, to, value))

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Option[Slice[Byte]]): W[Level0Meter] =
    transform.toOther(zero.update(fromKey, to, value))

  override def clear(): W[Level0Meter] =
    transform.toOther(zero.clear().safeGetBlocking)

  def function(key: Slice[Byte], function: Slice[Byte]): W[Level0Meter] =
    transform.toOther(zero.applyFunction(key, function))

  def function(from: Slice[Byte], to: Slice[Byte], function: Slice[Byte]): W[Level0Meter] =
    transform.toOther(zero.applyFunction(from, to, function))

  def registerFunction(functionID: Slice[Byte], function: SwayFunction): SwayFunction =
    zero.registerFunction(functionID, function)

  private def headIO: IO[Option[KeyValueTuple]] =
    zero.head.safeGetBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafeIfFileExists(response.getOrFetchValue.get).safeGetBlockingIfFileExists map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: Error.Busy =>
                    headIO

                  case failure =>
                    IO.Failure(failure)
                }
            }
        } getOrElse IO.none
    }

  def head: W[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    transform.toOther(headIO)

  def headKey: W[Option[Slice[Byte]]] =
    transform.toOther(zero.headKey.safeGetBlocking)

  private def lastIO: IO[Option[KeyValueTuple]] =
    zero.last.safeGetBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafeIfFileExists(response.getOrFetchValue.get).safeGetBlockingIfFileExists map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: Error.Busy =>
                    lastIO

                  case failure =>
                    IO.Failure(failure)
                }
            }
        } getOrElse IO.none
    }

  def last: W[Option[KeyValueTuple]] =
    transform.toOther(lastIO)

  def lastKey: W[Option[Slice[Byte]]] =
    transform.toOther(zero.lastKey.safeGetBlocking)

  def bloomFilterKeyValueCount: W[Int] =
    transform.toOther(IO.Async.runSafe(zero.bloomFilterKeyValueCount.get).safeGetBlocking)

  def deadline(key: Slice[Byte]): W[Option[Deadline]] =
    transform.toOther(zero.deadline(key).safeGetBlocking)

  def sizeOfSegments: Long =
    zero.sizeOfSegments

  def contains(key: Slice[Byte]): W[Boolean] =
    transform.toOther(zero.contains(key).safeGetBlocking)

  def mightContain(key: Slice[Byte]): W[Boolean] =
    transform.toOther(IO.Async.runSafe(zero.mightContain(key).get).safeGetBlocking)

  private def getIO(key: Slice[Byte]): IO[Option[Option[Slice[Byte]]]] =
    zero.get(key).safeGetBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafeIfFileExists(response.getOrFetchValue.get).safeGetBlockingIfFileExists map {
              result =>
                Some(result)
            } recoverWith {
              case error =>
                error match {
                  case _: Error.Busy =>
                    getIO(key)

                  case failure =>
                    IO.Failure(failure)
                }
            }
        } getOrElse IO.none
    }

  def get(key: Slice[Byte]): W[Option[Option[Slice[Byte]]]] =
    transform.toOther(getIO(key))

  def getKey(key: Slice[Byte]): W[Option[Slice[Byte]]] =
    transform.toOther(zero.getKey(key).safeGetBlocking)

  private def getKeyValueIO(key: Slice[Byte]): IO[Option[KeyValueTuple]] =
    zero.get(key).safeGetBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafeIfFileExists(response.getOrFetchValue.get).safeGetBlockingIfFileExists map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: Error.Busy =>
                    getKeyValueIO(key)

                  case failure =>
                    IO.Failure(failure)
                }
            }
        } getOrElse IO.none
    }

  def getKeyValue(key: Slice[Byte]): W[Option[KeyValueTuple]] =
    transform.toOther(getKeyValueIO(key))

  private def beforeIO(key: Slice[Byte]): IO[Option[KeyValueTuple]] =
    zero.lower(key).safeGetBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafeIfFileExists(response.getOrFetchValue.get).safeGetBlockingIfFileExists map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: Error.Busy =>
                    beforeIO(key)

                  case failure =>
                    IO.Failure(failure)
                }
            }
        } getOrElse IO.none
    }

  def before(key: Slice[Byte]): W[Option[KeyValueTuple]] =
    transform.toOther(beforeIO(key))

  def beforeKey(key: Slice[Byte]): W[Option[Slice[Byte]]] =
    transform.toOther(zero.lower(key).safeGetBlocking.map(_.map(_.key)))

  private def afterIO(key: Slice[Byte]): IO[Option[KeyValueTuple]] =
    zero.higher(key).safeGetBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafeIfFileExists(response.getOrFetchValue.get).safeGetBlockingIfFileExists map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: Error.Busy =>
                    afterIO(key)

                  case failure =>
                    IO.Failure(failure)
                }
            }
        } getOrElse IO.none
    }

  def after(key: Slice[Byte]): W[Option[KeyValueTuple]] =
    transform.toOther(afterIO(key))

  def afterKey(key: Slice[Byte]): W[Option[Slice[Byte]]] =
    transform.toOther(zero.higher(key).safeGetBlocking.map(_.map(_.key)))

  def valueSize(key: Slice[Byte]): W[Option[Int]] =
    transform.toOther(zero.valueSize(key).safeGetBlocking)

  def level0Meter: Level0Meter =
    zero.level0Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    zero.levelMeter(levelNumber)

  def close(): W[Unit] =
    transform.toOther(zero.close)

  override def async[T[_]](implicit ec: ExecutionContext, transform: FutureTransformer[T]): Core[T] =
    AsyncCore(zero)

  override def blocking[T[_]](implicit transform: IOTransformer[T]): BlockingCore[T] =
    BlockingCore(zero)
}
