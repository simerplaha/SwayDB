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

import scala.concurrent.duration.Deadline
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import swaydb.Prepare
import swaydb.core.data.KeyValue._
import swaydb.core.data.SwayFunction
import swaydb.core.level.zero.LevelZero
import swaydb.core.util.Delay
import swaydb.data.IO
import swaydb.data.IO.Error
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice

private[swaydb] case class AsyncCore(zero: LevelZero)(implicit ec: ExecutionContext) extends Core[Future] {

  private val blocking = new BlockingCore(zero)

  override def put(key: Slice[Byte]): Future[Level0Meter] =
    blocking.put(key).toFuture

  override def put(key: Slice[Byte], value: Slice[Byte]): Future[Level0Meter] =
    blocking.put(key, value).toFuture

  override def put(key: Slice[Byte], value: Option[Slice[Byte]]): Future[Level0Meter] =
    blocking.put(key, value).toFuture

  override def put(key: Slice[Byte], value: Option[Slice[Byte]], removeAt: Deadline): Future[Level0Meter] =
    blocking.put(key, value, removeAt).toFuture

  override def put(entries: Iterable[Prepare[Slice[Byte], Option[Slice[Byte]]]]): Future[Level0Meter] =
    blocking.put(entries).toFuture

  override def remove(key: Slice[Byte]): Future[Level0Meter] =
    blocking.remove(key).toFuture

  override def remove(key: Slice[Byte], at: Deadline): Future[Level0Meter] =
    blocking.remove(key, at).toFuture

  override def remove(from: Slice[Byte], to: Slice[Byte]): Future[Level0Meter] =
    blocking.remove(from, to).toFuture

  override def remove(from: Slice[Byte], to: Slice[Byte], at: Deadline): Future[Level0Meter] =
    blocking.remove(from, to, at).toFuture

  override def update(key: Slice[Byte], value: Slice[Byte]): Future[Level0Meter] =
    blocking.update(key, value).toFuture

  override def update(key: Slice[Byte], value: Option[Slice[Byte]]): Future[Level0Meter] =
    blocking.update(key, value).toFuture

  override def update(fromKey: Slice[Byte], to: Slice[Byte], value: Slice[Byte]): Future[Level0Meter] =
    blocking.update(fromKey, to, value).toFuture

  override def update(fromKey: Slice[Byte], to: Slice[Byte], value: Option[Slice[Byte]]): Future[Level0Meter] =
    blocking.update(fromKey, to, value).toFuture

  override def clear(): Future[Level0Meter] =
    zero.clear().safeGetFuture

  override def function(key: Slice[Byte], function: Slice[Byte]): Future[Level0Meter] =
    blocking.function(key, function).toFuture

  override def function(from: Slice[Byte], to: Slice[Byte], function: Slice[Byte]): Future[Level0Meter] =
    blocking.function(from, to, function).toFuture

  override def registerFunction(functionID: Slice[Byte], function: SwayFunction): SwayFunction =
    blocking.registerFunction(functionID, function)

  override def sizeOfSegments: Long =
    blocking.sizeOfSegments

  override def level0Meter: Level0Meter =
    blocking.level0Meter

  override def levelMeter(levelNumber: Int): Option[LevelMeter] =
    blocking.levelMeter(levelNumber)

  override def close(): Future[Unit] =
    blocking.close().toFuture

  def head: Future[Option[KeyValueTuple]] =
    zero.head.safeGetFuture flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafeIfFileExists(response.getOrFetchValue.get).safeGetFutureIfFileExists map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: Error.Busy =>
                    head

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def headKey: Future[Option[Slice[Byte]]] =
    zero.headKey.safeGetFuture

  def last: Future[Option[KeyValueTuple]] =
    zero.last.safeGetFuture flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafeIfFileExists(response.getOrFetchValue.get).safeGetFutureIfFileExists map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: Error.Busy =>
                    last

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def lastKey: Future[Option[Slice[Byte]]] =
    zero.lastKey.safeGetFuture

  def bloomFilterKeyValueCount: Future[Int] =
    IO.Async.runSafe(zero.bloomFilterKeyValueCount.get).safeGetFuture

  def deadline(key: Slice[Byte]): Future[Option[Deadline]] =
    zero.deadline(key).safeGetFuture

  def contains(key: Slice[Byte]): Future[Boolean] =
    zero.contains(key).safeGetFuture

  def mightContain(key: Slice[Byte]): Future[Boolean] =
    IO.Async.runSafe(zero.mightContain(key).get).safeGetFuture

  def get(key: Slice[Byte]): Future[Option[Option[Slice[Byte]]]] =
    zero.get(key).safeGetFuture flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafeIfFileExists(response.getOrFetchValue.get).safeGetFutureIfFileExists map {
              result =>
                Some(result)
            } recoverWith {
              case error =>
                error match {
                  case _: Error.Busy =>
                    get(key)

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def getKey(key: Slice[Byte]): Future[Option[Slice[Byte]]] =
    zero.getKey(key).safeGetFuture

  def getKeyValue(key: Slice[Byte]): Future[Option[KeyValueTuple]] =
    zero.get(key).safeGetFuture flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafeIfFileExists(response.getOrFetchValue.get).safeGetFutureIfFileExists map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: Error.Busy =>
                    getKeyValue(key)

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def before(key: Slice[Byte]): Future[Option[KeyValueTuple]] =
    zero.lower(key).safeGetFuture flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafeIfFileExists(response.getOrFetchValue.get).safeGetFutureIfFileExists map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: Error.Busy =>
                    before(key)

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def beforeKey(key: Slice[Byte]): Future[Option[Slice[Byte]]] =
    zero.lower(key).safeGetFuture.map(_.map(_.key))

  def after(key: Slice[Byte]): Future[Option[KeyValueTuple]] =
    zero.higher(key).safeGetFuture flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafeIfFileExists(response.getOrFetchValue.get).safeGetFutureIfFileExists map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: Error.Busy =>
                    after(key)

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def afterKey(key: Slice[Byte]): Future[Option[Slice[Byte]]] =
    zero.higher(key).safeGetFuture.map(_.map(_.key))

  def valueSize(key: Slice[Byte]): Future[Option[Int]] =
    zero.valueSize(key).safeGetFuture

  override def async()(implicit ec: ExecutionContext): Core[Future] =
    this

  override def sync()(implicit ec: ExecutionContext): Core[IO] =
    BlockingCore(zero)
}
