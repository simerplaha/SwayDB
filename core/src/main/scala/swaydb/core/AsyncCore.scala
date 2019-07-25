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
import swaydb.core.data.SwayFunction
import swaydb.core.level.zero.LevelZero
import swaydb.core.util.Delay
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice
import swaydb.{IO, Prepare, Tag}

import scala.concurrent.duration.Deadline
import scala.concurrent.{ExecutionContext, Future}

private[swaydb] case class AsyncCore[T[_]](zero: LevelZero, onClose: () => IO[swaydb.Error.Close, Unit])(implicit ec: ExecutionContext,
                                                                                                         tag: Tag.Async[T]) extends Core[T] {
  private val block = BlockingCore[Tag.API](zero, onClose)(Tag.sio)

  override def put(key: Slice[Byte]): T[IO.Done] =
    tag.fromIO(block.put(key))

  override def put(key: Slice[Byte], value: Slice[Byte]): T[IO.Done] =
    tag.fromIO(block.put(key, value))

  override def put(key: Slice[Byte], value: Option[Slice[Byte]]): T[IO.Done] =
    tag.fromIO(block.put(key, value))

  override def put(key: Slice[Byte], value: Option[Slice[Byte]], removeAt: Deadline): T[IO.Done] =
    tag.fromIO(block.put(key, value, removeAt))

  override def put(entries: Iterable[Prepare[Slice[Byte], Option[Slice[Byte]]]]): T[IO.Done] =
    tag.fromIO(block.put(entries))

  override def remove(key: Slice[Byte]): T[IO.Done] =
    tag.fromIO(block.remove(key))

  override def remove(key: Slice[Byte], at: Deadline): T[IO.Done] =
    tag.fromIO(block.remove(key, at))

  override def remove(from: Slice[Byte], to: Slice[Byte]): T[IO.Done] =
    tag.fromIO(block.remove(from, to))

  override def remove(from: Slice[Byte], to: Slice[Byte], at: Deadline): T[IO.Done] =
    tag.fromIO(block.remove(from, to, at))

  override def update(key: Slice[Byte], value: Slice[Byte]): T[IO.Done] =
    tag.fromIO(block.update(key, value))

  override def update(key: Slice[Byte], value: Option[Slice[Byte]]): T[IO.Done] =
    tag.fromIO(block.update(key, value))

  override def update(fromKey: Slice[Byte], to: Slice[Byte], value: Slice[Byte]): T[IO.Done] =
    tag.fromIO(block.update(fromKey, to, value))

  override def update(fromKey: Slice[Byte], to: Slice[Byte], value: Option[Slice[Byte]]): T[IO.Done] =
    tag.fromIO(block.update(fromKey, to, value))

  override def clear(): T[IO.Done] =
    tag.fromFuture(zero.clear().runInFuture)

  override def function(key: Slice[Byte], function: Slice[Byte]): T[IO.Done] =
    tag.fromIO(block.function(key, function))

  override def function(from: Slice[Byte], to: Slice[Byte], function: Slice[Byte]): T[IO.Done] =
    tag.fromIO(block.function(from, to, function))

  override def registerFunction(functionID: Slice[Byte], function: SwayFunction): SwayFunction =
    block.registerFunction(functionID, function)

  override def sizeOfSegments: Long =
    block.sizeOfSegments

  override def level0Meter: LevelZeroMeter =
    block.level0Meter

  override def levelMeter(levelNumber: Int): Option[LevelMeter] =
    block.levelMeter(levelNumber)

  override def close(): T[Unit] =
    tag.fromIO(block.close())

  override def delete(): T[Unit] =
    tag.fromIO(block.delete())

  private def headFuture: Future[Option[KeyValueTuple]] =
    zero.head.runInFuture flatMap {
      result =>
        result map {
          response =>
            IO.Defer.recover(response.getOrFetchValue.get).runInFutureIfFileExists map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: swaydb.Error.ReservedIO =>
                    headFuture

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def head: T[Option[KeyValueTuple]] =
    tag.fromFuture(headFuture)

  def headKey: T[Option[Slice[Byte]]] =
    tag.fromFuture(zero.headKey.runInFuture)

  private def lastFuture: Future[Option[KeyValueTuple]] =
    zero.last.runInFuture flatMap {
      result =>
        result map {
          response =>
            IO.Defer.recover(response.getOrFetchValue.get).runInFutureIfFileExists map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: swaydb.Error.ReservedIO =>
                    lastFuture

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def last: T[Option[KeyValueTuple]] =
    tag.fromFuture(lastFuture)

  def lastKey: T[Option[Slice[Byte]]] =
    tag.fromFuture(zero.lastKey.runInFuture)

  def bloomFilterKeyValueCount: T[Int] =
    tag.fromFuture(IO.Defer.recover(zero.bloomFilterKeyValueCount.get).runInFuture)

  def deadline(key: Slice[Byte]): T[Option[Deadline]] =
    tag.fromFuture(zero.deadline(key).runInFuture)

  def contains(key: Slice[Byte]): T[Boolean] =
    tag.fromFuture(zero.contains(key).runInFuture)

  def mightContainKey(key: Slice[Byte]): T[Boolean] =
    tag.fromFuture(IO.Defer.recover(zero.mightContainKey(key).get).runInFuture)

  def mightContainFunction(functionId: Slice[Byte]): T[Boolean] =
    tag.fromFuture(IO.Defer.recover(zero.mightContainFunction(functionId).get).runInFuture)

  def getFuture(key: Slice[Byte]): Future[Option[Option[Slice[Byte]]]] =
    zero.get(key).runInFuture flatMap {
      result =>
        result map {
          response =>
            IO.Defer.recover(response.getOrFetchValue.get).runInFutureIfFileExists map {
              result =>
                Some(result)
            } recoverWith {
              case error =>
                error match {
                  case _: swaydb.Error.ReservedIO =>
                    getFuture(key)

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def get(key: Slice[Byte]): T[Option[Option[Slice[Byte]]]] =
    tag.fromFuture(getFuture(key))

  def getKey(key: Slice[Byte]): T[Option[Slice[Byte]]] =
    tag.fromFuture(zero.getKey(key).runInFuture)

  def getKeyValueFuture(key: Slice[Byte]): Future[Option[KeyValueTuple]] =
    zero.get(key).runInFuture flatMap {
      result =>
        result map {
          response =>
            IO.Defer.recover(response.getOrFetchValue.get).runInFutureIfFileExists map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: swaydb.Error.ReservedIO =>
                    getKeyValueFuture(key)

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def getKeyValue(key: Slice[Byte]): T[Option[KeyValueTuple]] =
    tag.fromFuture(getKeyValueFuture(key))

  def beforeFuture(key: Slice[Byte]): Future[Option[KeyValueTuple]] =
    zero.lower(key).runInFuture flatMap {
      result =>
        result map {
          response =>
            IO.Defer.recover(response.getOrFetchValue.get).runInFutureIfFileExists map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: swaydb.Error.ReservedIO =>
                    beforeFuture(key)

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def before(key: Slice[Byte]): T[Option[KeyValueTuple]] =
    tag.fromFuture(beforeFuture(key))

  def beforeKey(key: Slice[Byte]): T[Option[Slice[Byte]]] =
    tag.fromFuture(zero.lower(key).runInFuture.map(_.map(_.key)))

  private def afterFuture(key: Slice[Byte]): Future[Option[KeyValueTuple]] =
    zero.higher(key).runInFuture flatMap {
      result =>
        result map {
          response =>
            IO.Defer.recover(response.getOrFetchValue.get).runInFutureIfFileExists map {
              result =>
                Some(response.key, result)
            } recoverWith {
              case error =>
                error match {
                  case _: swaydb.Error.ReservedIO =>
                    afterFuture(key)

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def after(key: Slice[Byte]): T[Option[KeyValueTuple]] =
    tag.fromFuture(afterFuture(key))

  def afterKey(key: Slice[Byte]): T[Option[Slice[Byte]]] =
    tag.fromFuture(zero.higher(key).runInFuture.map(_.map(_.key)))

  def valueSize(key: Slice[Byte]): T[Option[Int]] =
    tag.fromFuture(zero.valueSize(key).runInFuture)

  override def tagAsync[T[_]](implicit ec: ExecutionContext, tag: Tag.Async[T]): Core[T] =
    copy(zero)

  override def tagBlocking[T[_]](implicit tag: Tag[T]): BlockingCore[T] =
    BlockingCore(zero, onClose)
}
