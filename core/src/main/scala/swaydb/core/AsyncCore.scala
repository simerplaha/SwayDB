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
import swaydb.Prepare
import swaydb.core.data.KeyValue._
import swaydb.core.data.SwayFunction
import swaydb.core.level.zero.LevelZero
import swaydb.core.util.Delay
import swaydb.data.IO
import swaydb.data.IO.Error
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.io.{FutureTransformer, IOTransformer}
import swaydb.data.slice.Slice

private[swaydb] case class AsyncCore[W[_]](zero: LevelZero)(implicit ec: ExecutionContext,
                                                            transform: FutureTransformer[W]) extends Core[W] {

  private val block = BlockingCore[IO](zero)(IOTransformer.IOToIOTransformer)

  override def put(key: Slice[Byte]): W[Level0Meter] =
    transform.toOther(block.put(key).toFuture)

  override def put(key: Slice[Byte], value: Slice[Byte]): W[Level0Meter] =
    transform.toOther(block.put(key, value).toFuture)

  override def put(key: Slice[Byte], value: Option[Slice[Byte]]): W[Level0Meter] =
    transform.toOther(block.put(key, value).toFuture)

  override def put(key: Slice[Byte], value: Option[Slice[Byte]], removeAt: Deadline): W[Level0Meter] =
    transform.toOther(block.put(key, value, removeAt).toFuture)

  override def put(entries: Iterable[Prepare[Slice[Byte], Option[Slice[Byte]]]]): W[Level0Meter] =
    transform.toOther(block.put(entries).toFuture)

  override def remove(key: Slice[Byte]): W[Level0Meter] =
    transform.toOther(block.remove(key).toFuture)

  override def remove(key: Slice[Byte], at: Deadline): W[Level0Meter] =
    transform.toOther(block.remove(key, at).toFuture)

  override def remove(from: Slice[Byte], to: Slice[Byte]): W[Level0Meter] =
    transform.toOther(block.remove(from, to).toFuture)

  override def remove(from: Slice[Byte], to: Slice[Byte], at: Deadline): W[Level0Meter] =
    transform.toOther(block.remove(from, to, at).toFuture)

  override def update(key: Slice[Byte], value: Slice[Byte]): W[Level0Meter] =
    transform.toOther(block.update(key, value).toFuture)

  override def update(key: Slice[Byte], value: Option[Slice[Byte]]): W[Level0Meter] =
    transform.toOther(block.update(key, value).toFuture)

  override def update(fromKey: Slice[Byte], to: Slice[Byte], value: Slice[Byte]): W[Level0Meter] =
    transform.toOther(block.update(fromKey, to, value).toFuture)

  override def update(fromKey: Slice[Byte], to: Slice[Byte], value: Option[Slice[Byte]]): W[Level0Meter] =
    transform.toOther(block.update(fromKey, to, value).toFuture)

  override def clear(): W[Level0Meter] =
    transform.toOther(zero.clear().safeGetFuture)

  override def function(key: Slice[Byte], function: Slice[Byte]): W[Level0Meter] =
    transform.toOther(block.function(key, function).toFuture)

  override def function(from: Slice[Byte], to: Slice[Byte], function: Slice[Byte]): W[Level0Meter] =
    transform.toOther(block.function(from, to, function).toFuture)

  override def registerFunction(functionID: Slice[Byte], function: SwayFunction): SwayFunction =
    block.registerFunction(functionID, function)

  override def sizeOfSegments: Long =
    block.sizeOfSegments

  override def level0Meter: Level0Meter =
    block.level0Meter

  override def levelMeter(levelNumber: Int): Option[LevelMeter] =
    block.levelMeter(levelNumber)

  override def close(): W[Unit] =
    transform.toOther(block.close().toFuture)

  private def headFuture: Future[Option[KeyValueTuple]] =
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
                    headFuture

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def head: W[Option[KeyValueTuple]] =
    transform.toOther(headFuture)

  def headKey: W[Option[Slice[Byte]]] =
    transform.toOther(zero.headKey.safeGetFuture)

  private def lastFuture: Future[Option[KeyValueTuple]] =
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
                    lastFuture

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def last: W[Option[KeyValueTuple]] =
    transform.toOther(lastFuture)

  def lastKey: W[Option[Slice[Byte]]] =
    transform.toOther(zero.lastKey.safeGetFuture)

  def bloomFilterKeyValueCount: W[Int] =
    transform.toOther(IO.Async.runSafe(zero.bloomFilterKeyValueCount.get).safeGetFuture)

  def deadline(key: Slice[Byte]): W[Option[Deadline]] =
    transform.toOther(zero.deadline(key).safeGetFuture)

  def contains(key: Slice[Byte]): W[Boolean] =
    transform.toOther(zero.contains(key).safeGetFuture)

  def mightContain(key: Slice[Byte]): W[Boolean] =
    transform.toOther(IO.Async.runSafe(zero.mightContain(key).get).safeGetFuture)

  def getFuture(key: Slice[Byte]): Future[Option[Option[Slice[Byte]]]] =
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
                    getFuture(key)

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def get(key: Slice[Byte]): W[Option[Option[Slice[Byte]]]] =
    transform.toOther(getFuture(key))

  def getKey(key: Slice[Byte]): W[Option[Slice[Byte]]] =
    transform.toOther(zero.getKey(key).safeGetFuture)

  def getKeyValueFuture(key: Slice[Byte]): Future[Option[KeyValueTuple]] =
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
                    getKeyValueFuture(key)

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def getKeyValue(key: Slice[Byte]): W[Option[KeyValueTuple]] =
    transform.toOther(getKeyValueFuture(key))

  def beforeFuture(key: Slice[Byte]): Future[Option[KeyValueTuple]] =
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
                    beforeFuture(key)

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def before(key: Slice[Byte]): W[Option[KeyValueTuple]] =
    transform.toOther(beforeFuture(key))

  def beforeKey(key: Slice[Byte]): W[Option[Slice[Byte]]] =
    transform.toOther(zero.lower(key).safeGetFuture.map(_.map(_.key)))

  def afterFuture(key: Slice[Byte]): Future[Option[KeyValueTuple]] =
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
                    afterFuture(key)

                  case failure =>
                    Future.failed(failure)
                }
            }
        } getOrElse Delay.futureNone
    }

  def after(key: Slice[Byte]): W[Option[KeyValueTuple]] =
    transform.toOther(afterFuture(key))

  def afterKey(key: Slice[Byte]): W[Option[Slice[Byte]]] =
    transform.toOther(zero.higher(key).safeGetFuture.map(_.map(_.key)))

  def valueSize(key: Slice[Byte]): W[Option[Int]] =
    transform.toOther(zero.valueSize(key).safeGetFuture)

  override def async[T[_]](implicit ec: ExecutionContext, transform: FutureTransformer[T]): Core[T] =
    copy(zero)

  override def blocking[T[_]](implicit transform: IOTransformer[T]): BlockingCore[T] =
    BlockingCore(zero)
}
