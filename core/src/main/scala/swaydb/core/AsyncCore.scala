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

import swaydb.Tag.Implicits._
import swaydb.core.data.KeyValue._
import swaydb.core.data.SwayFunction
import swaydb.core.level.zero.LevelZero
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice
import swaydb.{IO, Prepare, Tag}

import scala.concurrent.duration.Deadline
import scala.concurrent.{ExecutionContext, Future}

private[swaydb] case class AsyncCore[T[_]](zero: LevelZero, onClose: () => IO[swaydb.Error.Close, Unit])(implicit tag: Tag.Async[T]) extends Core[T] {

  /**
   * All reads are Async and use [[Tag.Async]] for execution.
   *
   * But writes currently occur synchronously in the execution thread which are converted
   * to the async type [[T]] in the current thread.
   *
   * For example: If [[T]] was Future, the result would get converted to Future in current thread as:
   *
   * {{{
   *   Future.successful(result) //for success
   *   Future.failure(error) //for failure
   * }}}
   *
   * Eventually async writes will also be supported but for now to apply back-pressure use [[LevelZero.levelZeroMeter]]
   * to check how the writes are progressing and to control the write throughput from the source.
   *
   * See documentation for [[LevelZero.levelZeroMeter]] at [[http://www.swaydb.io/api/read/level0Meter/ level0Meter]].
   */

  private val block = BlockingCore[IO.ApiIO](zero, onClose)(Tag.dbIO)

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
    zero.clear().runAsync

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
  //    zero.head.runInFuture flatMap {
  //      result =>
  //        result map {
  //          response =>
  //            IO.Deferred.recover(response.getOrFetchValue.get).runInFuture map {
  //              result =>
  //                Some(response.key, result)
  //            } recoverWith {
  //              case error =>
  //                error match {
  //                  case _: swaydb.Error.ReservedIO =>
  //                    headFuture
  //
  //                  case failure =>
  //                    Future.failed(failure)
  //                }
  //            }
  //        } getOrElse Delay.futureNone
  //    }
    ???

  def head: T[Option[KeyValueTuple]] =
    tag.fromFuture(headFuture)

  def headKey: T[Option[Slice[Byte]]] =
    zero.headKey.runAsync

  private def lastFuture: Future[Option[KeyValueTuple]] =
  //    zero.last.runInFuture flatMap {
  //      result =>
  //        result map {
  //          response =>
  //            IO.Deferred.recover(response.getOrFetchValue.get).runInFuture map {
  //              result =>
  //                Some(response.key, result)
  //            } recoverWith {
  //              case error =>
  //                error match {
  //                  case _: swaydb.Error.ReservedIO =>
  //                    lastFuture
  //
  //                  case failure =>
  //                    Future.failed(failure)
  //                }
  //            }
  //        } getOrElse Delay.futureNone
  //    }
    ???

  def last: T[Option[KeyValueTuple]] =
    tag.fromFuture(lastFuture)

  def lastKey: T[Option[Slice[Byte]]] =
    zero.lastKey.runAsync

  def bloomFilterKeyValueCount: T[Int] =
  //    tag.fromFuture(IO.Deferred.recover(zero.bloomFilterKeyValueCount.get).runInFuture)
    ???

  def deadline(key: Slice[Byte]): T[Option[Deadline]] =
    zero.deadline(key).runAsync

  def contains(key: Slice[Byte]): T[Boolean] =
    zero.contains(key).runAsync

  def mightContainKey(key: Slice[Byte]): T[Boolean] =
  //    tag.fromFuture(IO.Deferred.recover(zero.mightContainKey(key).get).runInFuture)
    ???

  def mightContainFunction(functionId: Slice[Byte]): T[Boolean] =
  //    tag.fromFuture(IO.Deferred.recover(zero.mightContainFunction(functionId).get).runInFuture)
    ???

  def getFuture(key: Slice[Byte]): Future[Option[Option[Slice[Byte]]]] =
  //    zero.get(key).runInFuture flatMap {
  //      result =>
  //        result map {
  //          response =>
  //            IO.Deferred.recover(response.getOrFetchValue.get).runInFuture map {
  //              result =>
  //                Some(result)
  //            } recoverWith {
  //              case error =>
  //                error match {
  //                  case _: swaydb.Error.ReservedIO =>
  //                    getFuture(key)
  //
  //                  case failure =>
  //                    Future.failed(failure)
  //                }
  //            }
  //        } getOrElse Delay.futureNone
  //    }
    ???

  def get(key: Slice[Byte]): T[Option[Option[Slice[Byte]]]] =
    tag.fromFuture(getFuture(key))

  def getKey(key: Slice[Byte]): T[Option[Slice[Byte]]] =
    zero.getKey(key).runAsync

  def getKeyValueFuture(key: Slice[Byte]): Future[Option[KeyValueTuple]] =
  //    zero.get(key).runInFuture flatMap {
  //      result =>
  //        result map {
  //          response =>
  //            IO.Deferred.recover(response.getOrFetchValue.get).runInFuture map {
  //              result =>
  //                Some(response.key, result)
  //            } recoverWith {
  //              case error =>
  //                error match {
  //                  case _: swaydb.Error.ReservedIO =>
  //                    getKeyValueFuture(key)
  //
  //                  case failure =>
  //                    Future.failed(failure)
  //                }
  //            }
  //        } getOrElse Delay.futureNone
  //    }
    ???

  def getKeyValue(key: Slice[Byte]): T[Option[KeyValueTuple]] =
    tag.fromFuture(getKeyValueFuture(key))

  def beforeFuture(key: Slice[Byte]): Future[Option[KeyValueTuple]] =
  //    zero.lower(key).runInFuture flatMap {
  //      result =>
  //        result map {
  //          response =>
  //            IO.Deferred.recover(response.getOrFetchValue.get).runInFuture map {
  //              result =>
  //                Some(response.key, result)
  //            } recoverWith {
  //              case error =>
  //                error match {
  //                  case _: swaydb.Error.ReservedIO =>
  //                    beforeFuture(key)
  //
  //                  case failure =>
  //                    Future.failed(failure)
  //                }
  //            }
  //        } getOrElse Delay.futureNone
  //    }
    ???

  def before(key: Slice[Byte]): T[Option[KeyValueTuple]] =
    tag.fromFuture(beforeFuture(key))

  def beforeKey(key: Slice[Byte]): T[Option[Slice[Byte]]] =
    zero.lower(key).runAsync.map(_.map(_.key))

  private def afterFuture(key: Slice[Byte]): Future[Option[KeyValueTuple]] =
  //    zero.higher(key).runInFuture flatMap {
  //      result =>
  //        result map {
  //          response =>
  //            IO.Deferred.recover(response.getOrFetchValue.get).runInFuture map {
  //              result =>
  //                Some(response.key, result)
  //            } recoverWith {
  //              case error =>
  //                error match {
  //                  case _: swaydb.Error.ReservedIO =>
  //                    afterFuture(key)
  //
  //                  case failure =>
  //                    Future.failed(failure)
  //                }
  //            }
  //        } getOrElse Delay.futureNone
  //    }
    ???

  def after(key: Slice[Byte]): T[Option[KeyValueTuple]] =
    tag.fromFuture(afterFuture(key))

  def afterKey(key: Slice[Byte]): T[Option[Slice[Byte]]] =
    zero.higher(key).runAsync.map(_.map(_.key))

  def valueSize(key: Slice[Byte]): T[Option[Int]] =
    zero.valueSize(key).runAsync

  override def tagAsync[T[_]](implicit tag: Tag.Async[T]): Core[T] =
    copy(zero)

  override def tagBlocking[T[_]](implicit tag: Tag.Sync[T]): BlockingCore[T] =
    BlockingCore(zero, onClose)
}
