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

package swaydb.data

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

private[swaydb] class Reserve[T](@volatile var info: Option[T],
                                 private[data] val promises: ListBuffer[Promise[Unit]]) {
  def savePromise(promise: Promise[Unit]): Unit =
    promises += promise

  def isBusy: Boolean =
    info.isDefined

  def isFree: Boolean =
    !isBusy
}

private[swaydb] object Reserve {
  private val blockingTimeout = 5.seconds.toMillis
  private val futureUnit = Future.successful(())

  def apply[T](): Reserve[T] =
    new Reserve(None, ListBuffer.empty)

  def apply[T](info: T): Reserve[T] =
    new Reserve(Some(info), ListBuffer.empty)

  def blockUntilFree[T](reserve: Reserve[T]): Unit =
    reserve.synchronized {
      while (reserve.isBusy) reserve.wait(blockingTimeout)
    }

  private def notifyBlocking[T](reserve: Reserve[T]): Unit = {
    reserve.notifyAll()
    reserve.promises.foreach(_.trySuccess(()))
  }

  def future[T](reserve: Reserve[T]): Future[Unit] =
    reserve.synchronized {
      if (reserve.isBusy) {
        val promise = Promise[Unit]
        reserve.savePromise(promise)
        promise.future
      } else {
        futureUnit
      }
    }

  def setBusyOrGet[T](info: T, reserve: Reserve[T]): Option[T] =
    reserve.synchronized {
      if (reserve.isFree) {
        reserve.info = Some(info)
        None
      } else {
        reserve.info
      }
    }

  def setFree[T](reserve: Reserve[T]): Unit =
    reserve.synchronized {
      reserve.info = None
      notifyBlocking(reserve)
    }
}
