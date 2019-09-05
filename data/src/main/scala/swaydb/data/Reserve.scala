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
import scala.concurrent.Promise

class Reserve[T](@volatile var info: Option[T],
                 private[data] val promises: ListBuffer[Promise[Unit]],
                 val name: String) {
  def savePromise(promise: Promise[Unit]): Unit =
    promises += promise

  def isBusy: Boolean =
    info.isDefined

  def isFree: Boolean =
    !isBusy
}

object Reserve {

  def free[T](name: String): Reserve[T] =
    new Reserve(None, ListBuffer.empty, name)

  def busy[T](info: T, name: String): Reserve[T] =
    new Reserve(Some(info), ListBuffer.empty, name)

  def blockUntilFree[T](reserve: Reserve[T]): Unit =
    reserve.synchronized {
      while (reserve.isBusy) reserve.wait()
    }

  private def notifyBlocking[T](reserve: Reserve[T]): Unit = {
    reserve.notifyAll()
    reserve.promises.foreach(_.trySuccess(()))
  }

  def promise[T](reserve: Reserve[T]): Promise[Unit] =
    reserve.synchronized {
      if (reserve.isBusy) {
        val promise = Promise[Unit]
        reserve.savePromise(promise)
        promise
      } else {
        Promise.successful(())
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
