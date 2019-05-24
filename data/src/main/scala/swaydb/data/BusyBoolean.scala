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

package swaydb.data

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

private[swaydb] class BusyBoolean(@volatile private var busy: Boolean,
                                  private[data] val promises: ListBuffer[Promise[Unit]]) {
  def savePromise(promise: Promise[Unit]): Unit =
    promises += promise

  def isBusy =
    busy
}

private[swaydb] object BusyBoolean {
  private val blockingTimeout = 5.seconds.toMillis
  private val futureUnit = Future.successful(())

  val notBusy = BusyBoolean(false)

  def apply(busy: Boolean): BusyBoolean =
    new BusyBoolean(busy, ListBuffer.empty)

  def blockUntilFree(boolean: BusyBoolean): Unit =
    boolean.synchronized {
      while (boolean.busy) boolean.wait(blockingTimeout)
    }

  private def notifyBlocking(boolean: BusyBoolean): Unit = {
    boolean.notifyAll()
    boolean.promises.foreach(_.trySuccess(()))
  }

  def future(boolean: BusyBoolean): Future[Unit] =
    boolean.synchronized {
      if (boolean.isBusy) {
        val promise = Promise[Unit]
        boolean.savePromise(promise)
        promise.future
      } else {
        futureUnit
      }
    }

  def setBusy(boolean: BusyBoolean): Boolean =
    boolean.synchronized {
      if (!boolean.busy) {
        boolean.busy = true
        true
      } else {
        false
      }
    }

  def setFree(boolean: BusyBoolean): Unit =
    boolean.synchronized {
      boolean.busy = false
      notifyBlocking(boolean)
    }
}

