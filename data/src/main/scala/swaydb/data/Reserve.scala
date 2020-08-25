/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.data

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.Promise

class Reserve[T](val info: AtomicReference[Option[T]],
                 private[data] val promises: ConcurrentLinkedQueue[Promise[Unit]],
                 val name: String) {
  def savePromise(promise: Promise[Unit]): Unit =
    promises add promise

  def isBusy: Boolean =
    info.get().isDefined

  def isFree: Boolean =
    !isBusy
}

object Reserve {

  def free[T](name: String): Reserve[T] =
    new Reserve(new AtomicReference[Option[T]](None), new ConcurrentLinkedQueue[Promise[Unit]](), name)

  def busy[T](info: T, name: String): Reserve[T] =
    new Reserve(new AtomicReference[Option[T]](Some(info)), new ConcurrentLinkedQueue[Promise[Unit]](), name)

  def blockUntilFree[T](reserve: Reserve[T]): Unit =
    reserve.synchronized {
      while (reserve.isBusy)
        reserve.wait()
    }

  private def notifyBlocking[T](reserve: Reserve[T]): Unit =
    reserve.synchronized {
      reserve.notifyAll()

      var continue = true
      while (continue) {
        val next = reserve.promises.poll()
        if (next == null)
          continue = false
        else
          next.trySuccess(())
      }
    }

  def promise[T](reserve: Reserve[T]): Promise[Unit] =
    reserve.synchronized {
      if (reserve.info.get().isDefined) {
        val promise = Promise[Unit]()
        reserve.savePromise(promise)
        promise
      } else {
        Promise.successful(())
      }
    }

  def compareAndSet[T](info: Some[T], reserve: Reserve[T]): Boolean =
    reserve.info.compareAndSet(None, info)

  def setFree[T](reserve: Reserve[T]): Unit = {
    reserve.info.set(None)
    notifyBlocking(reserve)
  }
}
