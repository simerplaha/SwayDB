/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.effect

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Promise
import scala.concurrent.duration.Deadline

class Reserve[T](private val info: AtomicReference[Option[T]],
                 private val promises: ConcurrentLinkedQueue[Promise[Unit]],
                 val name: String) {

  @inline private def savePromise(promise: Promise[Unit]): Unit =
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

  def blockUntilFree[T](reserve: Reserve[T], deadline: Deadline): Unit =
    reserve.synchronized {
      while (reserve.isBusy && deadline.hasTimeLeft())
        reserve.wait(deadline.timeLeft.toMillis)
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
