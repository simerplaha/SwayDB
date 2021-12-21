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

class Reserve[@specialized(Unit) T](info: AtomicReference[Option[T]],
                                    promises: ConcurrentLinkedQueue[Promise[Unit]],
                                    val name: String) { self =>

  @inline private def savePromise(promise: Promise[Unit]): Unit =
    promises add promise

  final def isBusy: Boolean =
    info.get().isDefined

  final def isFree: Boolean =
    !isBusy

  final def blockUntilFree(): Unit =
    self.synchronized {
      while (self.isBusy)
        self.wait()
    }

  final def blockUntilFree(deadline: Deadline): Unit =
    self.synchronized {
      while (self.isBusy && deadline.hasTimeLeft())
        self.wait(deadline.timeLeft.toMillis)
    }

  @inline final private def notifyBlocking(): Unit =
    self.synchronized {
      self.notifyAll()

      var continue = true
      while (continue) {
        val next = self.promises.poll()
        if (next == null)
          continue = false
        else
          next.trySuccess(())
      }
    }

  final def promise(): Promise[Unit] =
    self.synchronized {
      if (self.info.get().isDefined) {
        val promise = Promise[Unit]()
        self.savePromise(promise)
        promise
      } else {
        Promise.successful(())
      }
    }

  final def compareAndSet(info: Some[T]): Boolean =
    self.info.compareAndSet(None, info)

  final def setFree(): Unit = {
    self.info.set(None)
    notifyBlocking()
  }
}

object Reserve {

  def free[@specialized(Unit) T](name: String): Reserve[T] =
    new Reserve(new AtomicReference[Option[T]](None), new ConcurrentLinkedQueue[Promise[Unit]](), name)

  def busy[@specialized(Unit) T](info: T, name: String): Reserve[T] =
    new Reserve(new AtomicReference[Option[T]](Some(info)), new ConcurrentLinkedQueue[Promise[Unit]](), name)

}
