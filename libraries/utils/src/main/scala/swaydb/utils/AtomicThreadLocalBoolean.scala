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

package swaydb.utils

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

private[swaydb] object AtomicThreadLocalBoolean {
  def apply(): AtomicThreadLocalBoolean =
    new AtomicThreadLocalBoolean()
}

private[swaydb] class AtomicThreadLocalBoolean {

  private val bool = new AtomicBoolean()
  private val threadLocal = new ThreadLocal[UUID]()

  @volatile private var currentThread: UUID = UUID.randomUUID()

  def compareAndSet(expect: Boolean, update: Boolean): Boolean =
    if (bool.compareAndSet(expect, update)) {
      val uuid = UUID.randomUUID()
      currentThread = uuid
      threadLocal.set(uuid)
      true
    } else {
      false
    }

  def setFree(): Unit = {
    threadLocal.remove()
    bool.set(false)
  }

  def isExecutingThread(): Boolean =
    currentThread == threadLocal.get()
}
