/*
 * Copyright (c) 19/12/21, 7:31 pm Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package swaydb.core.log.timer

import swaydb.core.log.counter.MemoryCounterLog
import swaydb.core.segment.data.Time

object MemoryTimer {

  @inline def apply(): MemoryTimer =
    new MemoryTimer()

}

class MemoryTimer private(memory: MemoryCounterLog = MemoryCounterLog()) extends Timer {

  override val isEmptyTimer: Boolean =
    false

  override def next: Time =
    Time(memory.next)

  override def close(): Unit =
    memory.close()
}