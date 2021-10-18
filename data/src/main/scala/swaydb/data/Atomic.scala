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

package swaydb.data

sealed trait Atomic {
  def enabled: Boolean
}

/**
 * [[Atomic.On]] ensures that all range operations and [[swaydb.Prepare]] transactions
 * are available for reads atomically.
 *
 * For eg: if you submit a [[swaydb.Prepare]]
 * that updates 10 key-values, those 10 key-values will be visible to reads only after
 * each update is applied.
 */

object Atomic {

  val on: Atomic.On = Atomic.On
  val off: Atomic.Off = Atomic.Off

  sealed trait On extends Atomic
  case object On extends On {
    override val enabled: Boolean = true
  }

  sealed trait Off extends Atomic
  case object Off extends Off {
    override val enabled: Boolean = false
  }
}
