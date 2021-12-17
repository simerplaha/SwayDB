/*
 * Copyright (c) 18/12/21, 4:59 am Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core

import swaydb.macros.MacroSealed
import swaydb.testkit.TestKit.randomBoolean

sealed trait CoreSpecType {
  def isMemory: Boolean

  def isPersistent: Boolean =
    !isMemory
}

object CoreSpecType {

  def random(): CoreSpecType =
    if (randomBoolean())
      Memory
    else
      Persistent

  case object Memory extends CoreSpecType {
    override val isMemory: Boolean = true
  }

  case object Persistent extends CoreSpecType {
    override def isMemory: Boolean = false
  }

  val all: Array[CoreSpecType] =
    MacroSealed.array[CoreSpecType]

  assert(all.length == 2)

}
