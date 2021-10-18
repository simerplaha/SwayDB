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

package swaydb.data.storage

import swaydb.data.config.{MMAP, RecoveryMode}

import java.nio.file.Path

sealed trait Level0Storage {
  val memory: Boolean

  def persistent = !memory

  def isMMAP: Boolean
}

object Level0Storage {

  case object Memory extends Level0Storage {
    override val memory: Boolean = true
    override def isMMAP: Boolean = false
  }

  case class Persistent(mmap: MMAP.Map,
                        dir: Path,
                        recovery: RecoveryMode) extends Level0Storage {
    override val memory: Boolean = false
    override def isMMAP: Boolean = mmap.isMMAP
  }
}
