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

package swaydb.config

import swaydb.{ActorConfig, Bagged}

sealed trait FileCache extends Bagged[FileCache.On, Option]

object FileCache {

  def off: FileCache.Off = FileCache.Off

  def on(maxOpen: Int,
         actorConfig: ActorConfig): FileCache.On =
    FileCache.On(
      maxOpen = maxOpen,
      actorConfig = actorConfig
    )

  sealed trait Off extends FileCache
  case object Off extends Off {
    override def get: Option[On] = None
  }

  case class On(maxOpen: Int,
                actorConfig: ActorConfig) extends FileCache {
    override def get: Option[On] = Some(this)

    def copyWithMaxOpen(maxOpen: Int): On =
      this.copy(maxOpen = maxOpen)

    def copyWithActorConfig(actorConfig: ActorConfig): On =
      this.copy(actorConfig = actorConfig)
  }
}
