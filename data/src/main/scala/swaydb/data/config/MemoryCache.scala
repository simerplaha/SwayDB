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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.data.config

sealed trait MemoryCache {
  def toOption: Option[MemoryCache.Enabled] =
    this match {
      case MemoryCache.Disable =>
        None
      case cache: MemoryCache.Enabled =>
        Some(cache)
    }
}

object MemoryCache {

  case object Disable extends MemoryCache

  sealed trait Enabled extends MemoryCache {
    def capacity: Int
    def actorQueue: ActorQueue
  }

  sealed trait Block extends Enabled {
    val blockSize: Int
    val capacity: Int
    val actorQueue: ActorQueue
  }

  case class EnableBlockCache(blockSize: Int,
                              capacity: Int,
                              actorQueue: ActorQueue) extends Block

  case class EnableKeyValueCache(capacity: Int,
                                 actorQueue: ActorQueue) extends Enabled

  case class EnableBoth(blockSize: Int,
                        capacity: Int,
                        actorQueue: ActorQueue) extends Block
}
