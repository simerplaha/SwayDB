/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.level.zero

import swaydb.core.CoreAPI
import swaydb.core.data.KeyValue.KeyValueTuple
import swaydb.data.slice.Slice

import scala.util.Try

private[core] trait LevelZeroRef extends CoreAPI {

  def releaseLocks: Try[Unit]

  def lower(key: Slice[Byte]): Try[Option[KeyValueTuple]]

  def higher(key: Slice[Byte]): Try[Option[KeyValueTuple]]
}
