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

package swaydb.data.slice

import swaydb.IO

abstract class Reader[E >: swaydb.Error.IO : IO.ExceptionHandler] extends ReaderBase[E] {

  def moveTo(position: Long): Reader[E]

  override def copy(): Reader[E]

  override def skip(skip: Long): Reader[E] =
    moveTo(getPosition + skip)

  override def reset(): Reader[E] =
    this moveTo 0
}