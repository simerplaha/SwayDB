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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.data.io

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

sealed trait Busy {
  def status: AtomicBoolean
}
object Busy {

  case class OpeningFile(file: Path, status: AtomicBoolean) extends Busy
  case class DecompressingIndex(status: AtomicBoolean) extends Busy
  case class DecompressionValues(status: AtomicBoolean) extends Busy
  case class ReadingHeader(status: AtomicBoolean) extends Busy
  case class FetchingValue(status: AtomicBoolean) extends Busy

}
