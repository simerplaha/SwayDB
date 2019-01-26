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

package swaydb.core.seek

import swaydb.core.data.KeyValue

private[swaydb] sealed trait CurrentSeek
private[swaydb] sealed trait NextSeek

private[swaydb] object Seek {
  sealed trait Stop extends CurrentSeek with NextSeek
  case object Stop extends Stop

  sealed trait Next extends CurrentSeek with NextSeek
  case object Next extends Next

  object Stash {
    case class Current(current: KeyValue.ReadOnly.SegmentResponse) extends CurrentSeek
    case class Next(next: KeyValue.ReadOnly.Put) extends NextSeek
  }
}
