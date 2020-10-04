/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.entry.reader

import swaydb.core.segment.format.a.entry.id._

sealed trait BaseEntryApplier[E] {

  def apply[T <: BaseEntryId](baseId: T)(implicit timeReader: TimeReader[T],
                                         deadlineReader: DeadlineReader[T],
                                         valueOffsetReader: ValueOffsetReader[T],
                                         valueLengthReader: ValueLengthReader[T],
                                         valueBytesReader: ValueReader[T]): E
}

object BaseEntryApplier {

  object ReturnFinders extends BaseEntryApplier[(TimeReader[_], DeadlineReader[_], ValueOffsetReader[_], ValueLengthReader[_], ValueReader[_])] {

    override def apply[T <: BaseEntryId](baseId: T)(implicit timeReader: TimeReader[T],
                                                    deadlineReader: DeadlineReader[T],
                                                    valueOffsetReader: ValueOffsetReader[T],
                                                    valueLengthReader: ValueLengthReader[T],
                                                    valueBytesReader: ValueReader[T]): (TimeReader[T], DeadlineReader[T], ValueOffsetReader[T], ValueLengthReader[T], ValueReader[T]) =
      (timeReader, deadlineReader, valueOffsetReader, valueLengthReader, valueBytesReader)
  }

}
