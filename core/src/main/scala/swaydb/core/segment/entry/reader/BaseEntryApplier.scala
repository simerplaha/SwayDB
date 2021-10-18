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

package swaydb.core.segment.entry.reader

import swaydb.core.segment.entry.id._

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
