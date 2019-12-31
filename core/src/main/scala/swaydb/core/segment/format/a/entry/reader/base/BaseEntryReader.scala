/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.core.segment.format.a.entry.reader.base

import swaydb.core.segment.format.a.entry.reader.BaseEntryApplier
import swaydb.core.util.NullOps

private[core] trait BaseEntryReader {

  def minID: Int

  def maxID: Int

  def read[T](baseId: Int,
              reader: BaseEntryApplier[T]): T
}

object BaseEntryReader {

  val readers: Array[BaseEntryReader] =
    Array(
      BaseEntryReader1,
      BaseEntryReader2,
      BaseEntryReader3,
      BaseEntryReader4
    ) sortBy (_.minID)

  def findReaderNullable(baseId: Int,
                         mightBeCompressed: Boolean,
                         keyCompressionOnly: Boolean): BaseEntryReader =
    if (mightBeCompressed && !keyCompressionOnly)
      NullOps.find[BaseEntryReader](readers, _.maxID >= baseId)
    else
      BaseEntryReaderUncompressed

  def search[T](baseId: Int,
                mightBeCompressed: Boolean,
                keyCompressionOnly: Boolean,
                parser: BaseEntryApplier[T]): T = {
    val baseEntryReaderNullable: BaseEntryReader =
      findReaderNullable(
        baseId = baseId,
        mightBeCompressed = mightBeCompressed,
        keyCompressionOnly = keyCompressionOnly
      )

    if (baseEntryReaderNullable == null)
      throw swaydb.Exception.InvalidBaseId(baseId)
    else
      baseEntryReaderNullable.read(
        baseId = baseId,
        reader = parser
      )
  }
}