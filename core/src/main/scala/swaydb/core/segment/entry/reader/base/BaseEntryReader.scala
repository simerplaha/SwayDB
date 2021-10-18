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

package swaydb.core.segment.entry.reader.base

import swaydb.core.segment.entry.reader.BaseEntryApplier
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

  def findReaderOrNull(baseId: Int,
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
    val baseEntryReaderOrNull: BaseEntryReader =
      findReaderOrNull(
        baseId = baseId,
        mightBeCompressed = mightBeCompressed,
        keyCompressionOnly = keyCompressionOnly
      )

    if (baseEntryReaderOrNull == null)
      throw swaydb.Exception.InvalidBaseId(baseId)
    else
      baseEntryReaderOrNull.read(
        baseId = baseId,
        reader = parser
      )
  }
}
