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

package swaydb.core.util

import swaydb.core.data.KeyValue
import swaydb.core.io.reader.Reader
import swaydb.data.slice.{Slice, SliceReader}

private[core] object SliceUtil {

  implicit class ByteArrayImplicits(slice: Slice[Byte]) {
    def createReader(): SliceReader =
      Reader(slice)
  }

  implicit class SliceKeyValueImplicits(slice: Slice[KeyValue]) {
    def persistentSegmentSize: Int =
      slice.lastOption.map(_.stats.segmentSize).getOrElse(0)

    def memorySegmentSize: Int =
      slice.lastOption.map(_.stats.memorySegmentSize).getOrElse(0)


    def persistentSegmentSizeWithoutFooter: Int =
      slice.lastOption.map(_.stats.segmentSizeWithoutFooter).getOrElse(0)
  }

}
