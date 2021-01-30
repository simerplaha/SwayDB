/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.utils

import com.typesafe.scalalogging.LazyLogging

object StorageUnits extends LazyLogging {

  implicit class StorageIntImplicits(measure: Int) {

    @inline final def bytes: Int = measure

    @inline final def byte: Int = measure
  }

  private def validate(bytes: Int, unit: String): Int = {
    if (bytes < 0) {
      val exception = new Exception(s"Negative Integer size. $bytes.$unit = $bytes.bytes")
      logger.error(exception.getMessage)
      throw exception
    }

    bytes
  }

  private def validate(bytes: Long, unit: String): Long = {
    if (bytes < 0) {
      val exception = new Exception(s"Negative Long size. $bytes.$unit = $bytes.bytes")
      logger.error(exception.getMessage)
      throw exception
    }

    bytes
  }

  implicit class StorageDoubleImplicits(measure: Double) {

    @inline final def mb: Int =
      validate(bytes = (measure * 1000000).toInt, unit = "mb")

    @inline final def gb: Int =
      validate(bytes = measure.mb * 1000, unit = "gb")

    @inline final def kb: Int =
      validate(bytes = (measure * 1000).toInt, unit = "kb")

    @inline final def mb_long: Long =
      validate(bytes = (measure * 1000000L).toLong, unit = "mb_long")

    @inline final def gb_long: Long =
      validate(bytes = measure.mb_long * 1000L, unit = "gb_long")

    @inline final def kb_long: Long =
      validate(bytes = (measure * 1000L).toLong, unit = "kb_long")
  }
}
