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
