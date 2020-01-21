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
 */

package swaydb.core.util

import org.scalatest.{FlatSpec, Matchers}
import scala.collection.parallel.CollectionConverters._

class IDGeneratorSpec extends FlatSpec with Matchers {

  it should "always return new incremental ids when access concurrently" in {

    val gen = IDGenerator()

    (1 to 100).par.foldLeft(-1L) {
      case (previous, _) =>
        //check nextSegmentId should return valid text
        gen.nextSegmentID should fullyMatch regex s"\\d+\\.seg"
        val next = gen.nextID
        next should be > previous
        next
    }
  }

  it should "return segment string" in {
    IDGenerator.segmentId(1) should fullyMatch regex "1.seg"
  }
}
