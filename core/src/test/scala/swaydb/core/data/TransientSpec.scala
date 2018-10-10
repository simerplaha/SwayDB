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

package swaydb.core.data

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.CommonAssertions
import swaydb.serializers.Default._
import swaydb.serializers._

class TransientSpec extends WordSpec with Matchers with CommonAssertions {

  val keyValueCount = 100

  "Transient" should {
    "be iterable" in {
      val one = Transient.Remove(1)
      val two = Transient.Remove(2, 0.1, Some(one))
      val three = Transient.Put(key = 3, value = Some(3), falsePositiveRate = 0.1, previousMayBe = Some(two))
      val four = Transient.Remove(4, 0.1, Some(three))
      val five = Transient.Put(key = 5, value = Some(5), falsePositiveRate = 0.1, previousMayBe = Some(four))

      five.reverseIterator.toList should contain inOrderOnly(five, four, three, two, one)
    }
  }
}