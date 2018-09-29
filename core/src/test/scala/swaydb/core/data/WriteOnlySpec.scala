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

class WriteOnlySpec extends WordSpec with Matchers with CommonAssertions {

  val keyValueCount = 100

  "Transient" should {
    "be iterable" in {
      val previousPrevious = Transient.Remove(1)
      val previous = Transient.Remove(2, 0.1, Some(previousPrevious))
      val next = Transient.Remove(3, 0.1, Some(previous))

      next.reverseIterator.toList should contain inOrderOnly(next, previous, previousPrevious)
    }
  }

  "lastGroup on a list of WriteOnly key-values" should {
    "return empty if there are no groups" in {
      Seq.empty[KeyValue.WriteOnly].lastGroup() shouldBe empty
    }

    "return last group if there is only one group" in {
      val group =
        Transient.Group(
          keyValues = randomizedIntKeyValues(keyValueCount),
          indexCompression = randomCompression(),
          valueCompression = randomCompression(),
          falsePositiveRate = 0.1,
          previous = None
        ).assertGet

      Seq(group).lastGroup().assertGet shouldBe group
    }

    "return last group if there are multiple groups" in {
      val groups =
        (1 to 10) map {
          _ =>
            Transient.Group(
              keyValues = randomizedIntKeyValues(keyValueCount),
              indexCompression = randomCompression(),
              valueCompression = randomCompression(),
              falsePositiveRate = 0.1,
              previous = None
            ).assertGet
        }

      groups.lastGroup().assertGet shouldBe groups.last
    }

    "return last group if there are multiple groups after a non Group key-value" in {
      val groups =
        (1 to 10) map {
          i =>
            if (i == 5)
              Transient.Put(1) //on the 5th iteration add a Put key-values. The 4th group should be returned.
            else
              Transient.Group(
                keyValues = randomizedIntKeyValues(keyValueCount),
                indexCompression = randomCompression(),
                valueCompression = randomCompression(),
                falsePositiveRate = 0.1,
                previous = None
              ).assertGet
        }

      groups.lastGroup().assertGet shouldBe groups.drop(3).head.asInstanceOf[Transient.Group]
    }
  }
}