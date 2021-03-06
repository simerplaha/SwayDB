/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.IOValues._
import swaydb.core.TestData._
import swaydb.core.data.Memory
import swaydb.data.MaxKey
import swaydb.data.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._

class KeyCompressorSpec extends AnyWordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default

  "None, Fixed" in {
    runThis(20.times) {
      val last = randomFixedKeyValue(2)

      val (minKey, maxKey, compressedKey) =
        KeyCompressor.compress(
          head = Memory.Null,
          last = last
        )

      minKey shouldBe last.key
      maxKey shouldBe MaxKey.Fixed(last.key)

      KeyCompressor.decompress(compressedKey).runRandomIO.right.value shouldBe ((last.key, MaxKey.Fixed(last.key)))
    }
  }

  "Some(Fixed), Fixed" in {
    runThis(20.times) {
      val head = randomFixedKeyValue(1)
      val last = randomFixedKeyValue(2)

      val (minKey, maxKey, compressedKey) =
        KeyCompressor.compress(
          head = head,
          last = last
        )

      minKey shouldBe head.key
      maxKey shouldBe MaxKey.Fixed(last.key)

      KeyCompressor.decompress(compressedKey).runRandomIO.right.value shouldBe ((head.key, MaxKey.Fixed(last.key)))
    }
  }

  "None, Range" in {
    runThis(20.times) {
      val last = randomRangeKeyValue(1, 10)

      val (minKey, maxKey, compressedKey) =
        KeyCompressor.compress(
          head = Memory.Null,
          last = last
        )

      minKey shouldBe last.key
      maxKey shouldBe MaxKey.Range(last.fromKey, last.toKey)

      KeyCompressor.decompress(compressedKey).runRandomIO.right.value shouldBe ((last.key, MaxKey.Range(last.fromKey, last.toKey)))
    }
  }

  "Some(_), Range" in {
    runThis(20.times) {
      val head = randomPutKeyValues(1, startId = Some(0)).head
      val last = randomRangeKeyValue(100, 200)

      val (minKey, maxKey, compressedKey) =
        KeyCompressor.compress(
          head = head,
          last = last
        )

      minKey shouldBe head.key
      maxKey shouldBe MaxKey.Range(last.fromKey, last.toKey)

      KeyCompressor.decompress(compressedKey).runRandomIO.right.value shouldBe ((head.key, MaxKey.Range(last.fromKey, last.toKey)))
    }
  }
}
