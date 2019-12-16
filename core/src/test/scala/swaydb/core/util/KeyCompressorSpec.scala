/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

import org.scalatest.{Matchers, WordSpec}
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.data.MaxKey
import swaydb.data.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

class KeyCompressorSpec extends WordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default

  "None, Fixed" in {
    runThis(20.times) {
      val last = randomFixedKeyValue(2)

      val (minKey, maxKey, compressedKey) =
        KeyCompressor.compress(
          head = None,
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
          head = Some(head),
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
          head = None,
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
          head = Some(head),
          last = last
        )
      minKey shouldBe head.key
      maxKey shouldBe MaxKey.Range(last.fromKey, last.toKey)

      KeyCompressor.decompress(compressedKey).runRandomIO.right.value shouldBe ((head.key, MaxKey.Range(last.fromKey, last.toKey)))
    }
  }
}
