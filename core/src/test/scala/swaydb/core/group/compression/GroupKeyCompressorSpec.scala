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

package swaydb.core.group.compression

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.{CommonAssertions, TestLimitQueues, TryAssert}
import swaydb.core.data.Transient
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.data.order.KeyOrder
import swaydb.core.TryAssert._
import swaydb.data.repairAppendix.MaxKey

class GroupKeyCompressorSpec extends WordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default

  "None, Fixed" in {
    runThis(20.times) {
      val last = randomFixedKeyValue(2).toTransient

      val (minKey, maxKey, compressedKey) =
        GroupKeyCompressor.compress(
          head = None,
          last = last
        )
      minKey shouldBe last.key
      maxKey shouldBe MaxKey.Fixed(last.key)

      GroupKeyCompressor.decompress(compressedKey).assertGet shouldBe ((last.key, MaxKey.Fixed(last.key)))
    }
  }

  "Some(Fixed), Fixed" in {
    runThis(20.times) {
      val head = randomFixedKeyValue(1).toTransient
      val last = randomFixedKeyValue(2).toTransient

      val (minKey, maxKey, compressedKey) =
        GroupKeyCompressor.compress(
          head = Some(head),
          last = last
        )
      minKey shouldBe head.key
      maxKey shouldBe MaxKey.Fixed(last.key)

      GroupKeyCompressor.decompress(compressedKey).assertGet shouldBe ((head.key, MaxKey.Fixed(last.key)))
    }
  }

  "None, Range" in {
    runThis(20.times) {
      val last = randomRangeKeyValue(1, 10)

      val (minKey, maxKey, compressedKey) =
        GroupKeyCompressor.compress(
          head = None,
          last = last.toTransient
        )
      minKey shouldBe last.key
      maxKey shouldBe MaxKey.Range(last.fromKey, last.toKey)

      GroupKeyCompressor.decompress(compressedKey).assertGet shouldBe ((last.key, MaxKey.Range(last.fromKey, last.toKey)))
    }
  }

  "Some(_), Range" in {
    runThis(20.times) {
      val head = randomPutKeyValues(1, startId = Some(0)).head.toTransient
      val last = randomRangeKeyValue(100, 200)

      val (minKey, maxKey, compressedKey) =
        GroupKeyCompressor.compress(
          head = Some(head),
          last = last.toTransient
        )
      minKey shouldBe head.key
      maxKey shouldBe MaxKey.Range(last.fromKey, last.toKey)

      GroupKeyCompressor.decompress(compressedKey).assertGet shouldBe ((head.key, MaxKey.Range(last.fromKey, last.toKey)))
    }
  }

  "None, Group" in {
    runThis(20.times) {
      val last: Transient.Group = randomGroup(randomizedKeyValues(100))

      val (minKey, maxKey, compressedKey) =
        GroupKeyCompressor.compress(
          head = None,
          last = last
        )
      minKey shouldBe last.key
      maxKey shouldBe last.maxKey

      GroupKeyCompressor.decompress(compressedKey).assertGet shouldBe ((last.key, last.maxKey))
    }
  }

  "Some(_), Group" in {
    runThis(20.times) {
      val head = randomPutKeyValues(1, startId = Some(0)).head.toTransient
      val last = randomGroup(randomizedKeyValues(100))

      val (minKey, maxKey, compressedKey) =
        GroupKeyCompressor.compress(
          head = Some(head),
          last = last
        )
      minKey shouldBe head.key
      maxKey shouldBe last.maxKey

      GroupKeyCompressor.decompress(compressedKey).assertGet shouldBe ((head.key, last.maxKey))
    }
  }

}
