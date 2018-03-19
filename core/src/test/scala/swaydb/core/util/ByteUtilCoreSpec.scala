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

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.TryAssert
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

class ByteUtilCoreSpec extends WordSpec with Matchers with TryAssert {

  "ByteUtil.commonPrefixBytes" should {
    "return common bytes" in {
      val bytes1: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte))
      val bytes2: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte, 5.toByte, 6.toByte))

      ByteUtilCore.commonPrefixBytes(bytes1, bytes2) shouldBe 4
    }

    "return 0 common bytes when there are no common bytes" in {
      val bytes1: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte))
      val bytes2: Slice[Byte] = Slice(Array(5.toByte, 6.toByte, 7.toByte, 8.toByte, 9.toByte, 10.toByte))

      ByteUtilCore.commonPrefixBytes(bytes1, bytes2) shouldBe 0
    }
  }

  "compress and uncompress" should {
    "merge, compress and uncompress two byte array" in {
      val key = "123456789"
      val bytes1: Slice[Byte] = key * 100
      val bytes2: Slice[Byte] = key * 200
      val mergedBytes = ByteUtilCore.compress(bytes1, bytes2)
      mergedBytes.size should be < (bytes1.size + bytes2.size)
      mergedBytes.isFull shouldBe true

      val (readBytes1, readBytes2) = ByteUtilCore.uncompress(mergedBytes).assertGet

      (readBytes1, readBytes2) shouldBe ((bytes1, bytes2))
      readBytes1.isFull shouldBe true
      readBytes2.isFull shouldBe true
    }

    "merge, compress and uncompress byte arrays" in {
      var key = Long.MaxValue

      def nextKey = {
        key = key + 1
        (key + 1).toString
      }

      val keys = List[(Slice[Byte], Slice[Byte])]((nextKey, nextKey), (nextKey, nextKey), (nextKey, nextKey), (nextKey, nextKey), (nextKey, nextKey))
      //This results in 200.bytes without any compression.
      val totalByteSizeWithoutCompression = keys.foldLeft(0) {
        case (size, (fromKey, toKey)) =>
          size + fromKey.size + toKey.size
      }
      totalByteSizeWithoutCompression shouldBe 200.bytes

      //merge each (fromKey, toKey) pair extracting common bytes only for each pair.
      val individuallyCompressedBytes: Iterable[Slice[Byte]] = keys map { case (fromKey, toKey) => ByteUtilCore.compress(fromKey, toKey) }
      val individualMergedSizes = individuallyCompressedBytes.foldLeft(0)(_ + _.size)
      individualMergedSizes shouldBe 120.bytes //results in 120.bytes which is smaller then without compression
      //uncompress
      individuallyCompressedBytes.map(ByteUtilCore.uncompress).map(_.assertGet).toList shouldBe keys

      //merge each (fromKey, toKey) pair with previous key-values merged bytes. This is should returns is higher compressed keys.
      val mergedCompressedKeys: Slice[Byte] =
        keys.drop(1).foldLeft(ByteUtilCore.compress(keys.head._1, keys.head._2)) {
          case (merged, (fromKey, toKey)) =>
            ByteUtilCore.compress(merged, ByteUtilCore.compress(fromKey, toKey))
        }
      mergedCompressedKeys.size shouldBe 58.bytes //this results in 58.bytes which is smaller then individually compressed (fromKey, toKey) pair.
    }
  }
}
