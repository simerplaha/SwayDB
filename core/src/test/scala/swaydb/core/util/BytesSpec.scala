/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

import org.scalatest.OptionValues._
import org.scalatest.{Matchers, WordSpec}
import swaydb.OK
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.io.reader.Reader
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

class BytesSpec extends WordSpec with Matchers {

  "compress, decompress & commonPrefixBytes" should {
    "return common bytes" in {
      val previous: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte))
      val next: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte, 5.toByte, 6.toByte))

      val (commonBytes, compressed) = Bytes.compress(previous, next, 1).value

      compressed shouldBe Slice(5, 6)
      commonBytes shouldBe 4
      compressed.isFull shouldBe true

      val decompress = Bytes.decompress(previous, compressed, 4)
      decompress.isFull shouldBe true
      decompress shouldBe next
      //return empty if minimum compressed bytes is not reached
      Bytes.compress(previous, next, 5) shouldBe empty

      Bytes.commonPrefixBytes(previous, next) shouldBe previous
    }

    "return empty bytes when all the bytes were compressed" in {
      val previous: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte))
      val next: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte))

      val (commonBytes, compressed) = Bytes.compress(previous, next, 1).value

      compressed shouldBe Slice.emptyBytes
      commonBytes shouldBe 3
      compressed.isFull shouldBe true

      val decompress = Bytes.decompress(previous, compressed, 3)
      decompress.isFull shouldBe true
      decompress shouldBe next
      //return empty if minimum compressed bytes is not reached
      Bytes.compress(previous, next, 4) shouldBe empty

      Bytes.commonPrefixBytes(previous, next) shouldBe next
    }

    "return empty when there are no common bytes" in {
      val previous: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte))
      val next: Slice[Byte] = Slice(Array(5.toByte, 6.toByte, 7.toByte, 8.toByte, 9.toByte, 10.toByte))

      Bytes.compress(previous, next, 1) shouldBe empty

      Bytes.commonPrefixBytes(previous, next) shouldBe Slice.emptyBytes
    }
  }

  "compressFull and compressExact" should {
    "compress when all bytes are compressed" in {
      val previous: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte))
      val next: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte))

      Bytes.compressFull(Some(previous), next).value shouldBe OK.instance
      Bytes.compressExact(previous, next).value shouldBe OK.instance
    }

    "return empty bytes when all the bytes were compressed and next key's size is smaller" in {
      val previous: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte))
      val next: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte))

      Bytes.compressFull(Some(previous), next).value shouldBe OK.instance
      Bytes.compressExact(previous, next) shouldBe empty
    }

    "return empty bytes when all the bytes were compressed and previous key's size is smaller" in {
      val previous: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte))
      val next: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte))

      Bytes.compressFull(Some(previous), next) shouldBe empty
      Bytes.compressExact(previous, next) shouldBe empty
    }

    "return empty when not all bytes were compressed" in {
      val previous: Slice[Byte] = Slice(Array(1.toByte, 2.toByte))
      val next: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte))

      Bytes.compressFull(Some(previous), next) shouldBe empty
      Bytes.compressExact(previous, next) shouldBe empty
    }

    "return empty when there are no common bytes" in {
      val previous: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte))
      val next: Slice[Byte] = Slice(Array(5.toByte, 6.toByte, 7.toByte, 8.toByte, 9.toByte, 10.toByte))

      Bytes.compressFull(Some(previous), next) shouldBe empty
      Bytes.compressExact(previous, next) shouldBe empty
    }
  }

  "compressJoin & decompressJoin" when {
    "there are no common bytes" in {
      val bytes1: Slice[Byte] = 12345
      val bytes2: Slice[Byte] = "abcde"
      val mergedBytes = Bytes.compressJoin(bytes1, bytes2)
      //2 extra bytes are required when compressJoin is not able to prefix compress.
      mergedBytes.size shouldBe 12
      mergedBytes.isFull shouldBe true

      val (readBytes1, readBytes2) = Bytes.decompressJoin(mergedBytes).runRandomIO.right.value

      (readBytes1, readBytes2) shouldBe ((bytes1, bytes2))
      readBytes1.isFull shouldBe true
      readBytes2.isFull shouldBe true
    }

    "merge, compress and decompress two byte array" in {
      val key = "123456789"
      val bytes1: Slice[Byte] = key * 100
      val bytes2: Slice[Byte] = key * 200
      val mergedBytes = Bytes.compressJoin(bytes1, bytes2)
      mergedBytes.size should be < (bytes1.size + bytes2.size)
      mergedBytes.isFull shouldBe true

      val (readBytes1, readBytes2) = Bytes.decompressJoin(mergedBytes).runRandomIO.right.value

      (readBytes1, readBytes2) shouldBe ((bytes1, bytes2))
      readBytes1.isFull shouldBe true
      readBytes2.isFull shouldBe true
    }

    "merge, compress and decompress two same byte array" in {
      val bytes: Slice[Byte] = "123456789" * 100
      val mergedBytes = Bytes.compressJoin(bytes, bytes)
      mergedBytes.size should be < (bytes.size + bytes.size)
      mergedBytes.isFull shouldBe true

      val (readBytes1, readBytes2) = Bytes.decompressJoin(mergedBytes).runRandomIO.right.value

      (readBytes1, readBytes2) shouldBe ((bytes, bytes))
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
      val totalByteSizeWithoutCompression =
        keys.foldLeft(0) {
          case (size, (fromKey, toKey)) =>
            size + fromKey.size + toKey.size
        }
      totalByteSizeWithoutCompression shouldBe 200.bytes

      //merge each (fromKey, toKey) pair extracting common bytes only for each pair.
      val individuallyCompressedBytes: Iterable[Slice[Byte]] = keys map { case (fromKey, toKey) => Bytes.compressJoin(fromKey, toKey) }
      val individualMergedSizes = individuallyCompressedBytes.foldLeft(0)(_ + _.size)
      individualMergedSizes shouldBe 120.bytes //results in 120.bytes which is smaller then without compression
      //uncompress
      individuallyCompressedBytes.map(Bytes.decompressJoin).map(_.runRandomIO.right.value).toList shouldBe keys

      //merge each (fromKey, toKey) pair with previous key-values merged bytes. This is should returns is higher compressed keys.
      val mergedCompressedKeys: Slice[Byte] =
        keys.drop(1).foldLeft(Bytes.compressJoin(keys.head._1, keys.head._2)) {
          case (merged, (fromKey, toKey)) =>
            Bytes.compressJoin(merged, Bytes.compressJoin(fromKey, toKey))
        }
      mergedCompressedKeys.size shouldBe 58.bytes //this results in 58.bytes which is smaller then individually compressed (fromKey, toKey) pair.
    }
  }

  "sizeOf" in {
    Bytes.sizeOfUnsignedInt(Int.MaxValue) shouldBe Slice.writeUnsignedInt(Int.MaxValue).size
    Bytes.sizeOfUnsignedLong(Long.MaxValue) shouldBe Slice.writeUnsignedLong(Long.MaxValue).size
  }

  "writeUnsignedIntReversed" in {
    Seq(Int.MaxValue, 100000000, 123, 0) foreach {
      intToWrite =>
        val slice = Slice.writeUnsignedInt(intToWrite)
        val sliceReverse = Bytes.writeUnsignedIntReversed(intToWrite)
        sliceReverse shouldBe Slice(slice.toList.reverse.toArray)

        Bytes.readLastUnsignedInt(sliceReverse).runRandomIO.right.value shouldBe ((intToWrite, slice.size))
    }
  }

  "normalise & deNormalise" should {
    "normalise keys" in {
      val bytes = Slice.fill(10)(1.toByte)

      var normalisedBytes: Slice[Byte] = Bytes.normalise(bytes, toSize = 11)
      normalisedBytes should have size 11
      Bytes.deNormalise(normalisedBytes) shouldBe bytes

      normalisedBytes = Bytes.normalise(bytes, toSize = 15)
      normalisedBytes should have size 15
      Bytes.deNormalise(normalisedBytes) shouldBe bytes

      normalisedBytes = Bytes.normalise(bytes, toSize = 100000)
      normalisedBytes should have size 100000
      Bytes.deNormalise(normalisedBytes) shouldBe bytes
    }

    "random" in {
      runThis(1000.times) {
        val bytes = randomBytesSlice(randomIntMax(100000))
        val toSize = (bytes.size + 1) max randomIntMax(100000)

        val normalisedBytes = Bytes.normalise(bytes, toSize = toSize)
        normalisedBytes should have size toSize
        Bytes.deNormalise(normalisedBytes) shouldBe bytes
      }

      runThis(1000.times) {
        val bytes = randomBytesSlice(randomIntMax(Byte.MaxValue))
        val toSize = (bytes.size + 1) max randomIntMax(Byte.MaxValue)

        val normalisedBytes = Bytes.normalise(bytes, toSize = toSize)
        normalisedBytes should have size toSize
        Bytes.deNormalise(normalisedBytes) shouldBe bytes
      }
    }
  }

  "normalise & deNormalise" when {
    "appendHeader: toSize == +1" in {
      val header = Slice.writeUnsignedInt(Int.MaxValue)
      val bytes = Slice.fill(10)(5.toByte)
      val normalisedBytes = Bytes.normalise(appendHeader = header, bytes = bytes, toSize = header.size + bytes.size + 1)
      normalisedBytes should have size 16

      val reader = Reader(normalisedBytes)
      reader.readUnsignedInt() shouldBe Int.MaxValue
      val deNormalisedBytes = Bytes.deNormalise(reader.readRemaining())
      deNormalisedBytes shouldBe bytes
    }

    "appendHeader: toSize == +5" in {
      val header = Slice.writeUnsignedInt(Int.MaxValue)
      val bytes = Slice.fill(10)(5.toByte)
      val normalisedBytes = Bytes.normalise(appendHeader = header, bytes = bytes, toSize = header.size + bytes.size + 5)
      normalisedBytes should have size 20

      val reader = Reader(normalisedBytes)
      reader.readUnsignedInt() shouldBe Int.MaxValue
      val deNormalisedBytes = Bytes.deNormalise(reader.readRemaining())
      deNormalisedBytes shouldBe bytes
    }
  }

  "nonZero" should {
    "not add zero bytes for non zero integers and calculate size" in {
      runThisParallel(100.times) {
        val max = randomIntMax(Int.MaxValue) max 1
        val min = (max - 100000) max 1

        (min to max) foreach {
          i =>
            val writtenBytes = Bytes.writeUnsignedIntNonZero(i)
            Bytes.readUnsignedIntNonZero(writtenBytes) shouldBe i

            val expectedSize = Bytes.sizeOfUnsignedInt(i)
            writtenBytes.size shouldBe expectedSize

            writtenBytes.exists(_ == 0.toByte) shouldBe false
            writtenBytes should not be empty
        }
      }
    }
  }
}
