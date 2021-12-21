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

package swaydb.core.util

import org.scalatest.OptionValues._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import swaydb.effect.IOValues._
import swaydb.OK
import swaydb.serializers._
import swaydb.serializers.Default._
import swaydb.slice.{Slice, SliceReader}
import swaydb.slice.SliceTestKit._
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._
import swaydb.utils.StorageUnits._

class BytesSpec extends AnyWordSpec {

  "compress, decompress & commonPrefixBytes" should {
    "return common bytes" in {
      val previous = Slice.wrap(Array[Byte](1, 2, 3, 4))
      val next = Slice.wrap(Array[Byte](1, 2, 3, 4, 5, 6))

      val (commonBytes, compressed) = Bytes.compress(previous, next, 1).toOption().value

      compressed shouldBe Slice(5, 6)
      commonBytes shouldBe 4
      compressed.isFull shouldBe true

      val decompress = Bytes.decompress(previous, compressed, 4)
      decompress.isFull shouldBe true
      decompress shouldBe next
      //return empty if minimum compressed bytes is not reached
      Bytes.compress(previous, next, 5).toOption() shouldBe empty

      Bytes.commonPrefixBytes(previous, next) shouldBe previous
    }

    "return empty bytes when all the bytes were compressed" in {
      val previous = Slice.wrap(Array[Byte](1, 2, 3, 4))
      val next = Slice.wrap(Array[Byte](1, 2, 3))

      val (commonBytes, compressed) = Bytes.compress(previous, next, 1).toOption().value

      compressed shouldBe Slice.emptyBytes
      commonBytes shouldBe 3
      compressed.isFull shouldBe true

      val decompress = Bytes.decompress(previous, compressed, 3)
      decompress.isFull shouldBe true
      decompress shouldBe next
      //return empty if minimum compressed bytes is not reached
      Bytes.compress(previous, next, 4).toOption() shouldBe empty

      Bytes.commonPrefixBytes(previous, next) shouldBe next
    }

    "return empty when there are no common bytes" in {
      val previous = Slice.wrap(Array[Byte](1, 2, 3, 4))
      val next = Slice.wrap(Array[Byte](5, 6, 7, 8, 9, 10))

      Bytes.compress(previous, next, 1).toOption() shouldBe empty

      Bytes.commonPrefixBytes(previous, next) shouldBe Slice.emptyBytes
    }
  }

  "compressFull and compressExact" should {
    "compress when all bytes are compressed" in {
      val previous = Slice.wrap(Array[Byte](1, 2, 3, 4))
      val next = Slice.wrap(Array[Byte](1, 2, 3, 4))

      Bytes.compressFull(Some(previous), next).value shouldBe OK.instance
      Bytes.compressExact(previous, next).value shouldBe OK.instance
    }

    "return empty bytes when all the bytes were compressed and next key's size is smaller" in {
      val previous = Slice.wrap(Array[Byte](1, 2, 3, 4))
      val next = Slice.wrap(Array[Byte](1, 2, 3))

      Bytes.compressFull(Some(previous), next).value shouldBe OK.instance
      Bytes.compressExact(previous, next) shouldBe empty
    }

    "return empty bytes when all the bytes were compressed and previous key's size is smaller" in {
      val previous = Slice.wrap(Array[Byte](1, 2, 3))
      val next = Slice.wrap(Array[Byte](1, 2, 3, 4))

      Bytes.compressFull(Some(previous), next) shouldBe empty
      Bytes.compressExact(previous, next) shouldBe empty
    }

    "return empty when not all bytes were compressed" in {
      val previous = Slice.wrap(Array[Byte](1, 2))
      val next = Slice.wrap(Array[Byte](1, 2, 3))

      Bytes.compressFull(Some(previous), next) shouldBe empty
      Bytes.compressExact(previous, next) shouldBe empty
    }

    "return empty when there are no common bytes" in {
      val previous = Slice.wrap(Array[Byte](1, 2, 3, 4))
      val next = Slice.wrap(Array[Byte](5, 6, 7, 8, 9, 10))

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

      val (readBytes1, readBytes2) = Bytes.decompressJoin(mergedBytes).runRandomIO.get

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

      val (readBytes1, readBytes2) = Bytes.decompressJoin(mergedBytes).runRandomIO.get

      (readBytes1, readBytes2) shouldBe ((bytes1, bytes2))
      readBytes1.isFull shouldBe true
      readBytes2.isFull shouldBe true
    }

    "merge, compress and decompress two same byte array" in {
      val bytes: Slice[Byte] = "123456789" * 100
      val mergedBytes = Bytes.compressJoin(bytes, bytes)
      mergedBytes.size should be < (bytes.size + bytes.size)
      mergedBytes.isFull shouldBe true

      val (readBytes1, readBytes2) = Bytes.decompressJoin(mergedBytes).runRandomIO.get

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
      val individuallyCompressedBytes: Iterable[Slice[Byte]] =
        keys map {
          case (fromKey, toKey) =>
            Bytes.compressJoin(fromKey, toKey)
        }

      val individualMergedSizes = individuallyCompressedBytes.foldLeft(0)(_ + _.size)
      individualMergedSizes shouldBe 120.bytes //results in 120.bytes which is smaller then without compression
      //uncompress
      individuallyCompressedBytes.map(Bytes.decompressJoin).map(_.runRandomIO.get).toList shouldBe keys

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
        sliceReverse shouldBe Slice.wrap(slice.toList.reverse.toArray)

        Bytes.readLastUnsignedInt(sliceReverse).runRandomIO.get shouldBe ((intToWrite, slice.size))
    }
  }

  "normalise & deNormalise" should {
    "normalise keys" in {
      val bytes = Slice.fill[Byte](10)(1)

      var normalisedBytes = Bytes.normalise(bytes, toSize = 11)
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
        val bytes = genBytesSlice(randomIntMax(100000))
        val toSize = (bytes.size + 1) max randomIntMax(100000)

        val normalisedBytes = Bytes.normalise(bytes, toSize = toSize)
        normalisedBytes should have size toSize
        Bytes.deNormalise(normalisedBytes) shouldBe bytes
      }

      runThis(1000.times) {
        val bytes = genBytesSlice(randomIntMax(Byte.MaxValue))
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
      val bytes = Slice.fill[Byte](10)(5)
      val normalisedBytes = Bytes.normalise(appendHeader = header, bytes = bytes, toSize = header.size + bytes.size + 1)
      normalisedBytes should have size 16

      val reader = SliceReader(normalisedBytes)
      reader.readUnsignedInt() shouldBe Int.MaxValue
      val deNormalisedBytes = Bytes.deNormalise(reader.readRemaining())
      deNormalisedBytes shouldBe bytes
    }

    "appendHeader: toSize == +5" in {
      val header = Slice.writeUnsignedInt(Int.MaxValue)
      val bytes = Slice.fill[Byte](10)(5)
      val normalisedBytes = Bytes.normalise(appendHeader = header, bytes = bytes, toSize = header.size + bytes.size + 5)
      normalisedBytes should have size 20

      val reader = SliceReader(normalisedBytes)
      reader.readUnsignedInt() shouldBe Int.MaxValue
      val deNormalisedBytes = Bytes.deNormalise(reader.readRemaining())
      deNormalisedBytes shouldBe bytes
    }
  }

  "nonZero" should {
    "not add zero bytes for non zero integers and calculate size" in {
      runThis(100.times) {
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
