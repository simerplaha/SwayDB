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

package swaydb.compression

import org.scalatest.Matchers._
import org.scalatest.WordSpec
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.util.Random

class CompressionSpec extends WordSpec {

  def assertSuccessfulCompression(compression: CompressionInternal) = {
    val string = "12345-12345-12345-12345" * Math.abs(Random.nextInt(99) + 1)
    val bytes: Slice[Byte] = string
    val compressedBytes: Slice[Byte] = compression.compressor.compress(bytes).get
    val decompressedBytes = compression.decompressor.decompress(compressedBytes, bytes.size)
    val decompressedString = decompressedBytes.readString()
    decompressedString shouldBe string
  }

  def assertUnsuccessfulCompression(compression: CompressionInternal) = {
    val string = "12345-12345-12345-12345" * Math.abs(Random.nextInt(99) + 1)
    val bytes: Slice[Byte] = string
    compression.compressor.compress(bytes) shouldBe empty
  }

  "Compression" should {

    "successfully compress bytes" when {
      "UnCompressed" in {
        assertSuccessfulCompression(CompressionInternal.UnCompressed)
      }

      "Snappy" in {
        assertSuccessfulCompression(CompressionInternal.Snappy(minCompressionPercentage = 10))
      }

      "LZ4" in {
        (1 to 100) foreach {
          _ =>
            val compressor = CompressorInternal.randomLZ4(minCompressionSavingsPercent = 10)
            val decompressor = DecompressorInternal.randomLZ4()
            //            println("compressor: " + compressor)
            //            println("decompressor: " + decompressor)
            assertSuccessfulCompression(CompressionInternal.LZ4(compressor, decompressor))
        }
      }
    }

    "successfully compress large byte array" in {
      val count = 10000
      val slice = Slice.create[Byte]((count + 1) * ByteSizeOf.long)

      val from = 1
      (from to (from + count)) map {
        long =>
          slice addAll Slice.writeLong(long)
      }

      val compressor = CompressorInternal.randomLZ4(minCompressionSavingsPercent = 20)

      def doCompression() = {
        val compressedBytes = compressor.compress(slice).get
        CompressorInternal.isCompressionSatisfied(20, compressedBytes.size, slice.size, compressor.getClass.getSimpleName) shouldBe true
        compressedBytes.size should be < slice.size
      }

      (1 to 10) foreach {
        _ =>
          doCompression()
      }
    }

    "return None" when {
      "Snappy" in {
        assertUnsuccessfulCompression(CompressionInternal.Snappy(minCompressionPercentage = 100))
      }

      "LZ4" in {
        (1 to 100) foreach {
          _ =>
            val compressor = CompressorInternal.randomLZ4(minCompressionSavingsPercent = 100)
            val decompressor = DecompressorInternal.randomLZ4()
            //            println("compressor: " + compressor)
            //            println("decompressor: " + decompressor)
            assertUnsuccessfulCompression(CompressionInternal.LZ4(compressor, decompressor))
        }
      }
    }

    "compress with header space" when {
      val string = "12345-12345-12345-12345" * 100
      val bytes: Slice[Byte] = string

      "lz4" in {
        (1 to 100) foreach {
          _ =>
            val compressed = CompressorInternal.randomLZ4().compress(10, bytes).get
            compressed.take(10) foreach (_ shouldBe 0.toByte)

            val decompressedBytes = DecompressorInternal.randomLZ4().decompress(compressed.drop(10), bytes.size)
            decompressedBytes shouldBe bytes
        }
      }

      "snappy" in {
        (1 to 100) foreach {
          _ =>
            val compressed = CompressorInternal.Snappy(Int.MinValue).compress(10, bytes).get
            compressed.take(10) foreach (_ shouldBe 0.toByte)

            val decompressedBytes = DecompressorInternal.Snappy.decompress(compressed.drop(10), bytes.size)
            decompressedBytes shouldBe bytes
        }
      }

      "UnCompressed" in {
        (1 to 100) foreach {
          _ =>
            val compressed = CompressorInternal.UnCompressed.compress(10, bytes).get
            compressed.take(10) foreach (_ shouldBe 0.toByte)

            val decompressedBytes = DecompressorInternal.UnCompressed.decompress(compressed.drop(10), bytes.size)
            decompressedBytes shouldBe bytes
        }
      }
    }
  }
}
