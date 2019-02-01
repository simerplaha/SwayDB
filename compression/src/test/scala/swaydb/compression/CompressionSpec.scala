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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.compression

import org.scalatest.{Matchers, WordSpec}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.serializers.Default._
import swaydb.serializers._
import IOAssert._

import scala.util.Random

class CompressionSpec extends WordSpec with Matchers {

  def assertSuccessfulCompression(compression: CompressionInternal) = {
    val string = "12345-12345-12345-12345" * Math.abs(Random.nextInt(99) + 1)
    val bytes: Slice[Byte] = string
    val compressedBytes: Slice[Byte] = compression.compressor.compress(bytes).assertGet
    val decompressedBytes = compression.decompressor.decompress(compressedBytes, bytes.size).assertGet
    val decompressedString = decompressedBytes.readString()
    decompressedString shouldBe string
  }

  def assertUnsuccessfulCompression(compression: CompressionInternal) = {
    val string = "12345-12345-12345-12345" * Math.abs(Random.nextInt(99) + 1)
    val bytes: Slice[Byte] = string
    compression.compressor.compress(bytes).assertGetOpt shouldBe empty
  }

  "Compression" should {

    "successfully compress bytes" when {
      "UnCompressedGroup" in {
        assertSuccessfulCompression(CompressionInternal.UnCompressedGroup)
      }

      "Snappy" in {
        assertSuccessfulCompression(CompressionInternal.Snappy(minCompressionPercentage = 10))
      }

      "LZ4" in {
        (1 to 100) foreach {
          _ =>
            val compressor = CompressorInternal.randomLZ4(minCompressionPercentage = 10)
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

      val compressor = CompressorInternal.randomLZ4(minCompressionPercentage = 20)

      def doCompression() = {
        val compressedBytes = compressor.compress(slice).assertGet
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
            val compressor = CompressorInternal.randomLZ4(minCompressionPercentage = 100)
            val decompressor = DecompressorInternal.randomLZ4()
            //            println("compressor: " + compressor)
            //            println("decompressor: " + decompressor)
            assertUnsuccessfulCompression(CompressionInternal.LZ4(compressor, decompressor))
        }
      }
    }
  }
}
