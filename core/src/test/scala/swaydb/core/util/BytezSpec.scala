package swaydb.core.util

import java.nio.charset.StandardCharsets

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.data.RunThis._
import swaydb.core.TestData._
import swaydb.core.io.reader.Reader
import swaydb.data.slice.Slice._
import swaydb.data.util.{ByteSizeOf, Bytez}

import scala.util.Random
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice.Sliced

class BytezSpec extends AnyWordSpec with Matchers {

  val intRanges =
    Seq(
      Int.MinValue to Int.MinValue + 100000,
      (1 to 10000) map (_ => Random.nextInt()),
      -1000000 to 1000000,
      Int.MaxValue - 100000 to Int.MaxValue
    )

  val longRanges =
    Seq(
      Long.MinValue to Long.MinValue + 100000,
      (1 to 10000) map (_ => Random.nextLong()),
      -1000000L to 1000000L,
      Long.MaxValue - 100000 to Long.MaxValue
    )

  "writeInt & readInt" in {
    intRanges foreach {
      range =>
        range foreach {
          int =>
            val slice = Slice.create[Byte](ByteSizeOf.int)
            Bytes.writeInt(int, slice)
            slice.readInt() shouldBe int //from Slice
            Bytes.readInt(slice) shouldBe int //from slice
            Bytes.readInt(Reader(slice)) shouldBe int //from reader
        }
    }
  }

  "writeLong & readLong" in {
    longRanges foreach {
      range =>
        range foreach {
          long =>
            val slice = Slice.create[Byte](ByteSizeOf.long)
            Bytes.writeLong(long, slice)
            slice.readLong() shouldBe long
            Bytes.readLong(slice) shouldBe long
            Bytes.readLong(Reader(slice)) shouldBe long
        }
    }
  }

  "writeBoolean & readBoolean" when {
    "slice" in {
      val trueSlice = Slice.create[Byte](ByteSizeOf.boolean)
      trueSlice.addBoolean(true)
      trueSlice.readBoolean() shouldBe true

      val falseSlice = Slice.create[Byte](ByteSizeOf.boolean)
      falseSlice.addBoolean(false)
      falseSlice.readBoolean() shouldBe false
    }

    "reader" in {
      val booleans = Seq.fill(10)(randomBoolean())

      val slice = Slice.create[Byte](ByteSizeOf.boolean * booleans.size)

      booleans foreach slice.addBoolean

      val reader = Reader(slice)

      booleans foreach {
        expectedBoolean =>
          reader.readBoolean() shouldBe expectedBoolean
      }
    }
  }

  "writeString & readString" when {
    "single" in {
      runThis(10.times) {
        val slice = Slice.create[Byte](10000)
        val string = randomCharacters(randomIntMax(1000) max 1)
        Bytes.writeString(string, slice, StandardCharsets.UTF_8)
        slice.readString() shouldBe string
        Bytes.readString(slice, StandardCharsets.UTF_8) shouldBe string

        Bytes.readString(Reader(slice), StandardCharsets.UTF_8) shouldBe string
      }
    }

    "multiple" in {
      runThis(10.times) {
        val slice = Slice.create[Byte](10000)
        val string1 = randomCharacters(randomIntMax(1000) max 1)
        val string2 = randomCharacters(randomIntMax(1000) max 1)

        Bytes.writeString(string1, slice, StandardCharsets.UTF_8)
        val string1Size = slice.size
        Bytes.writeString(string2, slice, StandardCharsets.UTF_8)
        val string2Size = slice.size - string1Size

        val reader = Reader(slice)

        Bytes.readString(string1Size, reader, StandardCharsets.UTF_8) shouldBe string1
        Bytes.readString(string2Size, reader, StandardCharsets.UTF_8) shouldBe string2
      }
    }
  }

  "slice" should {
    "writeSignedInt, readSignedInt, readUnsignedIntWithByteSize & sizeOf" in {
      intRanges foreach {
        range =>
          range foreach {
            int =>
              val unsignedBytes = Slice.writeUnsignedInt(int)
              unsignedBytes.readUnsignedInt() shouldBe int
              val actualByteSize = Bytes.sizeOfUnsignedInt(int)
              actualByteSize shouldBe unsignedBytes.size

              Bytes.readUnsignedIntWithByteSize(unsignedBytes) shouldBe(int, actualByteSize)

              val signedBytes = Slice.create[Byte](ByteSizeOf.varInt)
              Bytes.writeSignedInt(int, signedBytes)
              Bytes.readSignedInt(signedBytes) shouldBe int
          }
      }
    }

    "writeSignedLong & readSignedLong & sizeOf" in {
      longRanges foreach {
        range =>
          range foreach {
            long =>
              val unsignedBytes = Slice.writeUnsignedLong(long)
              unsignedBytes.readUnsignedLong() shouldBe long
              val actualByteSize = Bytes.sizeOfUnsignedLong(long)
              actualByteSize shouldBe unsignedBytes.size

              Bytes.readUnsignedLongWithByteSize(unsignedBytes) shouldBe(long, actualByteSize)

              val signedBytes = Slice.create[Byte](ByteSizeOf.varLong)
              Bytes.writeSignedLong(long, signedBytes)
              Bytes.readSignedLong(signedBytes) shouldBe long
          }
      }
    }
  }

  "reader" should {
    "readUnsignedInt" in {
      intRanges foreach {
        range =>
          val slice = Slice.create[Byte](2000000 * ByteSizeOf.varInt)

          range foreach {
            int =>
              Bytes.writeUnsignedInt(int, slice)
          }

          val reader = Reader(slice)
          range foreach {
            int =>
              reader.readUnsignedInt() shouldBe int
          }
      }
    }

    "readSignedInt" in {
      intRanges foreach {
        range =>
          val slice = Slice.create[Byte](2000000 * ByteSizeOf.varInt)

          range foreach {
            int =>
              Bytes.writeSignedInt(int, slice)
          }

          val reader = Reader(slice)
          range foreach {
            int =>
              reader.readSignedInt() shouldBe int
          }
      }
    }

    "readUnsignedLong" in {
      longRanges foreach {
        range =>
          val slice = Slice.create[Byte](2000000 * ByteSizeOf.varLong)

          range foreach {
            long =>
              Bytes.writeUnsignedLong(long, slice)
          }

          val reader = Reader(slice)
          range foreach {
            int =>
              reader.readUnsignedLong() shouldBe int
          }
      }
    }

    "readSignedLong" in {
      longRanges foreach {
        range =>
          val slice = Slice.create[Byte](2000000 * ByteSizeOf.varLong)

          range foreach {
            long =>
              Bytes.writeSignedLong(long, slice)
          }

          val reader = Reader(slice)
          range foreach {
            int =>
              reader.readSignedLong() shouldBe int
          }
      }
    }

    "readUnsignedLongByteSize" in {
      var bytes = Slice.create[Byte](9)
      bytes.addUnsignedLong(0)
      bytes.size shouldBe 1
      bytes.readUnsignedLongByteSize() shouldBe 1

      bytes = Slice.create[Byte](9)
      bytes.addUnsignedLong(Long.MaxValue)
      bytes.size shouldBe 9
      bytes.readUnsignedLongByteSize() shouldBe 9
      bytes.isFull shouldBe true

      longRanges foreach {
        range =>
          range foreach {
            long =>
              val slice = Slice.create[Byte](ByteSizeOf.varLong)
              Bytes.writeUnsignedLong(long, slice)

              Bytez.readUnsignedLongByteSize(slice) shouldBe slice.size
          }
      }
    }

  }

  "writeUnsignedIntReversed" in {

    intRanges foreach {
      range =>
        range foreach {
          int =>
            val slice = Bytes.writeUnsignedIntReversed(int)
            Bytes.readLastUnsignedInt(slice) shouldBe(int, Bytes.sizeOfUnsignedInt(int))
        }

    }
  }
}
