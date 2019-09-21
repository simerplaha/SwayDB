package swaydb.core.util

import org.scalatest.{Matchers, WordSpec}
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.io.reader.Reader
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.util.Random

class BytezSpec extends WordSpec with Matchers {

  val intRanges =
    Seq(
      Int.MinValue to Int.MinValue + 100000,
      -1000000 to 1000000,
      Int.MaxValue - 100000 to Int.MaxValue
    )

  val longRanges =
    Seq(
      Long.MinValue to Long.MinValue + 100000,
      -1000000L to 1000000L,
      Long.MaxValue - 100000 to Long.MaxValue
    )

  "writeInt & readInt" in {
    runThis(1000.times) {
      val int = randomIntMax()
      val slice = Slice.create[Byte](ByteSizeOf.int)
      Bytes.writeInt(int, slice)
      slice.readInt() shouldBe int
    }
  }

  "writeLong & readLong" in {
    runThis(1000.times) {
      val long = Random.nextLong()
      val slice = Slice.create[Byte](ByteSizeOf.long)
      Bytes.writeLong(long, slice)
      slice.readLong() shouldBe long
    }
  }

  "writeBoolean & readBoolean" in {
    val trueSlice = Slice.create[Byte](ByteSizeOf.boolean)
    trueSlice.addBoolean(true)
    trueSlice.readBoolean() shouldBe true

    val falseSlice = Slice.create[Byte](ByteSizeOf.boolean)
    falseSlice.addBoolean(false)
    falseSlice.readBoolean() shouldBe false
  }

  "slice" should {
    "writeSignedInt & readSignedInt & sizeOf" in {
      intRanges foreach {
        range =>
          range foreach {
            i =>
              val unsignedBytes = Slice.writeUnsignedInt(i)
              unsignedBytes.readUnsignedInt().value shouldBe i
              Bytes.sizeOfUnsignedInt(i) shouldBe unsignedBytes.size

              val signedBytes = Slice.create[Byte](ByteSizeOf.varInt)
              Bytes.writeSignedInt(i, signedBytes)
              Bytes.readSignedInt(signedBytes).value shouldBe i
          }
      }
    }

    "writeSignedLong & readSignedLong & sizeOf" in {
      longRanges foreach {
        range =>
          range foreach {
            i =>
              val unsignedBytes = Slice.writeUnsignedLong(i)
              unsignedBytes.readUnsignedLong().value shouldBe i
              Bytes.sizeOfUnsignedLong(i) shouldBe unsignedBytes.size

              val signedBytes = Slice.create[Byte](ByteSizeOf.varLong)
              Bytes.writeSignedLong(i, signedBytes)
              Bytes.readSignedLong(signedBytes).value shouldBe i
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
              reader.readUnsignedInt().value shouldBe int
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
              reader.readSignedInt().value shouldBe int
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
              reader.readUnsignedLong().value shouldBe int
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
              reader.readSignedLong().value shouldBe int
          }
      }
    }

  }

}
