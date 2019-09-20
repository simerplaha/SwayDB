package swaydb.core.util

import org.scalatest.{Matchers, WordSpec}
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.data.slice.Slice
import swaydb.data.util.{ByteSizeOf, ByteUtil}

import scala.util.Random

class ByteUtilSpec extends WordSpec with Matchers {

  "writeInt & readInt" in {
    runThis(1000.times) {
      val int = randomIntMax()
      val slice = Slice.create[Byte](ByteSizeOf.int)
      ByteUtil.writeInt(int, slice)
      slice.readInt() shouldBe int
    }
  }

  "writeLong & readLong" in {
    runThis(1000.times) {
      val long = Random.nextLong()
      val slice = Slice.create[Byte](ByteSizeOf.long)
      ByteUtil.writeLong(long, slice)
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

  "writeSignedInt & readSignedInt & sizeOf" in {
    ((-1000000 to 1000000) ++ (Int.MaxValue - 100000 to Int.MaxValue) ++ (Int.MinValue to Int.MinValue + 100000)) foreach {
      i =>
        val unsignedPositiveBytes = Slice.writeIntUnsigned(i)
        unsignedPositiveBytes.readIntUnsigned().value shouldBe i
        ByteUtil.sizeOfUnsignedInt(i) shouldBe unsignedPositiveBytes.size

        val unsignedNegativeBytes = Slice.writeIntUnsigned(-i)
        unsignedNegativeBytes.readIntUnsigned().value shouldBe -i
        ByteUtil.sizeOfUnsignedInt(-i) shouldBe unsignedNegativeBytes.size

        val signedNegativeBytes = Slice.create[Byte](ByteSizeOf.varInt)
        ByteUtil.writeSignedInt(-i, signedNegativeBytes)
        ByteUtil.readSignedInt(signedNegativeBytes).value shouldBe -i

        val signedPositiveBytes = Slice.create[Byte](ByteSizeOf.varInt)
        ByteUtil.writeSignedInt(i, signedPositiveBytes)
        ByteUtil.readSignedInt(signedPositiveBytes).value shouldBe i
    }
  }

  "writeSignedLong & readSignedLong & sizeOf" in {
    ((-1000000L to 1000000L) ++ (Long.MaxValue - 100000 to Long.MaxValue) ++ (Long.MinValue to Long.MinValue + 100000)) foreach {
      i =>
        val unsignedPositiveBytes = Slice.writeLongUnsigned(i)
        unsignedPositiveBytes.readLongUnsigned().value shouldBe i
        ByteUtil.sizeOfUnsignedLong(i) shouldBe unsignedPositiveBytes.size

        val unsignedNegativeBytes = Slice.writeLongUnsigned(-i)
        unsignedNegativeBytes.readLongUnsigned().value shouldBe -i
        ByteUtil.sizeOfUnsignedLong(-i) shouldBe unsignedNegativeBytes.size

        val signedNegativeBytes = Slice.create[Byte](ByteSizeOf.varLong)
        ByteUtil.writeSignedLong(-i, signedNegativeBytes)
        ByteUtil.readSignedLong(signedNegativeBytes).value shouldBe -i

        val signedPositiveBytes = Slice.create[Byte](ByteSizeOf.varLong)
        ByteUtil.writeSignedLong(i, signedPositiveBytes)
        ByteUtil.readSignedLong(signedPositiveBytes).value shouldBe i
    }
  }

}
