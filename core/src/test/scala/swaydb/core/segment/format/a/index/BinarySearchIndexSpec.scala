package swaydb.core.segment.format.a.index

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.CommonAssertions.eitherOne
import swaydb.core.RunThis._
import swaydb.core.TestData.{randomBytesSlice, randomIntMax}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.MatchResult
import swaydb.core.util.Bytes
import swaydb.data.IO
import swaydb.data.slice.Slice

class BinarySearchIndexSpec extends WordSpec with Matchers {

  def assertFind(bytes: Slice[Byte],
                 values: Seq[Int],
                 index: BinarySearchIndex) =
    runThis(10.times) {
      val randomBytes = randomBytesSlice(1)

      val (adjustedOffset, alteredBytes) =
        eitherOne(
          (index.offset, bytes),
          (index.offset, bytes ++ randomBytesSlice(randomIntMax(100))),
          (index.offset.copy(start = randomBytes.size), randomBytes ++ bytes.close()),
          (index.offset.copy(start = randomBytes.size), randomBytes ++ bytes ++ randomBytesSlice(randomIntMax(100)))
        )

      val largestValue = values.last

      def getValue(valueToFind: Int)(valueFound: Int): IO[MatchResult] =
        IO {
          if (valueToFind == valueFound)
            MatchResult.Matched(null)
          else if (valueToFind < valueFound)
            MatchResult.Stop
          else
            MatchResult.Next
        }

      values foreach {
        value =>
          BinarySearchIndex.find(
            index = index.copy(offset = adjustedOffset),
            reader = Reader(alteredBytes),
            assertValue = getValue(valueToFind = value)
          ).get shouldBe defined
      }

      val notInIndex = (values.head - 100 until values.head) ++ (largestValue + 1 to largestValue + 100)

      notInIndex foreach {
        i =>
          BinarySearchIndex.find(
            index = index.copy(offset = adjustedOffset),
            reader = Reader(alteredBytes),
            assertValue = getValue(valueToFind = i)
          ).get shouldBe empty
      }
    }

  "it" should {
    "write full index" when {
      "all values have the same size" in {
        Seq(0 to 127, 128 to 300, 16384 to 16384 + 200, Int.MaxValue - 5000 to Int.MaxValue - 1000) foreach {
          values =>
            val valuesCount = values.size
            val largestValue = values.last
            val state =
              BinarySearchIndex.State(
                largestValue = largestValue,
                uniqueValuesCount = valuesCount,
                isFullIndex = true
              )

            values foreach {
              offset =>
                BinarySearchIndex.write(value = offset, state = state).get
            }

            BinarySearchIndex.writeHeader(state).get

            state.bytes.isFull shouldBe true

            val index =
              BinarySearchIndex.read(
                offset = BinarySearchIndex.Offset(0, state.bytes.written),
                reader = Reader(state.bytes)
              ).get

            //byte size of Int.MaxValue is 5, but the index will switch to using 4 byte ints.
            index.bytesPerValue should be <= 4

            assertFind(
              bytes = state.bytes,
              values = values,
              index = index
            )
        }
      }
    }
  }

  "it" should {
    "write full index" when {
      "all values have unique size" in {
        val values = (126 to 130) ++ (16384 - 2 to 16384)
        val valuesCount = values.size
        val largestValue = values.last
        val state =
          BinarySearchIndex.State(
            largestValue = largestValue,
            uniqueValuesCount = valuesCount,
            isFullIndex = true
          )

        values foreach {
          value =>
            BinarySearchIndex.write(value = value, state = state).get
        }
        BinarySearchIndex.writeHeader(state).get

        state.writtenValues shouldBe values.size

        val index =
          BinarySearchIndex.read(
            offset = BinarySearchIndex.Offset(0, state.bytes.written),
            reader = Reader(state.bytes)
          ).get

        index.bytesPerValue shouldBe Bytes.sizeOf(largestValue)
        val headerSize = BinarySearchIndex.optimalHeaderSize(largestValue = largestValue, valuesCount = values.size)
        index.headerSize shouldBe headerSize
        index.valuesCount shouldBe values.size

        assertFind(
          bytes = state.bytes,
          values = values,
          index = index
        )
      }
    }
  }
}
