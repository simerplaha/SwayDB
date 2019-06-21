package swaydb.core.segment.format.a.index

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.TestData
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.MatchResult
import swaydb.core.util.Bytes
import swaydb.data.IO

class BinarySearchIndexSpec extends WordSpec with Matchers {

  "search" in {
    val values = 0 to 9
    val valuesCount = values.size
    val largestValue = values.last
    val state =
      BinarySearchIndex.State(
        largestValue = largestValue,
        valuesCount = valuesCount,
        buildFullBinarySearchIndex = TestData.buildFullBinarySearchIndex
      )

    values map {
      offset =>
        BinarySearchIndex.write(value = offset, state = state).get
        offset
    }
    BinarySearchIndex.writeHeader(state).get

    state.bytes.isFull shouldBe true

    val index =
      BinarySearchIndex.read(
        offset = BinarySearchIndex.Offset(0, state.bytes.written),
        reader = Reader(state.bytes)
      ).get

    index.byteSizeOfLargestValue shouldBe Bytes.sizeOf(largestValue)
    val headerSize = BinarySearchIndex.optimalHeaderSize(largestValue = largestValue, valuesCount = values.size)
    index.headerSize shouldBe headerSize
    index.valuesCount shouldBe values.size

    def getValue(valueToFind: Int)(value: Int): IO[MatchResult] =
      IO {
        val valueOffset = state.bytes.take(value, index.byteSizeOfLargestValue).readIntUnsigned().get
        if (valueToFind == valueOffset)
          MatchResult.Matched(null)
        else if (valueToFind > valueOffset)
          MatchResult.Stop
        else
          MatchResult.Next
      }

    values foreach {
      value =>
        BinarySearchIndex.find(
          index = index,
          assertValue = getValue(valueToFind = value)
        ).get shouldBe defined
    }

    val notInIndex = (-100 until values.head) ++ (largestValue + 1 to largestValue + 100)

    notInIndex foreach {
      i =>
        BinarySearchIndex.find(
          index = index,
          assertValue = getValue(valueToFind = i)
        ).get shouldBe empty
    }
  }
}
