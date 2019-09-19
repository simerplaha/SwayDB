package swaydb.core.segment.format.a.entry.id

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.data.slice.Slice

class StatsSpec extends WordSpec with Matchers {

  "it" should {
    "update stats" when {
      "value length is empty" in {
        val stats = randomStats(indexEntry = Slice.fill(2)(1.toByte), value = Slice.emptyEmptyBytes)
        stats.valueLength shouldBe 0
        stats.segmentSize should be > 0
        stats.chainPosition shouldBe 1
        stats.segmentValueAndSortedIndexEntrySize should be > 0
        stats.segmentSortedIndexSizeWithoutHeader should be > 0
      }
    }
  }
}
