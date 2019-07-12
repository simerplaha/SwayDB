package swaydb.core.segment.format.a.block


import swaydb.core.CommonAssertions._
import swaydb.core.IOAssert._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Value.{FromValue, RangeValue}
import swaydb.core.data._
import swaydb.core.io.reader.Reader
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestLimitQueues, TestTimer}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class SegmentBlockCacheSpec extends TestBase {
  implicit val order = KeyOrder.default

  "getSegmentBlock" should {
    "read uncompressed block" in {
      val keyValues =
        randomizedKeyValues(100, addPut = true, addRandomGroups = false)
          .updateStats(
            binarySearchIndexConfig =
              BinarySearchIndex.Config(
                enabled = true,
                minimumNumberOfKeys = 0,
                fullIndex = true,
                cacheOnAccess = false,
                compressions = randomCompressions()
              ),
            sortedIndexConfig =
              SortedIndex.Config(
                cacheOnAccess = false,
                prefixCompressionResetCount = 3,
                enableAccessPositionIndex = true,
                compressions = randomCompressions()
              )
          )

      val segment: SegmentBlock.Blocked = SegmentBlock.writeBlocked(keyValues, Int.MaxValue, Seq.empty).get
      val segmentBlockCache = getSegmentBlockCache(segment)

      val segmentBlock = segmentBlockCache.getSegmentBlock().get
      segmentBlockCache.segmentBlockCache.isCached shouldBe false

      segmentBlock.compressionInfo shouldBe empty
      segmentBlock.headerSize shouldBe segment.segmentBytes.head.size

      segmentBlockCache.footerCache.isCached shouldBe false
      val footer = segmentBlockCache.getFooter().get
      segmentBlockCache.footerCache.isCached shouldBe true
      footer.keyValueCount shouldBe keyValues.size
      footer.createdInLevel shouldBe Int.MaxValue

      //      val binarySearchIndexBytes = segmentBlockCache.createBinarySearchIndexReader().get.get.readRemaining()
      //
      //      println(binarySearchIndexBytes)

      keyValues foreach {
        keyValue =>
          BinarySearchIndex.search(
            key = keyValue.minKey,
            start = None,
            end = None,
            binarySearchIndexReader = segmentBlockCache.createBinarySearchIndexReader().get.get,
            sortedIndexReader = segmentBlockCache.createSortedIndexReader().get,
            valuesReader = segmentBlockCache.createValuesReader().get
          ).get shouldBe keyValue
      }
    }
  }
}
