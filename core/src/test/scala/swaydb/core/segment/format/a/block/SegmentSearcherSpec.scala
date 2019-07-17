//package swaydb.core.segment.format.a.block
//
//import org.scalamock.scalatest.MockFactory
//import swaydb.core.CommonAssertions._
//import swaydb.core.RunThis._
//import swaydb.core.TestBase
//import swaydb.core.TestData._
//import swaydb.core.data.Transient
//import swaydb.data.order.KeyOrder
//import swaydb.data.slice.Slice
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//class SegmentSearcherSpec extends TestBase with MockFactory {
//  implicit val order = KeyOrder.default
//
//  "search" should {
//    "not invoke binary search index if hashIndex is perfect and segment contains no ranges" in {
//
//      def runTest(_keyValues: Slice[Transient]) = {
//        val keyValues =
//          _keyValues.updateStats(
//            binarySearchIndexConfig =
//              BinarySearchIndexBlock.Config(
//                enabled = true,
//                minimumNumberOfKeys = 0,
//                fullIndex = randomBoolean(),
//                blockIO = _ => randomIOAccess(),
//                compressions = _ => randomCompressionsOrEmpty()
//              ),
//            hashIndexConfig =
//              HashIndexBlock.Config(
//                maxProbe = 100,
//                minimumNumberOfKeys = 0,
//                minimumNumberOfHits = 0,
//                allocateSpace = _.requiredSpace * 10,
//                blockIO = _ => randomIOAccess(),
//                compressions = _ => randomCompressionsLZ4OrSnappy()
//              )
//          )
//
//        val blocks = getBlocks(keyValues).get
//        blocks.hashIndexReader shouldBe defined
//        blocks.hashIndexReader.get.block.miss shouldBe 0
//        blocks.hashIndexReader.get.block.hit shouldBe keyValues.last.stats.segmentUniqueKeysCount
//
//        keyValues foreach {
//          keyValue =>
//            SegmentSearcher.search(
//              key = keyValue.minKey,
//              start = None,
//              end = None,
//              hashIndexReader = blocks.hashIndexReader,
//              binarySearchIndexReader = null,
//              sortedIndexReader = blocks.sortedIndexReader,
//              valuesReaderReader = blocks.valuesReader,
//              hasRange = keyValues.last.stats.segmentHasRange
//            ).get shouldBe keyValue
//        }
//
//
//        //check keys that do not exist return none. This will also not hit binary search index since HashIndex is perfect.
//        (1000000 to 1000000 + 100) foreach {
//          key =>
//            SegmentSearcher.search(
//              key = key,
//              start = None,
//              end = None,
//              hashIndexReader = blocks.hashIndexReader,
//              binarySearchIndexReader = null,
//              sortedIndexReader = blocks.sortedIndexReader,
//              valuesReaderReader = blocks.valuesReader,
//              hasRange = keyValues.last.stats.segmentHasRange
//            ).get shouldBe empty
//        }
//      }
//
//      runThis(100.times) {
//        val keyValues = randomizedKeyValues(startId = Some(1), count = 1000, addRandomRanges = false)
//        if (keyValues.nonEmpty) runTest(keyValues)
//      }
//    }
//  }
//}
