package swaydb.core.segment.format.a.block


import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.data.config.IOStrategy
import swaydb.data.order.KeyOrder

class SegmentBlockCacheSpec extends TestBase {
  implicit val order = KeyOrder.default

  "read blocks" in {
    //one big azz test.
    //It create a Segment with randomly generated key-values and asserts all the caches.

    runThis(100.times) {

      val binarySearchIndexCompression = randomCompressionsOrEmpty()
      val sortedIndexCompression = randomCompressionsOrEmpty()
      val keyValues =
        randomizedKeyValues(100, addPut = true, addGroups = false)
          .updateStats(
            binarySearchIndexConfig =
              BinarySearchIndexBlock.Config(
                enabled = true,
                minimumNumberOfKeys = 0,
                fullIndex = true,
                blockIO = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
                compressions = _ => binarySearchIndexCompression
              ),
            sortedIndexConfig =
              SortedIndexBlock.Config(
                blockIO = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
                prefixCompressionResetCount = 3,
                enableAccessPositionIndex = true,
                compressions = _ => sortedIndexCompression
              )
          )

      val segmentCompression = Seq.empty

      //create an open block and a closed block. SegmentBlockCache cannot be created on opened block.
      //Open block is used to assert the decompressed bytes got from closed block.
      val openSegment: SegmentBlock.Open = SegmentBlock.writeOpen(keyValues, Int.MaxValue, SegmentBlock.Config.random).get
      val closedSegment: SegmentBlock.Closed = SegmentBlock.writeClosed(keyValues, Int.MaxValue, SegmentBlock.Config.random).get

      //closedSegments are compressed and the sizes will not match
      //      if (segmentCompression.isEmpty)
      //        openSegment.segmentSize shouldBe closedSegment.segmentSize
      //      else
      //        closedSegment.segmentBytes should have size 1 //compressed bytes.

      //but both will not have the same header bytes because openSegment is not compressed and does not have compression info.
      openSegment.headerBytes.head shouldBe closedSegment.segmentBytes.head.head

      openSegment.headerBytes.isOriginalFullSlice shouldBe true //bytes should be full.
      closedSegment.segmentBytes.head.isFull shouldBe true //bytes should be full.

      //initialise a block cache and run asserts
      val segmentBlockCache = getSegmentBlockCacheFromSegmentClosed(closedSegment)
      segmentBlockCache.readAll().get shouldBe keyValues

      /**
       * One by one fetch everything from [[SegmentBlockCache]] and assert.
       */
      //on init, nothing is cached.
      //      segmentBlockCache.isCached shouldBe false

      //      //getSegmentBlock
      //      segmentBlockCache.segmentBlockReaderCache.isCached shouldBe false
      //      val segmentBlock = segmentBlockCache.getSegmentBlock().get
      //      segmentBlockCache.segmentBlockCache.isCached shouldBe true
      //      segmentBlock.compressionInfo.isDefined shouldBe segmentCompression.nonEmpty //segment is not closed.
      //      segmentBlock.headerSize shouldBe openSegment.headerBytes.size
      //      //read the entire segment from blockedSegment. Compressed or uncompressed it should result in the same bytes as original.
      //      segmentBlockCache.createSegmentBlockReader().get.readRemaining().get shouldBe openSegment.segmentBytes.dropHead().flatten.toSlice
      //      segmentBlockCache.segmentBlockCache.isCached shouldBe true
      //      segmentBlockCache.segmentBlockReaderCache.isCached shouldBe segmentCompression.nonEmpty
      //
      //      segmentBlockCache.footerBlockCache.isCached shouldBe false
      //      val footer = segmentBlockCache.getFooter().get
      //      segmentBlockCache.footerBlockCache.isCached shouldBe true
      //      footer.keyValueCount shouldBe keyValues.size
      //      footer.createdInLevel shouldBe Int.MaxValue


      //      val binarySearchIndexBytes = segmentBlockCache.createBinarySearchIndexReader().get.get.readRemaining()
      //
      //      println(binarySearchIndexBytes)

      //      keyValues foreach {
      //        keyValue =>
      //          BinarySearchIndex.search(
      //            key = keyValue.minKey,
      //            start = None,
      //            end = None,
      //            binarySearchIndexReader = segmentBlockCache.createBinarySearchIndexReader().get.get,
      //            sortedIndexReader = segmentBlockCache.createSortedIndexReader().get,
      //            valuesReader = segmentBlockCache.createValuesReader().get
      //          ).get shouldBe keyValue
      //      }
    }
  }
}
