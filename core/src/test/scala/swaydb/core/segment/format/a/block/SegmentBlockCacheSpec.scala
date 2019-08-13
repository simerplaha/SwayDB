package swaydb.core.segment.format.a.block


import java.util.concurrent.ConcurrentLinkedQueue

import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Transient
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.{TestBase, TestTimer}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.JavaConverters._

class SegmentBlockCacheSpec extends TestBase {
  implicit val order = KeyOrder.default
  implicit val timer: TestTimer = TestTimer.Empty

  /**
   * Running this test with [[SegmentBlockCache.segmentIOStrategyCache]]'s stored set to false will
   * result is offset conflicts. Segment's [[swaydb.data.config.IOStrategy]] should be fixed ones read.
   */

  "it" should {
    "return distinct Readers" in {
      runThis(1000.times, log = true) {
        val keyValues = Slice(Transient.put(1, 1))
        val blockCache = getSegmentBlockCache(keyValues)
        blockCache.isCached shouldBe false

        val segmentBlockReader = new ConcurrentLinkedQueue[UnblockedReader[_, _]]()
        val sortedIndexReader = new ConcurrentLinkedQueue[UnblockedReader[_, _]]()
        val binarySearchIndexReader = new ConcurrentLinkedQueue[UnblockedReader[_, _]]()
        val bloomFilterReader = new ConcurrentLinkedQueue[UnblockedReader[_, _]]()
        val hashIndexReader = new ConcurrentLinkedQueue[UnblockedReader[_, _]]()
        val valuesReader = new ConcurrentLinkedQueue[UnblockedReader[_, _]]()

        (1 to 1000).par foreach {
          _ =>
            Seq(
              () => blockCache.getFooter().runRandomIO.get,
              () => segmentBlockReader add blockCache.createSegmentBlockReader().runRandomIO.value,
              () => sortedIndexReader add blockCache.createSortedIndexReader().runRandomIO.value,
              () => blockCache.createBinarySearchIndexReader().runRandomIO.value.foreach(reader => binarySearchIndexReader.add(reader)),
              () => blockCache.createBloomFilterReader().runRandomIO.value.foreach(reader => bloomFilterReader.add(reader)),
              () => blockCache.createHashIndexReader().runRandomIO.value.foreach(reader => hashIndexReader.add(reader)),
              () => blockCache.createValuesReader().runRandomIO.value.foreach(reader => valuesReader.add(reader)),
              () => eitherOne(blockCache.clear(), ())
            ).runThisRandomlyInParallel
        }

        segmentBlockReader.asScala.toList.distinct.size shouldBe segmentBlockReader.size
        sortedIndexReader.asScala.toList.distinct.size shouldBe sortedIndexReader.size
        binarySearchIndexReader.asScala.toList.distinct.size shouldBe binarySearchIndexReader.size
        bloomFilterReader.asScala.toList.distinct.size shouldBe bloomFilterReader.size
        hashIndexReader.asScala.toList.distinct.size shouldBe hashIndexReader.size
        valuesReader.asScala.toList.distinct.size shouldBe valuesReader.size
      }
    }
  }
}
