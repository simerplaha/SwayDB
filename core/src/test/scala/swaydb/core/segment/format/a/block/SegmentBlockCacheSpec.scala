package swaydb.core.segment.format.a.block

import java.util.concurrent.ConcurrentLinkedQueue

import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.TestSweeper.level0PushDownPool
import swaydb.core.actor.MemorySweeper
import swaydb.core.data.Transient
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.{TestBase, TestTimer}
import swaydb.data.config.{ActorConfig, MemoryCache}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import org.scalatest.OptionValues._
import swaydb.data.util.StorageUnits._
import scala.concurrent.duration._

import scala.collection.JavaConverters._
import scala.util.Random

class SegmentBlockCacheSpec extends TestBase {
  implicit val order = KeyOrder.default
  implicit val timer: TestTimer = TestTimer.Empty
  implicit val sweeper: Option[MemorySweeper.Block] = None

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
              () => segmentBlockReader add blockCache.createSegmentBlockReader().runRandomIO.right.value,
              () => sortedIndexReader add blockCache.createSortedIndexReader().runRandomIO.right.value,
              () => blockCache.createBinarySearchIndexReader().runRandomIO.right.value.foreach(reader => binarySearchIndexReader.add(reader)),
              () => blockCache.createBloomFilterReader().runRandomIO.right.value.foreach(reader => bloomFilterReader.add(reader)),
              () => blockCache.createHashIndexReader().runRandomIO.right.value.foreach(reader => hashIndexReader.add(reader)),
              () => blockCache.createValuesReader().runRandomIO.right.value.foreach(reader => valuesReader.add(reader)),
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

  "clear" should {
    "none all cached" in {
      val keyValues = Slice(Transient.put(1, 1))
      val blockCache = getSegmentBlockCache(keyValues)
      blockCache.isCached shouldBe false

      val readers: Seq[() => Option[Object]] =
        Seq(
          () => Some(blockCache.getFooter().runRandomIO.get),
          () => Some(blockCache.createSegmentBlockReader().runRandomIO.right.value),
          () => Some(blockCache.createSortedIndexReader().runRandomIO.right.value),
          () => blockCache.createBinarySearchIndexReader().runRandomIO.right.value,
          () => blockCache.createBloomFilterReader().runRandomIO.right.value,
          () => blockCache.createHashIndexReader().runRandomIO.right.value,
          () => blockCache.createValuesReader().runRandomIO.right.value
        )

      runThis(100.times) {

        //this will randomly run some readers and ignore some.
        Random.shuffle(readers) takeWhile {
          reader =>
            reader().isEmpty
        }

        blockCache.isCached shouldBe true
        blockCache.clear()
        blockCache.isCached shouldBe false
      }
    }
  }

  "it" should {
    "not add un-cached blocks and readers to memory sweeper" in {
      runThis(10.times) {
        implicit val sweeper: Option[MemorySweeper.Block] =
          MemorySweeper(MemoryCache.ByteCacheOnly(4098, 600.mb, ActorConfig.random()(level0PushDownPool)))
            .map(_.asInstanceOf[MemorySweeper.Block])

        val actor = sweeper.value.actor.value

        def assertSweeperIsEmpty(delay: FiniteDuration = 100.milliseconds) = {
          sleep(delay)
          actor.messageCount shouldBe 0
        }

        assertSweeperIsEmpty()

        val segmentIO =
          SegmentIO(
            segmentBlockIO = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false),
            hashIndexBlockIO = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false),
            bloomFilterBlockIO = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false),
            binarySearchIndexBlockIO = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false),
            sortedIndexBlockIO = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false),
            valuesBlockIO = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false),
            segmentFooterBlockIO = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false)
          )

        //initialise block cache
        val keyValues = randomizedKeyValues(100, startId = Some(1))
        val blockCache = getSegmentBlockCache(keyValues, segmentIO)
        blockCache.isCached shouldBe false

        //nothing gets added to the sweeper
        assertSweeperIsEmpty()

        //assert fetching blocks does populate the sweeper
        blockCache.getFooter()
        assertSweeperIsEmpty()

        blockCache.getBinarySearchIndex()
        assertSweeperIsEmpty()

        blockCache.getHashIndex()
        assertSweeperIsEmpty()

        blockCache.getBloomFilter()
        assertSweeperIsEmpty()

        blockCache.getSortedIndex()
        assertSweeperIsEmpty()

        blockCache.getValues()
        assertSweeperIsEmpty()

        //assert fetching readers does populate the sweeper
        blockCache.createValuesReader()
        assertSweeperIsEmpty()

        blockCache.createSortedIndexReader()
        assertSweeperIsEmpty()

        blockCache.createBinarySearchIndexReader()
        assertSweeperIsEmpty()

        blockCache.createHashIndexReader()
        assertSweeperIsEmpty()

        blockCache.createBloomFilterReader()
        assertSweeperIsEmpty()

        blockCache.createSegmentBlockReader()
        assertSweeperIsEmpty()
      }
    }

    "add cached blocks to memory sweeper" in {
      runThis(10.times) {
        implicit val sweeper: Option[MemorySweeper.Block] =
          MemorySweeper(MemoryCache.ByteCacheOnly(4098, 600.mb, ActorConfig.random(10.seconds)(level0PushDownPool)))
            .map(_.asInstanceOf[MemorySweeper.Block])

        val actor = sweeper.value.actor.value

        def assertSweeperActorSize(expectedSize: Int) = {
          println(s"expected message count: $expectedSize. Current actor totalWeight: ${actor.totalWeight}")
          sleep(100.milliseconds)
          actor.messageCount shouldBe expectedSize
        }

        assertSweeperActorSize(0)

        val segmentIO =
          SegmentIO(
            segmentBlockIO = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true),
            hashIndexBlockIO = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true),
            bloomFilterBlockIO = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true),
            binarySearchIndexBlockIO = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true),
            sortedIndexBlockIO = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true),
            valuesBlockIO = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true),
            segmentFooterBlockIO = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true)
          )

        //initialise block cache
        val keyValues = randomizedKeyValues(100, startId = Some(1))
        val blockCache = getSegmentBlockCache(keyValues, segmentIO)
        blockCache.isCached shouldBe false

        //number of messages expected sweeper's Actor.
        var expectedMessageCount = 0

        //nothing gets added to the sweeper
        assertSweeperActorSize(0)

        //assert fetching blocks does populate the sweeper
        blockCache.getFooter()
        expectedMessageCount = 2 //SegmentReader and Footer blockInfo gets added
        assertSweeperActorSize(expectedMessageCount)

        //binarySearchIndex and others blocks that are optional can be None
        blockCache.getBinarySearchIndex() foreach { _ => expectedMessageCount += 1 }
        assertSweeperActorSize(expectedMessageCount)
        (1 to randomIntMax(10)) foreach (_ => blockCache.getBinarySearchIndex()) //calling the same block multiple times does not increase the memory sweeper's size
        assertSweeperActorSize(expectedMessageCount)

        blockCache.getHashIndex() foreach { _ => expectedMessageCount += 1 }
        assertSweeperActorSize(expectedMessageCount)
        (1 to randomIntMax(10)) foreach (_ => blockCache.getHashIndex())
        assertSweeperActorSize(expectedMessageCount)

        blockCache.getBloomFilter() foreach { _ => expectedMessageCount += 1 }
        assertSweeperActorSize(expectedMessageCount)
        (1 to randomIntMax(10)) foreach (_ => blockCache.getBloomFilter())
        assertSweeperActorSize(expectedMessageCount)

        blockCache.getSortedIndex()
        expectedMessageCount += 1
        assertSweeperActorSize(expectedMessageCount)
        (1 to randomIntMax(10)) foreach (_ => blockCache.getSortedIndex())
        assertSweeperActorSize(expectedMessageCount)

        blockCache.getValues() foreach { _ => expectedMessageCount += 1 }
        assertSweeperActorSize(expectedMessageCount)
        (1 to randomIntMax(10)) foreach (_ => blockCache.getValues())
        assertSweeperActorSize(expectedMessageCount)


        //assert fetching readers does populate the sweeper
        //segment block read was already read above when fetching Footer and other blocks info so messages should not increase.
        blockCache.createSegmentBlockReader()
        assertSweeperActorSize(expectedMessageCount)
        (1 to randomIntMax(10)) foreach (_ => blockCache.createSegmentBlockReader())
        assertSweeperActorSize(expectedMessageCount)

        blockCache.createHashIndexReader() foreach { _ => expectedMessageCount += 1 }
        assertSweeperActorSize(expectedMessageCount)
        (1 to randomIntMax(10)) foreach (_ => blockCache.createHashIndexReader())
        assertSweeperActorSize(expectedMessageCount)

        blockCache.createBinarySearchIndexReader() foreach { _ => expectedMessageCount += 1 }
        assertSweeperActorSize(expectedMessageCount)
        (1 to randomIntMax(10)) foreach (_ => blockCache.createBinarySearchIndexReader())
        assertSweeperActorSize(expectedMessageCount)

        blockCache.createBloomFilterReader() foreach { _ => expectedMessageCount += 1 }
        assertSweeperActorSize(expectedMessageCount)
        (1 to randomIntMax(10)) foreach (_ => blockCache.createBloomFilterReader())
        assertSweeperActorSize(expectedMessageCount)

        blockCache.createSortedIndexReader()
        expectedMessageCount += 1
        assertSweeperActorSize(expectedMessageCount)
        (1 to randomIntMax(10)) foreach (_ => blockCache.createSortedIndexReader())
        assertSweeperActorSize(expectedMessageCount)

        blockCache.createValuesReader() foreach { _ => expectedMessageCount += 1 }
        assertSweeperActorSize(expectedMessageCount)
        (1 to randomIntMax(10)) foreach (_ => blockCache.createValuesReader())
        assertSweeperActorSize(expectedMessageCount)
      }
    }
  }
}
