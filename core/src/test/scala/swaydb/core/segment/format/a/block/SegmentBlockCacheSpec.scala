//package swaydb.core.segment.format.a.block
//
//import java.util.concurrent.ConcurrentLinkedQueue
//
//import org.scalatest.OptionValues._
//import swaydb.IOValues._
//import swaydb.core.CommonAssertions._
//import swaydb.core.RunThis._
//import swaydb.core.TestData._
//import swaydb.core.TestSweeper.level0PushDownPool
//import swaydb.core.actor.MemorySweeper
//import swaydb.core.data.Memory
//import swaydb.core.segment.PersistentSegment
//import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
//import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
//import swaydb.core.segment.format.a.block.reader.UnblockedReader
//import swaydb.core.{TestBase, TestTimer}
//import swaydb.data.config.{ActorConfig, IOAction, MemoryCache}
//import swaydb.data.order.KeyOrder
//import swaydb.data.slice.Slice
//import swaydb.data.util.StorageUnits._
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//import scala.jdk.CollectionConverters._
//import scala.concurrent.duration._
//import scala.util.Random
//import scala.collection.parallel.CollectionConverters._
//
//class SegmentBlockCacheSpec extends TestBase {
//  implicit val order = KeyOrder.default
//  implicit val timer: TestTimer = TestTimer.Empty
//  implicit val sweeper: Option[MemorySweeper.Block] = None
//
//  /**
//   * Running this test with [[SegmentBlockCache.segmentIOStrategyCache]]'s stored set to false will
//   * result is offset conflicts. Segment's [[swaydb.data.config.IOStrategy]] should be fixed ones read.
//   */
//
//  "it" should {
//    "return distinct Readers" in {
//      runThis(10.times, log = true) {
//        val keyValues = Slice(Memory.put(1, 1))
//        val blockCache = getSegmentBlockCacheSingle(keyValues)
//        blockCache.isCached shouldBe false
//
//        val segmentBlockReader = new ConcurrentLinkedQueue[UnblockedReader[_, _]]()
//        val sortedIndexReader = new ConcurrentLinkedQueue[UnblockedReader[_, _]]()
//        val binarySearchIndexReader = new ConcurrentLinkedQueue[UnblockedReader[_, _]]()
//        val bloomFilterReader = new ConcurrentLinkedQueue[UnblockedReader[_, _]]()
//        val hashIndexReader = new ConcurrentLinkedQueue[UnblockedReader[_, _]]()
//        val valuesReader = new ConcurrentLinkedQueue[UnblockedReader[_, _]]()
//
//        (1 to 1000).par foreach {
//          _ =>
//            Seq(
//              () => blockCache.getFooter().runRandomIO.get,
//              () => segmentBlockReader add blockCache.createSegmentBlockReader().runRandomIO.right.value,
//              () => sortedIndexReader add blockCache.createSortedIndexReader().runRandomIO.right.value,
//              () => blockCache.createBinarySearchIndexReaderNullable().runRandomIO.right.value.foreach(reader => binarySearchIndexReader.add(reader)),
//              () => blockCache.createBloomFilterReaderNullable().runRandomIO.right.value.foreach(reader => bloomFilterReader.add(reader)),
//              () => blockCache.createHashIndexReaderNullable().runRandomIO.right.value.foreach(reader => hashIndexReader.add(reader)),
//              () => blockCache.createValuesReaderNullable().runRandomIO.right.value.foreach(reader => valuesReader.add(reader)),
//              () => eitherOne(blockCache.clear(), ())
//            ).runThisRandomlyInParallel
//        }
//
//        segmentBlockReader.asScala.toList.distinct.size shouldBe segmentBlockReader.size
//        sortedIndexReader.asScala.toList.distinct.size shouldBe sortedIndexReader.size
//        binarySearchIndexReader.asScala.toList.distinct.size shouldBe binarySearchIndexReader.size
//        bloomFilterReader.asScala.toList.distinct.size shouldBe bloomFilterReader.size
//        hashIndexReader.asScala.toList.distinct.size shouldBe hashIndexReader.size
//        valuesReader.asScala.toList.distinct.size shouldBe valuesReader.size
//      }
//    }
//  }
//
//  "clear" should {
//    "none all cached" in {
//      val keyValues = Slice(Memory.put(1, 1))
//      val blockCache = getSegmentBlockCacheSingle(keyValues)
//      blockCache.isCached shouldBe false
//
//      val readers: Seq[() => Option[Object]] =
//        Seq(
//          () => Some(blockCache.getFooter().runRandomIO.get),
//          () => Some(blockCache.createSegmentBlockReader().runRandomIO.right.value),
//          () => Some(blockCache.createSortedIndexReader().runRandomIO.right.value),
//          () => blockCache.createBinarySearchIndexReaderNullable().runRandomIO.right.value,
//          () => blockCache.createBloomFilterReaderNullable().runRandomIO.right.value,
//          () => blockCache.createHashIndexReaderNullable().runRandomIO.right.value,
//          () => blockCache.createValuesReaderNullable().runRandomIO.right.value
//        )
//
//      runThis(100.times) {
//
//        //this will randomly run some readers and ignore some.
//        Random.shuffle(readers) takeWhile {
//          reader =>
//            reader().isEmpty
//        }
//
//        blockCache.isCached shouldBe true
//        blockCache.clear()
//        blockCache.isCached shouldBe false
//      }
//    }
//  }
//
//  "it" should {
//    "not add un-cached blocks and readers to memory sweeper" in {
//      runThis(10.times) {
//        implicit val sweeper: Option[MemorySweeper.Block] =
//          MemorySweeper(MemoryCache.ByteCacheOnly(4098, 50000.bytes, 600.mb, ActorConfig.random()(level0PushDownPool)))
//            .map(_.asInstanceOf[MemorySweeper.Block])
//
//        val actor = sweeper.value.actor.value
//
//        def assertSweeperIsEmpty(delay: FiniteDuration = 100.milliseconds) = {
//          sleep(delay)
//          actor.messageCount shouldBe 0
//        }
//
//        assertSweeperIsEmpty()
//
//        //initialise block cache
//        val keyValues = randomizedKeyValues(100, startId = Some(1))
//        val blockCache =
//          getSegmentBlockCacheSingle(
//            keyValues = keyValues,
//            valuesConfig = ValuesBlock.Config.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false)),
//            sortedIndexConfig = SortedIndexBlock.Config.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false)),
//            binarySearchIndexConfig = BinarySearchIndexBlock.Config.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false)),
//            hashIndexConfig = HashIndexBlock.Config.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false)),
//            bloomFilterConfig = BloomFilterBlock.Config.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false)),
//            segmentConfig = new SegmentBlock.Config(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false), _ => randomCompressions())
//          )
//
//        blockCache.isCached shouldBe false
//
//        //nothing gets added to the sweeper
//        assertSweeperIsEmpty()
//
//        //assert fetching blocks does populate the sweeper
//        blockCache.getFooter()
//        assertSweeperIsEmpty()
//
//        blockCache.getBinarySearchIndex()
//        assertSweeperIsEmpty()
//
//        blockCache.getHashIndex()
//        assertSweeperIsEmpty()
//
//        blockCache.getBloomFilter()
//        assertSweeperIsEmpty()
//
//        blockCache.getSortedIndex()
//        assertSweeperIsEmpty()
//
//        blockCache.getValues()
//        assertSweeperIsEmpty()
//
//        //assert fetching readers does populate the sweeper
//        blockCache.createValuesReaderNullable()
//        assertSweeperIsEmpty()
//
//        blockCache.createSortedIndexReader()
//        assertSweeperIsEmpty()
//
//        blockCache.createBinarySearchIndexReaderNullable()
//        assertSweeperIsEmpty()
//
//        blockCache.createHashIndexReaderNullable()
//        assertSweeperIsEmpty()
//
//        blockCache.createBloomFilterReaderNullable()
//        assertSweeperIsEmpty()
//
//        blockCache.createSegmentBlockReader()
//        assertSweeperIsEmpty()
//      }
//    }
//
//    "add cached blocks to memory sweeper" in {
//      runThis(10.times) {
//        implicit val sweeper: Option[MemorySweeper.Block] =
//          MemorySweeper(MemoryCache.ByteCacheOnly(4098, 50000.bytes, 600.mb, ActorConfig.random(10.seconds)(level0PushDownPool)))
//            .map(_.asInstanceOf[MemorySweeper.Block])
//
//        val actor = sweeper.value.actor.value
//
//        def assertSweeperActorSize(expectedSize: Int) = {
//          println(s"expected message count: $expectedSize. Current actor totalWeight: ${actor.totalWeight}")
//          sleep(100.milliseconds)
//          actor.messageCount shouldBe expectedSize
//        }
//
//        assertSweeperActorSize(0)
//
//        //initialise block cache
//        val keyValues = randomizedKeyValues(100, startId = Some(1))
//        val blockCache =
//          getSegmentBlockCacheSingle(
//            keyValues,
//            valuesConfig = ValuesBlock.Config.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true)),
//            sortedIndexConfig = SortedIndexBlock.Config.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true)),
//            binarySearchIndexConfig = BinarySearchIndexBlock.Config.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true)),
//            hashIndexConfig = HashIndexBlock.Config.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true)),
//            bloomFilterConfig = BloomFilterBlock.Config.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true)),
//            segmentConfig = new SegmentBlock.Config(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true), _ => randomCompressions())
//          )
//        blockCache.isCached shouldBe false
//
//        //number of messages expected sweeper's Actor.
//        var expectedMessageCount = 0
//
//        //nothing gets added to the sweeper
//        assertSweeperActorSize(0)
//
//        //assert fetching blocks does populate the sweeper
//        blockCache.getFooter()
//        expectedMessageCount = 2 //SegmentReader and Footer blockInfo gets added
//        assertSweeperActorSize(expectedMessageCount)
//
//        //binarySearchIndex and others blocks that are optional can be None
//        blockCache.getBinarySearchIndex() foreach { _ => expectedMessageCount += 1 }
//        assertSweeperActorSize(expectedMessageCount)
//        (1 to randomIntMax(10)) foreach (_ => blockCache.getBinarySearchIndex()) //calling the same block multiple times does not increase the memory sweeper's size
//        assertSweeperActorSize(expectedMessageCount)
//
//        blockCache.getHashIndex() foreach { _ => expectedMessageCount += 1 }
//        assertSweeperActorSize(expectedMessageCount)
//        (1 to randomIntMax(10)) foreach (_ => blockCache.getHashIndex())
//        assertSweeperActorSize(expectedMessageCount)
//
//        blockCache.getBloomFilter() foreach { _ => expectedMessageCount += 1 }
//        assertSweeperActorSize(expectedMessageCount)
//        (1 to randomIntMax(10)) foreach (_ => blockCache.getBloomFilter())
//        assertSweeperActorSize(expectedMessageCount)
//
//        blockCache.getSortedIndex()
//        expectedMessageCount += 1
//        assertSweeperActorSize(expectedMessageCount)
//        (1 to randomIntMax(10)) foreach (_ => blockCache.getSortedIndex())
//        assertSweeperActorSize(expectedMessageCount)
//
//        blockCache.getValues() foreach { _ => expectedMessageCount += 1 }
//        assertSweeperActorSize(expectedMessageCount)
//        (1 to randomIntMax(10)) foreach (_ => blockCache.getValues())
//        assertSweeperActorSize(expectedMessageCount)
//
//
//        //assert fetching readers does populate the sweeper
//        //segment block read was already read above when fetching Footer and other blocks info so messages should not increase.
//        blockCache.createSegmentBlockReader()
//        assertSweeperActorSize(expectedMessageCount)
//        (1 to randomIntMax(10)) foreach (_ => blockCache.createSegmentBlockReader())
//        assertSweeperActorSize(expectedMessageCount)
//
//        blockCache.createHashIndexReaderNullable() foreach { _ => expectedMessageCount += 1 }
//        assertSweeperActorSize(expectedMessageCount)
//        (1 to randomIntMax(10)) foreach (_ => blockCache.createHashIndexReaderNullable())
//        assertSweeperActorSize(expectedMessageCount)
//
//        blockCache.createBinarySearchIndexReaderNullable() foreach { _ => expectedMessageCount += 1 }
//        assertSweeperActorSize(expectedMessageCount)
//        (1 to randomIntMax(10)) foreach (_ => blockCache.createBinarySearchIndexReaderNullable())
//        assertSweeperActorSize(expectedMessageCount)
//
//        blockCache.createBloomFilterReaderNullable() foreach { _ => expectedMessageCount += 1 }
//        assertSweeperActorSize(expectedMessageCount)
//        (1 to randomIntMax(10)) foreach (_ => blockCache.createBloomFilterReaderNullable())
//        assertSweeperActorSize(expectedMessageCount)
//
//        blockCache.createSortedIndexReader()
//        expectedMessageCount += 1
//        assertSweeperActorSize(expectedMessageCount)
//        (1 to randomIntMax(10)) foreach (_ => blockCache.createSortedIndexReader())
//        assertSweeperActorSize(expectedMessageCount)
//
//        blockCache.createValuesReaderNullable() foreach { _ => expectedMessageCount += 1 }
//        assertSweeperActorSize(expectedMessageCount)
//        (1 to randomIntMax(10)) foreach (_ => blockCache.createValuesReaderNullable())
//        assertSweeperActorSize(expectedMessageCount)
//      }
//    }
//  }
//
//  "it" should {
//    "cache sortedIndex and values blocks on readAll" in {
//      runThis(10.times) {
//        //ensure Segment itself is not caching bytes.
//        val segmentIOStrategy = randomIOStrategy(cacheOnAccess = false)
//        val sortedIndexIOStrategy = randomIOStrategy(cacheOnAccess = false)
//        val valuesIOStrategy = randomIOStrategy(cacheOnAccess = false)
//        //for testing that HashIndex does not get cache if false
//        val hashIndexBlockIOStrategy = randomIOStrategy(cacheOnAccess = false)
//
//        val oldSortedIndexBlockIO = (_: IOAction) => sortedIndexIOStrategy
//        val oldValuesBlockIO = (_: IOAction) => valuesIOStrategy
//
//        //disable compression so bytes do not get cached
//        val keyValues =
//          randomizedKeyValues(randomIntMax(100) max 1)
//
//        implicit val segmentIO =
//          SegmentIO(
//            segmentBlockIO = _ => segmentIOStrategy,
//            hashIndexBlockIO = _ => hashIndexBlockIOStrategy,
//            bloomFilterBlockIO = _ => randomIOStrategy(),
//            binarySearchIndexBlockIO = _ => randomIOStrategy(),
//            sortedIndexBlockIO = oldSortedIndexBlockIO,
//            valuesBlockIO = oldValuesBlockIO,
//            segmentFooterBlockIO = _ => randomIOStrategy()
//          )
//
//        val segment = TestSegment(keyValues, segmentConfig = SegmentBlock.Config.random(false)).asInstanceOf[PersistentSegment]
//        val blockCache = segment.segmentCache.blockCache
//
//        def assertIsCached() = {
//          blockCache.createSortedIndexReader().isFile shouldBe false
//          blockCache.createValuesReaderNullable().foreach(_.isFile shouldBe false)
//          blockCache.createHashIndexReaderNullable().foreach(_.isFile shouldBe true)
//        }
//
//        def assertIsNotCached() = {
//          blockCache.createSortedIndexReader().isFile shouldBe true
//          blockCache.createValuesReaderNullable().foreach(_.isFile shouldBe true)
//          blockCache.createHashIndexReaderNullable().foreach(_.isFile shouldBe true)
//        }
//
//        //initially they are not cached
//        assertIsNotCached()
//
//        //read all an expect sortedIndex and value bytes to get cached but not hashIndex
//        blockCache.readAll()
//
//        assertIsCached()
//
//        //clear cache
//        blockCache.clear()
//
//        //reverts back to not caching
//        assertIsNotCached()
//
//        //read all
//        blockCache.readAll()
//
//        //caches all again
//        assertIsCached()
//
//        //clear cache
//        blockCache.clear()
//
//        //no caching
//        assertIsNotCached()
//
//        segment.close
//
//      }
//    }
//  }
//}
