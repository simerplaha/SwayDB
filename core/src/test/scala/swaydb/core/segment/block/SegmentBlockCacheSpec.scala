package swaydb.core.segment.block

import org.scalatest.OptionValues._
import swaydb.ActorConfig
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.core.data.Memory
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
import swaydb.core.segment.block.bloomfilter.{BloomFilterBlock, BloomFilterBlockConfig}
import swaydb.core.segment.block.hashindex.{HashIndexBlock, HashIndexBlockConfig}
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.segment.{SegmentBlock, SegmentBlockCache, SegmentBlockConfig}
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockConfig}
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockConfig}
import swaydb.core.segment.{PersistentSegment, PersistentSegmentMany, PersistentSegmentOne}
import swaydb.core.sweeper.MemorySweeper
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestTimer}
import swaydb.data.config.MemoryCache
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.effect.IOStrategy
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._
import swaydb.utils.StorageUnits._

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Random

class SegmentBlockCacheSpec extends TestBase {
  implicit val order = KeyOrder.default
  implicit val timer: TestTimer = TestTimer.Empty
  implicit val sweeper: Option[MemorySweeper.Block] = None

  /**
   * Running this test with [[SegmentBlockCache.segmentIOStrategyCache]]'s stored set to false will
   * result is offset conflicts. Segment's [[IOStrategy]] should be fixed ones read.
   */

  "it" should {
    "return distinct Readers" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            val keyValues = Slice(Memory.put(1, 1))
            val segmentConfig = SegmentBlockConfig.random
            val blockCache = getSegmentBlockCacheSingle(keyValues, segmentConfig = segmentConfig)
            blockCache.isCached shouldBe segmentConfig.cacheBlocksOnCreate

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
                  () => Option(blockCache.createBinarySearchIndexReaderOrNull().runRandomIO.right.value).foreach(reader => binarySearchIndexReader.add(reader)),
                  () => Option(blockCache.createBloomFilterReaderOrNull().runRandomIO.right.value).foreach(reader => bloomFilterReader.add(reader)),
                  () => Option(blockCache.createHashIndexReaderOrNull().runRandomIO.right.value).foreach(reader => hashIndexReader.add(reader)),
                  () => Option(blockCache.createValuesReaderOrNull().runRandomIO.right.value).foreach(reader => valuesReader.add(reader)),
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

  "clear" should {
    "none all cached" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val keyValues = Slice(Memory.put(1, 1))
          val segmentConfig = SegmentBlockConfig.random
          val blockCache = getSegmentBlockCacheSingle(keyValues, segmentConfig = segmentConfig)
          blockCache.isCached shouldBe segmentConfig.cacheBlocksOnCreate

          val readers: Seq[() => Object] =
            Seq(
              () => blockCache.getFooter().runRandomIO.get,
              () => blockCache.createSegmentBlockReader().runRandomIO.right.value,
              () => blockCache.createSortedIndexReader().runRandomIO.right.value,
              () => blockCache.createBinarySearchIndexReaderOrNull().runRandomIO.right.value,
              () => blockCache.createBloomFilterReaderOrNull().runRandomIO.right.value,
              () => blockCache.createHashIndexReaderOrNull().runRandomIO.right.value,
              () => blockCache.createValuesReaderOrNull().runRandomIO.right.value
            )

          runThis(100.times) {

            //this will randomly run some readers and ignore some.
            Random.shuffle(readers) takeWhile {
              reader =>
                reader() == null
            }

            blockCache.isCached shouldBe true
            blockCache.clear()
            blockCache.isCached shouldBe false
          }
      }
    }
  }

  "it" should {
    "not add un-cached blocks and readers to memory sweeper" in {
      runThis(10.times) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            val memorySweeper = MemorySweeper(MemoryCache.ByteCacheOnly(4096, 50000.bytes, 600.mb, false, ActorConfig.random()(TestExecutionContext.executionContext)))
              .map(_.asInstanceOf[MemorySweeper.Block].sweep())

            val actor = memorySweeper.value.actor.value

            def assertSweeperIsEmpty(delay: FiniteDuration = 100.milliseconds) = {
              sleep(delay)
              actor.messageCount shouldBe 0
            }

            assertSweeperIsEmpty()

            //initialise block cache
            val keyValues = randomizedKeyValues(100, startId = Some(1))
            val segmentConfig =
              SegmentBlockConfig.random2(
                blockIOStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false),
                cacheBlocksOnCreate = false,
                compressions = _ => randomCompressions()
              )

            val blockCache =
              getSegmentBlockCacheSingle(
                keyValues = keyValues,
                valuesConfig = ValuesBlockConfig.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false)),
                sortedIndexConfig = SortedIndexBlockConfig.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false)),
                binarySearchIndexConfig = BinarySearchIndexBlockConfig.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false)),
                hashIndexConfig = HashIndexBlockConfig.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false)),
                bloomFilterConfig = BloomFilterBlockConfig.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = false)),
                segmentConfig = segmentConfig
              )

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
            blockCache.createValuesReaderOrNull()
            assertSweeperIsEmpty()

            blockCache.createSortedIndexReader()
            assertSweeperIsEmpty()

            blockCache.createBinarySearchIndexReaderOrNull()
            assertSweeperIsEmpty()

            blockCache.createHashIndexReaderOrNull()
            assertSweeperIsEmpty()

            blockCache.createBloomFilterReaderOrNull()
            assertSweeperIsEmpty()

            blockCache.createSegmentBlockReader()
            assertSweeperIsEmpty()
        }
      }
    }

    "add cached blocks to memory sweeper" in {
      runThis(10.times) {
        TestCaseSweeper {
          implicit sweeper =>
            implicit val blockSweeper: Option[MemorySweeper.Block] =
              MemorySweeper(MemoryCache.ByteCacheOnly(4096, 50000.bytes, 600.mb, disableForSearchIO = false, ActorConfig.random(10.seconds)(TestExecutionContext.executionContext)))
                .map(_.asInstanceOf[MemorySweeper.Block].sweep())

            val actor = blockSweeper.value.actor.value

            def assertSweeperActorSize(expectedSize: Int) = {
              println(s"expected message count: $expectedSize. Current actor totalWeight: ${actor.totalWeight}")
              sleep(100.milliseconds)
              actor.messageCount shouldBe expectedSize
            }

            assertSweeperActorSize(0)

            //initialise block cache
            val keyValues = randomizedKeyValues(100, startId = Some(1))
            val blockCache =
              getSegmentBlockCacheSingle(
                keyValues,
                valuesConfig = ValuesBlockConfig.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true)),
                sortedIndexConfig = SortedIndexBlockConfig.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true)),
                binarySearchIndexConfig = BinarySearchIndexBlockConfig.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true)),
                hashIndexConfig = HashIndexBlockConfig.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true)),
                bloomFilterConfig = BloomFilterBlockConfig.random.copy(ioStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true)),
                segmentConfig = SegmentBlockConfig.random2(blockIOStrategy = _ => randomIOStrategyWithCacheOnAccess(cacheOnAccess = true), cacheBlocksOnCreate = false, compressions = _ => randomCompressions())
              )

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

            Option(blockCache.createHashIndexReaderOrNull()) foreach { _ => expectedMessageCount += 1 }
            assertSweeperActorSize(expectedMessageCount)
            (1 to randomIntMax(10)) foreach (_ => blockCache.createHashIndexReaderOrNull())
            assertSweeperActorSize(expectedMessageCount)

            Option(blockCache.createBinarySearchIndexReaderOrNull()) foreach { _ => expectedMessageCount += 1 }
            assertSweeperActorSize(expectedMessageCount)
            (1 to randomIntMax(10)) foreach (_ => blockCache.createBinarySearchIndexReaderOrNull())
            assertSweeperActorSize(expectedMessageCount)

            Option(blockCache.createBloomFilterReaderOrNull()) foreach { _ => expectedMessageCount += 1 }
            assertSweeperActorSize(expectedMessageCount)
            (1 to randomIntMax(10)) foreach (_ => blockCache.createBloomFilterReaderOrNull())
            assertSweeperActorSize(expectedMessageCount)

            blockCache.createSortedIndexReader()
            expectedMessageCount += 1
            assertSweeperActorSize(expectedMessageCount)
            (1 to randomIntMax(10)) foreach (_ => blockCache.createSortedIndexReader())
            assertSweeperActorSize(expectedMessageCount)

            Option(blockCache.createValuesReaderOrNull()) foreach { _ => expectedMessageCount += 1 }
            assertSweeperActorSize(expectedMessageCount)
            (1 to randomIntMax(10)) foreach (_ => blockCache.createValuesReaderOrNull())
            assertSweeperActorSize(expectedMessageCount)
        }
      }
    }
  }

  "it" should {
    "cache sortedIndex and values blocks on readAll" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            //ensure Segment itself is not caching bytes.
            //disable compression so bytes do not get cached

            val keyValues =
              randomizedKeyValues(randomIntMax(100) max 1)

            val segment =
              TestSegment(
                keyValues = keyValues,
                valuesConfig = ValuesBlockConfig.random(hasCompression = false, cacheOnAccess = false),
                sortedIndexConfig = SortedIndexBlockConfig.random(hasCompression = false, cacheOnAccess = false),
                binarySearchIndexConfig = BinarySearchIndexBlockConfig.random(hasCompression = false, cacheOnAccess = false),
                hashIndexConfig = HashIndexBlockConfig.random(hasCompression = false, cacheOnAccess = false),
                bloomFilterConfig = BloomFilterBlockConfig.random(hasCompression = false, cacheOnAccess = false),
                segmentConfig = SegmentBlockConfig.random(hasCompression = false, cacheOnAccess = false, cacheBlocksOnCreate = false, mmap = mmapSegments)
              )

            val refs =
              segment match {
                case segment: PersistentSegment =>
                  segment match {
                    case segment: PersistentSegmentMany =>
                      segment.segmentRefs(randomBoolean()).toList

                    case segment: PersistentSegmentOne =>
                      List(segment.ref)
                  }
              }

            refs.size should be >= 1

            refs foreach {
              ref =>
                val blockCache = ref.segmentBlockCache

                def assertIsCached() = {
                  blockCache.createSortedIndexReader().isFile shouldBe false
                  Option(blockCache.createValuesReaderOrNull()).foreach(_.isFile shouldBe false)
                  Option(blockCache.createHashIndexReaderOrNull()).foreach(_.isFile shouldBe true)
                }

                def assertIsNotCached() = {
                  blockCache.createSortedIndexReader().isFile shouldBe true
                  Option(blockCache.createValuesReaderOrNull()).foreach(_.isFile shouldBe true)
                  Option(blockCache.createHashIndexReaderOrNull()).foreach(_.isFile shouldBe true)
                }

                //initially they are not cached
                assertIsNotCached()

                //read all an expect sortedIndex and value bytes to get cached but not hashIndex
                blockCache.iterator(true).foreach(_ => ())

                assertIsCached()

                //clear cache
                blockCache.clear()

                //reverts back to not caching
                assertIsNotCached()

                //read all
                blockCache.iterator(true).foreach(_ => ())

                //caches all again
                assertIsCached()

                //clear cache
                blockCache.clear()

                //no caching
                assertIsNotCached()

                segment.close
            }
        }
      }
    }
  }
}
