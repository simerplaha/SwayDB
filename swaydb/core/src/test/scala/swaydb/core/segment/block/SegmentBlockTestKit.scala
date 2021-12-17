package swaydb.core.segment.block

import swaydb.config.{MMAP, SegmentRefCacheLife, UncompressedBlockInfo}
import swaydb.core.segment.block.binarysearch.{BinarySearchEntryFormat, BinarySearchIndexBlockConfig}
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
import swaydb.core.segment.block.segment.{SegmentBlock, SegmentBlockConfig, SegmentBlockOffset}
import swaydb.core.segment.block.sortedindex.SortedIndexBlockConfig
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockConfig, ValuesBlockOffset}
import swaydb.core.compression.CoreCompression
import swaydb.core.segment.block.hashindex.{HashIndexBlockConfig, HashIndexEntryFormat}
import swaydb.core.cache.Cache
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockOffset.BinarySearchIndexBlockOps
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockOffset.BloomFilterBlockOps
import swaydb.core.segment.block.hashindex.HashIndexBlockOffset.HashIndexBlockOps
import swaydb.core.segment.block.reader.{BlockedReader, UnblockedReader}
import swaydb.core.segment.block.segment.SegmentBlockOffset.SegmentBlockOps
import swaydb.core.segment.block.segment.footer.SegmentFooterBlockOffset.SegmentFooterBlockOps
import swaydb.core.segment.block.sortedindex.SortedIndexBlockOffset.SortedIndexBlockOps
import swaydb.core.segment.block.values.ValuesBlockOffset.ValuesBlockOps
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.testkit.TestKit.{eitherOne, randomBoolean, randomFiniteDuration, randomIntMax}
import swaydb.IO.ExceptionHandler.Nothing
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.segment.data.Memory
import swaydb.slice.order.KeyOrder
import swaydb.IO
import swaydb.slice.Slice
import swaydb.core.segment.data.KeyValueTestKit._
import swaydb.slice.SliceTestKit._
import swaydb.testkit.RunThis._
import swaydb.config.CoreConfigTestKit._
import swaydb.core.segment.SegmentTestKit._
import swaydb.core.compression.CompressionTestKit._
import swaydb.effect.EffectTestKit._
import org.scalatest.matchers.should.Matchers._
import swaydb.utils.StorageUnits._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext
import scala.util.Random

object SegmentBlockTestKit {

  implicit class ValuesBlockConfigImplicits(values: ValuesBlockConfig.type) {
    def random: ValuesBlockConfig =
      random(randomBoolean())

    def random(hasCompression: Boolean, cacheOnAccess: Boolean = randomBoolean()): ValuesBlockConfig =
      ValuesBlockConfig(
        compressDuplicateValues = randomBoolean(),
        compressDuplicateRangeValues = randomBoolean(),
        ioStrategy = _ => randomIOStrategy(cacheOnAccess),
        compressions = _ => if (hasCompression) randomCompressions() else Seq.empty
      )
  }

  implicit class SortedIndexBlockConfigImplicits(values: SortedIndexBlockConfig.type) {
    def random: SortedIndexBlockConfig =
      random(randomBoolean())

    def random(hasCompression: Boolean,
               cacheOnAccess: Boolean = randomBoolean(),
               shouldPrefixCompress: Boolean = randomBoolean()): SortedIndexBlockConfig =
      SortedIndexBlockConfig(
        ioStrategy = _ => randomIOStrategy(cacheOnAccess),
        enablePrefixCompression = randomBoolean(),
        optimiseForReverseIteration = randomBoolean(),
        shouldPrefixCompress = _ => shouldPrefixCompress,
        prefixCompressKeysOnly = randomBoolean(),
        enableAccessPositionIndex = randomBoolean(),
        normaliseIndex = randomBoolean(),
        compressions = _ => if (hasCompression) randomCompressions() else Seq.empty
      )
  }

  implicit class BinarySearchIndexBlockConfigImplicits(values: BinarySearchIndexBlockConfig.type) {
    def random: BinarySearchIndexBlockConfig =
      random(randomBoolean())

    def random(hasCompression: Boolean, cacheOnAccess: Boolean = randomBoolean()): BinarySearchIndexBlockConfig =
      BinarySearchIndexBlockConfig(
        enabled = randomBoolean(),
        format = randomBinarySearchFormat(),
        minimumNumberOfKeys = randomIntMax(5),
        searchSortedIndexDirectlyIfPossible = randomBoolean(),
        fullIndex = randomBoolean(),
        ioStrategy = _ => randomIOStrategy(cacheOnAccess),
        compressions = _ => if (hasCompression) randomCompressions() else Seq.empty
      )
  }

  implicit class HashIndexBlockConfigImplicits(values: HashIndexBlockConfig.type) {
    def random: HashIndexBlockConfig =
      random(randomBoolean())

    def random(hasCompression: Boolean, cacheOnAccess: Boolean = randomBoolean()): HashIndexBlockConfig =
      HashIndexBlockConfig(
        maxProbe = randomIntMax(10),
        minimumNumberOfKeys = randomIntMax(5),
        minimumNumberOfHits = randomIntMax(5),
        format = randomHashIndexSearchFormat(),
        allocateSpace = _.requiredSpace * randomIntMax(3),
        ioStrategy = _ => randomIOStrategy(cacheOnAccess),
        compressions = _ => if (hasCompression) randomCompressions() else Seq.empty
      )
  }

  implicit class BloomFilterBlockConfigImplicits(values: BloomFilterBlockConfig.type) {
    def random: BloomFilterBlockConfig =
      random(randomBoolean())

    def random(hasCompression: Boolean, cacheOnAccess: Boolean = randomBoolean()): BloomFilterBlockConfig =
      BloomFilterBlockConfig(
        falsePositiveRate = Random.nextDouble() min 0.5,
        minimumNumberOfKeys = randomIntMax(5),
        optimalMaxProbe = optimalMaxProbe => optimalMaxProbe,
        ioStrategy = _ => randomIOStrategy(cacheOnAccess),
        compressions = _ => if (hasCompression) randomCompressions() else Seq.empty
      )
  }

  implicit class SegmentBlockConfigImplicits(values: SegmentBlockConfig.type) {
    def random: SegmentBlockConfig =
      random(hasCompression = randomBoolean())

    def random(hasCompression: Boolean = randomBoolean(),
               minSegmentSize: Int = randomIntMax(4.mb),
               maxKeyValuesPerSegment: Int = eitherOne(randomIntMax(100), randomIntMax(100000)),
               deleteDelay: FiniteDuration = randomFiniteDuration(),
               mmap: MMAP.Segment = MMAP.randomForSegment(),
               cacheBlocksOnCreate: Boolean = randomBoolean(),
               enableHashIndexForListSegment: Boolean = randomBoolean(),
               cacheOnAccess: Boolean = randomBoolean(),
               segmentRefCacheLife: SegmentRefCacheLife = randomSegmentRefCacheLife(),
               initialiseIteratorsInOneSeek: Boolean = randomBoolean()): SegmentBlockConfig =
      SegmentBlockConfig.applyInternal(
        fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess),
        blockIOStrategy = _ => randomIOStrategy(cacheOnAccess),
        cacheBlocksOnCreate = cacheBlocksOnCreate,
        minSize = minSegmentSize,
        maxCount = maxKeyValuesPerSegment,
        segmentRefCacheLife = segmentRefCacheLife,
        enableHashIndexForListSegment = enableHashIndexForListSegment,
        mmap = mmap,
        deleteDelay = deleteDelay,
        compressions = _ => if (hasCompression) randomCompressions() else Seq.empty,
        initialiseIteratorsInOneSeek = initialiseIteratorsInOneSeek
      )

    def random2(fileOpenIOStrategy: IOStrategy.ThreadSafe = randomThreadSafeIOStrategy(),
                blockIOStrategy: IOAction => IOStrategy = _ => randomIOStrategy(),
                cacheBlocksOnCreate: Boolean = randomBoolean(),
                compressions: UncompressedBlockInfo => Iterable[CoreCompression] = _ => randomCompressionsOrEmpty(),
                maxKeyValuesPerSegment: Int = randomIntMax(1000000),
                deleteDelay: FiniteDuration = randomFiniteDuration(),
                mmap: MMAP.Segment = MMAP.randomForSegment(),
                enableHashIndexForListSegment: Boolean = randomBoolean(),
                minSegmentSize: Int = randomIntMax(30.mb),
                segmentRefCacheLife: SegmentRefCacheLife = randomSegmentRefCacheLife(),
                initialiseIteratorsInOneSeek: Boolean = randomBoolean()): SegmentBlockConfig =
      SegmentBlockConfig.applyInternal(
        fileOpenIOStrategy = fileOpenIOStrategy,
        blockIOStrategy = blockIOStrategy,
        cacheBlocksOnCreate = cacheBlocksOnCreate,
        minSize = minSegmentSize,
        maxCount = maxKeyValuesPerSegment,
        segmentRefCacheLife = segmentRefCacheLife,
        enableHashIndexForListSegment = enableHashIndexForListSegment,
        mmap = mmap,
        deleteDelay = deleteDelay,
        compressions = compressions,
        initialiseIteratorsInOneSeek = initialiseIteratorsInOneSeek
      )
  }


  def allBlockOps(): Array[BlockOps[_, _]] =
    Array(
      BinarySearchIndexBlockOps,
      BloomFilterBlockOps,
      HashIndexBlockOps,
      SegmentBlockOps,
      SegmentFooterBlockOps,
      SortedIndexBlockOps,
      ValuesBlockOps
    )

  def randomBlockOps(): BlockOps[_, _] =
    Random.shuffle(allBlockOps().to(List)).head

  def randomFalsePositiveRate() =
    Random.nextDouble()

  def randomBinarySearchFormat(): BinarySearchEntryFormat =
    Random.shuffle(BinarySearchEntryFormat.formats.toList).head

  def randomHashIndexSearchFormat(): HashIndexEntryFormat =
    Random.shuffle(HashIndexEntryFormat.formats.toList).head

  implicit class SegmentBlockImplicits(segmentBlock: SegmentBlock.type) {

    def emptyDecompressedBlock: UnblockedReader[SegmentBlockOffset, SegmentBlock] =
      UnblockedReader.empty(
        SegmentBlock(
          offset = SegmentBlockOffset.empty,
          headerSize = 0,
          compressionInfo = BlockCompressionInfo.Null
        )
      )

    def unblocked(bytes: Slice[Byte])(implicit updater: BlockOps[SegmentBlockOffset, SegmentBlock]): UnblockedReader[SegmentBlockOffset, SegmentBlock] =
      UnblockedReader(
        block =
          SegmentBlock(
            offset = SegmentBlockOffset(
              start = 0,
              size = bytes.size
            ),
            headerSize = 0,
            compressionInfo = BlockCompressionInfo.Null
          ),
        bytes = bytes
      )

    def blocked(bytes: Slice[Byte], headerSize: Int, compressionInfo: BlockCompressionInfo)(implicit updater: BlockOps[SegmentBlockOffset, SegmentBlock]): BlockedReader[SegmentBlockOffset, SegmentBlock] =
      BlockedReader(
        bytes = bytes,
        block =
          SegmentBlock(
            offset = SegmentBlockOffset(
              start = 0,
              size = bytes.size
            ),
            headerSize = headerSize,
            compressionInfo = compressionInfo
          )
      )

    def writeClosedOne(keyValues: Iterable[Memory],
                       createdInLevel: Int = randomIntMax(),
                       bloomFilterConfig: BloomFilterBlockConfig = BloomFilterBlockConfig.random,
                       hashIndexConfig: HashIndexBlockConfig = HashIndexBlockConfig.random,
                       binarySearchIndexConfig: BinarySearchIndexBlockConfig = BinarySearchIndexBlockConfig.random,
                       sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random,
                       valuesConfig: ValuesBlockConfig = ValuesBlockConfig.random,
                       segmentConfig: SegmentBlockConfig = SegmentBlockConfig.random)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                      ec: ExecutionContext): TransientSegment.One = {
      val segments =
        SegmentBlock.writeOnes(
          mergeStats =
            MergeStats
              .persistentBuilder(keyValues)
              .close(
                hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
                optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
              ),
          createdInLevel = createdInLevel,
          bloomFilterConfig = bloomFilterConfig,
          hashIndexConfig = hashIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          sortedIndexConfig = sortedIndexConfig,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig.copy(minSize = Int.MaxValue, maxCount = Int.MaxValue)
        ).awaitInf

      segments should have size 1
      segments.head
    }
  }

  def buildSingleValueCache(bytes: Slice[Byte]): Cache[swaydb.Error.Segment, ValuesBlockOffset, UnblockedReader[ValuesBlockOffset, ValuesBlock]] =
    Cache.concurrent[swaydb.Error.Segment, ValuesBlockOffset, UnblockedReader[ValuesBlockOffset, ValuesBlock]](randomBoolean(), randomBoolean(), None) {
      (offset, _) =>
        IO[Nothing, UnblockedReader[ValuesBlockOffset, ValuesBlock]](
          UnblockedReader(
            block = ValuesBlock(offset, 0, BlockCompressionInfo.Null),
            bytes = bytes
          )
        )(Nothing)
    }

  def buildSingleValueReader(bytes: Slice[Byte]): UnblockedReader[ValuesBlockOffset, ValuesBlock] =
    UnblockedReader(
      block = ValuesBlock(ValuesBlockOffset(0, bytes.size), 0, BlockCompressionInfo.Null),
      bytes = bytes
    )

}
