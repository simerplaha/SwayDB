package swaydb.core.segment.block

import org.scalatest.matchers.should.Matchers._
import swaydb.{Error, IO, TestExecutionContext}
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO.ExceptionHandler.Nothing
import swaydb.config._
import swaydb.core.cache.Cache
import swaydb.core.compression.CompressionTestKit._
import swaydb.core.compression.CoreCompression
import swaydb.core.segment.block.binarysearch.{BinarySearchEntryFormat, BinarySearchIndexBlockConfig}
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockOffset.BinarySearchIndexBlockOps
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockOffset.BloomFilterBlockOps
import swaydb.core.segment.block.hashindex.{HashIndexBlockConfig, HashIndexEntryFormat}
import swaydb.core.segment.block.hashindex.HashIndexBlockOffset.HashIndexBlockOps
import swaydb.core.segment.block.reader.{BlockedReader, BlockRefReader, UnblockedReader}
import swaydb.core.segment.block.segment.{SegmentBlock, SegmentBlockCache, SegmentBlockConfig, SegmentBlockOffset}
import swaydb.core.segment.block.segment.SegmentBlockOffset.SegmentBlockOps
import swaydb.core.segment.block.segment.footer.SegmentFooterBlockOffset.SegmentFooterBlockOps
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockConfig}
import swaydb.core.segment.block.sortedindex.SortedIndexBlockOffset.SortedIndexBlockOps
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockConfig, ValuesBlockOffset}
import swaydb.core.segment.block.values.ValuesBlockOffset.ValuesBlockOps
import swaydb.core.segment.cache.sweeper.MemorySweeper
import swaydb.core.segment.data._
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.segment.io.SegmentReadIO
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.serializers._
import swaydb.serializers.Default._
import swaydb.slice.order.KeyOrder
import swaydb.testkit.RunThis.FutureImplicits
import swaydb.testkit.TestKit._
import swaydb.utils.StorageUnits._
import swaydb.SliceIOImplicits._
import swaydb.config.CoreConfigTestKit._
import swaydb.core.file.FileReader
import swaydb.core.segment.SegmentBlocks
import swaydb.core.segment.SegmentTestKit._
import swaydb.effect.EffectTestKit._
import swaydb.slice.{Reader, Slice, SliceReader}

import java.nio.file.Paths
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
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

  def getBlocks(keyValues: Iterable[Memory],
                useCacheableReaders: Boolean = randomBoolean(),
                valuesConfig: ValuesBlockConfig = ValuesBlockConfig.random,
                sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random,
                binarySearchIndexConfig: BinarySearchIndexBlockConfig = BinarySearchIndexBlockConfig.random,
                hashIndexConfig: HashIndexBlockConfig = HashIndexBlockConfig.random,
                bloomFilterConfig: BloomFilterBlockConfig = BloomFilterBlockConfig.random,
                segmentConfig: SegmentBlockConfig = SegmentBlockConfig.random)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                                               keyOrder: KeyOrder[Slice[Byte]],
                                                                               ec: ExecutionContext = TestExecutionContext.executionContext): IO[Error.Segment, Slice[SegmentBlocks]] = {
    val closedSegments =
      SegmentBlock.writeOnes(
        mergeStats =
          MergeStats
            .persistentBuilder(keyValues)
            .close(
              hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
              optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
            ),
        createdInLevel = 0,
        bloomFilterConfig = bloomFilterConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        sortedIndexConfig = sortedIndexConfig,
        valuesConfig = valuesConfig,
        segmentConfig = segmentConfig
      ).awaitInf

    val segmentIO =
      SegmentReadIO(
        bloomFilterConfig = bloomFilterConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        sortedIndexConfig = sortedIndexConfig,
        valuesConfig = valuesConfig,
        segmentConfig = segmentConfig
      )

    closedSegments.mapRecoverIO {
      closedSegment =>
        readBlocksFromSegment(
          useCacheableReaders = useCacheableReaders,
          closedSegment = closedSegment,
          segmentIO = segmentIO
        )
    }
  }

  def getBlocksSingle(keyValues: Iterable[Memory],
                      valuesConfig: ValuesBlockConfig = ValuesBlockConfig.random,
                      sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random,
                      binarySearchIndexConfig: BinarySearchIndexBlockConfig = BinarySearchIndexBlockConfig.random,
                      hashIndexConfig: HashIndexBlockConfig = HashIndexBlockConfig.random,
                      bloomFilterConfig: BloomFilterBlockConfig = BloomFilterBlockConfig.random,
                      segmentConfig: SegmentBlockConfig = SegmentBlockConfig.random)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                                                     keyOrder: KeyOrder[Slice[Byte]],
                                                                                     ec: ExecutionContext = TestExecutionContext.executionContext): IO[Error.Segment, SegmentBlocks] =
    getBlocks(
      keyValues = keyValues,
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig,
      segmentConfig = segmentConfig.copy(minSize = Int.MaxValue, maxCount = Int.MaxValue)
    ) map {
      segments =>
        segments should have size 1
        segments.head
    }

  def readBlocks(bytes: Slice[Byte],
                 segmentIO: SegmentReadIO = SegmentReadIO.random)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): IO[swaydb.Error.Segment, SegmentBlocks] =
    readBlocksFromReader(SliceReader(bytes), segmentIO)

  def getSegmentBlockCache(keyValues: Slice[Memory],
                           valuesConfig: ValuesBlockConfig = ValuesBlockConfig.random,
                           sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random,
                           binarySearchIndexConfig: BinarySearchIndexBlockConfig = BinarySearchIndexBlockConfig.random,
                           hashIndexConfig: HashIndexBlockConfig = HashIndexBlockConfig.random,
                           bloomFilterConfig: BloomFilterBlockConfig = BloomFilterBlockConfig.random,
                           segmentConfig: SegmentBlockConfig = SegmentBlockConfig.random)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                                                          keyOrder: KeyOrder[Slice[Byte]],
                                                                                          ec: ExecutionContext): Slice[SegmentBlockCache] =
    SegmentBlock.writeOnes(
      mergeStats =
        MergeStats
          .persistentBuilder(keyValues)
          .close(
            hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
            optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
          ),
      createdInLevel = Int.MaxValue,
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig,
      segmentConfig = segmentConfig
    ) map {
      slices =>
        slices mapToSlice {
          closed =>
            val segmentIO =
              SegmentReadIO(
                bloomFilterConfig = bloomFilterConfig,
                hashIndexConfig = hashIndexConfig,
                binarySearchIndexConfig = binarySearchIndexConfig,
                sortedIndexConfig = sortedIndexConfig,
                valuesConfig = valuesConfig,
                segmentConfig = segmentConfig
              )

            getSegmentBlockCacheFromSegmentClosed(closed, segmentIO)
        }
    } awaitInf

  def getSegmentBlockCacheSingle(keyValues: Slice[Memory],
                                 valuesConfig: ValuesBlockConfig = ValuesBlockConfig.random,
                                 sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random,
                                 binarySearchIndexConfig: BinarySearchIndexBlockConfig = BinarySearchIndexBlockConfig.random,
                                 hashIndexConfig: HashIndexBlockConfig = HashIndexBlockConfig.random,
                                 bloomFilterConfig: BloomFilterBlockConfig = BloomFilterBlockConfig.random,
                                 segmentConfig: SegmentBlockConfig = SegmentBlockConfig.random)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                                                                keyOrder: KeyOrder[Slice[Byte]],
                                                                                                ec: ExecutionContext = TestExecutionContext.executionContext): SegmentBlockCache = {
    val blockCaches =
      getSegmentBlockCache(
        keyValues = keyValues,
        bloomFilterConfig = bloomFilterConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        sortedIndexConfig = sortedIndexConfig,
        valuesConfig = valuesConfig,
        segmentConfig = segmentConfig.copy(minSize = Int.MaxValue, maxCount = Int.MaxValue)
      )

    blockCaches should have size 1
    blockCaches.head
  }

  def getSegmentBlockCacheFromSegmentClosed(segment: TransientSegment.One,
                                            segmentIO: SegmentReadIO = SegmentReadIO.random)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): SegmentBlockCache =
    SegmentBlockCache(
      path = Paths.get("test"),
      segmentIO = segmentIO,
      blockRef = BlockRefReader(segment.flattenSegmentBytes),
      valuesReaderCacheable = segment.valuesUnblockedReader,
      sortedIndexReaderCacheable = segment.sortedIndexUnblockedReader,
      hashIndexReaderCacheable = segment.hashIndexUnblockedReader,
      binarySearchIndexReaderCacheable = segment.binarySearchUnblockedReader,
      bloomFilterReaderCacheable = segment.bloomFilterUnblockedReader,
      footerCacheable = segment.footerUnblocked
    )

  def getSegmentBlockCacheFromReader(reader: Reader,
                                     segmentIO: SegmentReadIO = SegmentReadIO.random)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): SegmentBlockCache =
    SegmentBlockCache(
      path = Paths.get("test-cache"),
      segmentIO = segmentIO,
      blockRef =
        reader match {
          case reader: FileReader =>
            import swaydb.core.file.CoreFileTestKit._
            BlockRefReader(invokePrivate_file(reader), BlockCache.forSearch(reader.size(), blockCacheMemorySweeper))

          case SliceReader(slice, position) =>
            BlockRefReader[SegmentBlockOffset](slice.drop(position))
        },
      valuesReaderCacheable = None,
      sortedIndexReaderCacheable = None,
      hashIndexReaderCacheable = None,
      binarySearchIndexReaderCacheable = None,
      bloomFilterReaderCacheable = None,
      footerCacheable = None
    )

  def readAll(bytes: Slice[Byte])(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): IO[swaydb.Error.Segment, Slice[KeyValue]] =
    readAll(SliceReader(bytes))

  def readAll(reader: Reader)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): IO[swaydb.Error.Segment, Slice[KeyValue]] =
    IO {
      val blockCache = getSegmentBlockCacheFromReader(reader)

      SortedIndexBlock
        .toSlice(
          keyValueCount = blockCache.getFooter().keyValueCount,
          sortedIndexReader = blockCache.createSortedIndexReader(),
          valuesReaderOrNull = blockCache.createValuesReaderOrNull()
        )
    }

  def readBlocksFromReader(reader: Reader, segmentIO: SegmentReadIO = SegmentReadIO.random)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                            blockCacheMemorySweeper: Option[MemorySweeper.Block]): IO[swaydb.Error.Segment, SegmentBlocks] = {
    val blockCache = getSegmentBlockCacheFromReader(reader, segmentIO)
    readBlocks(blockCache)
  }

  def readBlocks(blockCache: SegmentBlockCache) =
    IO {
      SegmentBlocks(
        footer = blockCache.getFooter(),
        valuesReader = Option(blockCache.createValuesReaderOrNull()),
        sortedIndexReader = blockCache.createSortedIndexReader(),
        hashIndexReader = Option(blockCache.createHashIndexReaderOrNull()),
        binarySearchIndexReader = Option(blockCache.createBinarySearchIndexReaderOrNull()),
        bloomFilterReader = Option(blockCache.createBloomFilterReaderOrNull())
      )
    }

  def readAll(segment: TransientSegment.One)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): IO[swaydb.Error.Segment, Slice[KeyValue]] =
    SegmentBlockTestKit.readAll(segment.flattenSegmentBytes)

  def writeAndRead(keyValues: Iterable[Memory])(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                keyOrder: KeyOrder[Slice[Byte]],
                                                ec: ExecutionContext): IO[swaydb.Error.Segment, Slice[KeyValue]] = {
    val sortedIndexBlock = SortedIndexBlockConfig.random

    val segment =
      SegmentBlock.writeOnes(
        mergeStats =
          MergeStats
            .persistentBuilder(keyValues)
            .close(
              hasAccessPositionIndex = sortedIndexBlock.enableAccessPositionIndex,
              optimiseForReverseIteration = sortedIndexBlock.optimiseForReverseIteration
            ),
        createdInLevel = 0,
        bloomFilterConfig = BloomFilterBlockConfig.random,
        hashIndexConfig = HashIndexBlockConfig.random,
        binarySearchIndexConfig = BinarySearchIndexBlockConfig.random,
        sortedIndexConfig = sortedIndexBlock,
        valuesConfig = ValuesBlockConfig.random,
        segmentConfig = SegmentBlockConfig.random.copy(minSize = Int.MaxValue, maxCount = Int.MaxValue)
      ).awaitInf

    segment should have size 1

    SegmentBlockTestKit.readAll(segment.head.flattenSegmentBytes)
  }

  def readBlocksFromSegment(closedSegment: TransientSegment.One,
                            segmentIO: SegmentReadIO = SegmentReadIO.random,
                            useCacheableReaders: Boolean = randomBoolean())(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): IO[swaydb.Error.Segment, SegmentBlocks] =
    if (useCacheableReaders && closedSegment.sortedIndexUnblockedReader.isDefined && randomBoolean()) //randomly also use cacheable readers
      IO(readCachedBlocksFromSegment(closedSegment).get)
    else
      readBlocks(closedSegment.flattenSegmentBytes, segmentIO)

  def readCachedBlocksFromSegment(closedSegment: TransientSegment.One): Option[SegmentBlocks] =
    if (closedSegment.sortedIndexUnblockedReader.isDefined)
      Some(
        SegmentBlocks(
          valuesReader = closedSegment.valuesUnblockedReader,
          sortedIndexReader = closedSegment.sortedIndexUnblockedReader.get,
          hashIndexReader = closedSegment.hashIndexUnblockedReader,
          binarySearchIndexReader = closedSegment.binarySearchUnblockedReader,
          bloomFilterReader = closedSegment.bloomFilterUnblockedReader,
          footer = closedSegment.footerUnblocked.get
        )
      )
    else
      None

  implicit class TransientSegmentImplicits(segment: TransientSegment.Persistent) {

    def flattenSegmentBytes: Slice[Byte] = {
      val slice = Slice.allocate[Byte](segment.segmentSize)

      segment match {
        case segment: TransientSegment.OneOrRemoteRef =>
          segment match {
            case segment: TransientSegment.RemoteRef =>
              slice addAll segment.ref.readAllBytes()

            case segment: TransientSegment.One =>
              slice addAll segment.fileHeader
              segment.bodyBytes foreach (slice addAll _)
          }

        case segment: TransientSegment.Many =>
          slice addAll segment.fileHeader
          segment.listSegment.bodyBytes foreach slice.addAll
          segment.segments.foreach(single => slice.addAll(single.flattenSegmentBytes))
      }

      assert(slice.isFull)
      slice
    }

    def flattenSegment: (Slice[Byte], Option[Deadline]) =
      (flattenSegmentBytes, segment.nearestPutDeadline)
  }
}
