package swaydb.core.segment.block.segment

import swaydb.Compression
import swaydb.core.compression.CoreCompression
import swaydb.config.{MMAP, SegmentConfig, SegmentRefCacheLife, UncompressedBlockInfo}
import swaydb.effect.{IOAction, IOStrategy}

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object SegmentBlockConfig {

  def apply(config: SegmentConfig): SegmentBlockConfig =
    apply(
      fileOpenIOStrategy = config.fileOpenIOStrategy,
      blockIOStrategy = config.blockIOStrategy,
      cacheBlocksOnCreate = config.cacheSegmentBlocksOnCreate,
      minSize = config.minSegmentSize,
      maxCount = config.segmentFormat.count,
      segmentRefCacheLife = config.segmentFormat.segmentRefCacheLife,
      enableHashIndexForListSegment = config.segmentFormat.enableRootHashIndex,
      initialiseIteratorsInOneSeek = config.initialiseIteratorsInOneSeek,
      mmap = config.mmap,
      deleteDelay = config.deleteDelay,
      compressions = config.compression
    )

  def apply(fileOpenIOStrategy: IOStrategy.ThreadSafe,
            blockIOStrategy: IOAction => IOStrategy,
            cacheBlocksOnCreate: Boolean,
            minSize: Int,
            maxCount: Int,
            segmentRefCacheLife: SegmentRefCacheLife,
            enableHashIndexForListSegment: Boolean,
            initialiseIteratorsInOneSeek: Boolean,
            mmap: MMAP.Segment,
            deleteDelay: FiniteDuration,
            compressions: UncompressedBlockInfo => Iterable[Compression]): SegmentBlockConfig =
    applyInternal(
      fileOpenIOStrategy = fileOpenIOStrategy,
      blockIOStrategy = blockIOStrategy,
      cacheBlocksOnCreate = cacheBlocksOnCreate,
      minSize = minSize,
      maxCount = maxCount,
      segmentRefCacheLife = segmentRefCacheLife,
      enableHashIndexForListSegment = enableHashIndexForListSegment,
      initialiseIteratorsInOneSeek = initialiseIteratorsInOneSeek,
      mmap = mmap,
      deleteDelay = deleteDelay,
      compressions =
        uncompressedBlockInfo =>
          Try(compressions(uncompressedBlockInfo))
            .getOrElse(Seq.empty)
            .map(CoreCompression.apply)
            .toSeq
    )

  private[core] def applyInternal(fileOpenIOStrategy: IOStrategy.ThreadSafe,
                                  blockIOStrategy: IOAction => IOStrategy,
                                  cacheBlocksOnCreate: Boolean,
                                  minSize: Int,
                                  maxCount: Int,
                                  segmentRefCacheLife: SegmentRefCacheLife,
                                  enableHashIndexForListSegment: Boolean,
                                  initialiseIteratorsInOneSeek: Boolean,
                                  mmap: MMAP.Segment,
                                  deleteDelay: FiniteDuration,
                                  compressions: UncompressedBlockInfo => Iterable[CoreCompression]): SegmentBlockConfig =
    new SegmentBlockConfig(
      fileOpenIOStrategy = fileOpenIOStrategy,
      blockIOStrategy = blockIOStrategy,
      cacheBlocksOnCreate = cacheBlocksOnCreate,
      minSize = minSize max 1,
      maxCount = maxCount max 1,
      segmentRefCacheLife = segmentRefCacheLife,
      enableHashIndexForListSegment = enableHashIndexForListSegment,
      initialiseIteratorsInOneSeek = initialiseIteratorsInOneSeek,
      mmap = mmap,
      deleteDelay = deleteDelay,
      compressions = compressions
    )
}

class SegmentBlockConfig private(val fileOpenIOStrategy: IOStrategy.ThreadSafe,
                                 val blockIOStrategy: IOAction => IOStrategy,
                                 val cacheBlocksOnCreate: Boolean,
                                 val minSize: Int,
                                 val maxCount: Int,
                                 val segmentRefCacheLife: SegmentRefCacheLife,
                                 val enableHashIndexForListSegment: Boolean,
                                 val initialiseIteratorsInOneSeek: Boolean,
                                 val mmap: MMAP.Segment,
                                 val deleteDelay: FiniteDuration,
                                 val compressions: UncompressedBlockInfo => Iterable[CoreCompression]) {

  val isDeleteEventually: Boolean =
    deleteDelay.fromNow.hasTimeLeft()

  //disables splitting of segments and creates a single segment.
  def singleton: SegmentBlockConfig =
    this.copy(minSize = Int.MaxValue, maxCount = Int.MaxValue)

  def copy(minSize: Int = minSize, maxCount: Int = maxCount, mmap: MMAP.Segment = mmap): SegmentBlockConfig =
    SegmentBlockConfig.applyInternal(
      fileOpenIOStrategy = fileOpenIOStrategy,
      blockIOStrategy = blockIOStrategy,
      cacheBlocksOnCreate = cacheBlocksOnCreate,
      minSize = minSize,
      maxCount = maxCount,
      segmentRefCacheLife = segmentRefCacheLife,
      enableHashIndexForListSegment = enableHashIndexForListSegment,
      initialiseIteratorsInOneSeek = initialiseIteratorsInOneSeek,
      mmap = mmap,
      deleteDelay = deleteDelay,
      compressions = compressions
    )
}
