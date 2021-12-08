package swaydb.core.segment

import swaydb.config.MMAP
import swaydb.core._
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
import swaydb.core.segment.block.hashindex.HashIndexBlockConfig
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.block.sortedindex.SortedIndexBlockConfig
import swaydb.core.segment.block.values.ValuesBlockConfig
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.segment.data.Memory
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.CoreTestData._
import swaydb.core.TestSweeper._
import swaydb.effect.{Dir, Effect}
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit.{eitherOne, randomIntMax}
import swaydb.utils.{IDGenerator, OperatingSystem}

import java.nio.file.Path
import scala.concurrent.ExecutionContext

trait ASegmentSpec extends ACoreSpec {

  def mmapSegments: MMAP.Segment = MMAP.On(OperatingSystem.isWindows, TestForceSave.mmap())

  def isWindowsAndMMAPSegments(): Boolean =
    OperatingSystem.isWindows && mmapSegments.mmapReads && mmapSegments.mmapWrites

  def nextSegmentId: String = ??? //idGenerator.nextSegment

  def testSegmentFile: Path =
    if (isMemorySpec)
      randomIntDirectory.resolve(nextSegmentId)
    else
      Effect.createDirectoriesIfAbsent(randomIntDirectory).resolve(nextSegmentId)

  object TestSegment {
    def apply(keyValues: Slice[Memory] = randomizedKeyValues()(TestTimer.Incremental()),
              createdInLevel: Int = 1,
              path: Path = testSegmentFile,
              valuesConfig: ValuesBlockConfig = ValuesBlockConfig.random,
              sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random,
              binarySearchIndexConfig: BinarySearchIndexBlockConfig = BinarySearchIndexBlockConfig.random,
              hashIndexConfig: HashIndexBlockConfig = HashIndexBlockConfig.random,
              bloomFilterConfig: BloomFilterBlockConfig = BloomFilterBlockConfig.random,
              segmentConfig: SegmentBlockConfig = SegmentBlockConfig.random.copy(mmap = mmapSegments))(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                                       timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                                                                       sweeper: TestSweeper,
                                                                                                       ec: ExecutionContext = TestExecutionContext.executionContext): Segment = {

      val segmentNumber = Effect.numberFileId(path)._1 - 1

      implicit val idGenerator: IDGenerator = IDGenerator(segmentNumber)

      implicit val pathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq.empty)

      val segments =
        many(
          createdInLevel = createdInLevel,
          keyValues = keyValues,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentConfig =
            if (isPersistentSpec)
              segmentConfig.copy(minSize = Int.MaxValue, maxCount = eitherOne(randomIntMax(keyValues.size), Int.MaxValue))
            else
              segmentConfig.copy(minSize = Int.MaxValue, maxCount = Int.MaxValue)
        )

      segments should have size 1

      segments.head
    }

    def one(keyValues: Slice[Memory] = randomizedKeyValues()(TestTimer.Incremental()),
            createdInLevel: Int = 1,
            path: Path = testSegmentFile,
            valuesConfig: ValuesBlockConfig = ValuesBlockConfig.random,
            sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random,
            binarySearchIndexConfig: BinarySearchIndexBlockConfig = BinarySearchIndexBlockConfig.random,
            hashIndexConfig: HashIndexBlockConfig = HashIndexBlockConfig.random,
            bloomFilterConfig: BloomFilterBlockConfig = BloomFilterBlockConfig.random,
            segmentConfig: SegmentBlockConfig = SegmentBlockConfig.random.copy(mmap = mmapSegments))(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                                     timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                                                                     sweeper: TestSweeper,
                                                                                                     ec: ExecutionContext = TestExecutionContext.executionContext): Segment = {

      val segmentNumber = Effect.numberFileId(path)._1 - 1

      implicit val idGenerator: IDGenerator = IDGenerator(segmentNumber)

      implicit val pathsDistributor: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq.empty)

      val segments =
        many(
          createdInLevel = createdInLevel,
          keyValues = keyValues,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentConfig = segmentConfig.copy(minSize = Int.MaxValue, maxCount = Int.MaxValue)
        )

      segments should have size 1

      segments.head
    }

    def many(createdInLevel: Int = 1,
             keyValues: Slice[Memory] = randomizedKeyValues()(TestTimer.Incremental()),
             valuesConfig: ValuesBlockConfig = ValuesBlockConfig.random,
             sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random,
             binarySearchIndexConfig: BinarySearchIndexBlockConfig = BinarySearchIndexBlockConfig.random,
             hashIndexConfig: HashIndexBlockConfig = HashIndexBlockConfig.random,
             bloomFilterConfig: BloomFilterBlockConfig = BloomFilterBlockConfig.random,
             segmentConfig: SegmentBlockConfig = SegmentBlockConfig.random.copy(mmap = mmapSegments))(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                                      timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                                                                      pathsDistributor: PathsDistributor,
                                                                                                      idGenerator: IDGenerator,
                                                                                                      sweeper: TestSweeper,
                                                                                                      ec: ExecutionContext = TestExecutionContext.executionContext): Slice[Segment] = {
      import sweeper._

//      implicit val segmentIO: SegmentReadIO =
//        SegmentReadIO(
//          bloomFilterConfig = bloomFilterConfig,
//          hashIndexConfig = hashIndexConfig,
//          binarySearchIndexConfig = binarySearchIndexConfig,
//          sortedIndexConfig = sortedIndexConfig,
//          valuesConfig = valuesConfig,
//          segmentConfig = segmentConfig
//        )
//
//      val segment =
//        if (isMemorySpec)
//          Segment.memory(
//            minSegmentSize = segmentConfig.minSize,
//            maxKeyValueCountPerSegment = segmentConfig.maxCount,
//            pathsDistributor = pathsDistributor,
//            createdInLevel = createdInLevel,
//            stats = MergeStats.memoryBuilder(keyValues).close()
//          )
//        else
//          Segment.persistent(
//            pathsDistributor = pathsDistributor,
//            createdInLevel = createdInLevel,
//            bloomFilterConfig = bloomFilterConfig,
//            hashIndexConfig = hashIndexConfig,
//            binarySearchIndexConfig = binarySearchIndexConfig,
//            sortedIndexConfig = sortedIndexConfig,
//            valuesConfig = valuesConfig,
//            segmentConfig = segmentConfig,
//            mergeStats =
//              MergeStats
//                .persistentBuilder(keyValues)
//                .close(
//                  hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
//                  optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
//                )
//          ).awaitInf
//
//      segment.foreach(_.sweep())
//
//      Slice.from(segment, segment.size)
      ???
    }
  }


  def assertSegment[T](keyValues: Slice[Memory],
                       assert: (Slice[Memory], Segment) => T,
                       segmentConfig: SegmentBlockConfig = SegmentBlockConfig.random.copy(mmap = mmapSegments),
                       ensureOneSegmentOnly: Boolean = false,
                       testAgainAfterAssert: Boolean = true,
                       closeAfterCreate: Boolean = false,
                       valuesConfig: ValuesBlockConfig = ValuesBlockConfig.random,
                       sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random,
                       binarySearchIndexConfig: BinarySearchIndexBlockConfig = BinarySearchIndexBlockConfig.random,
                       hashIndexConfig: HashIndexBlockConfig = HashIndexBlockConfig.random,
                       bloomFilterConfig: BloomFilterBlockConfig = BloomFilterBlockConfig.random)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                                  sweeper: TestSweeper,
                                                                                                  segmentIO: SegmentReadIO = SegmentReadIO.random,
                                                                                                  ec: ExecutionContext = TestExecutionContext.executionContext) = {
    println(s"assertSegment - keyValues: ${keyValues.size}")

    val segment =
      if (ensureOneSegmentOnly)
        TestSegment.one(
          keyValues = keyValues,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentConfig = segmentConfig
        )
      else
        TestSegment(
          keyValues = keyValues,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentConfig = segmentConfig
        )

    if (closeAfterCreate) segment.close()

    assert(keyValues, segment) //first
    if (testAgainAfterAssert) {
      assert(keyValues, segment) //with cache populated

      //clear cache and assert
      segment.clearCachedKeyValues()
      assert(keyValues, segment) //same Segment but test with cleared cache.

      //clear all caches and assert
      segment.clearAllCaches()
      assert(keyValues, segment) //same Segment but test with cleared cache.
    }

    segment match {
      case segment: PersistentSegment =>
        val segmentReopened = segment.reopen //reopen
        if (closeAfterCreate) segmentReopened.close()
        assert(keyValues, segmentReopened)

        if (testAgainAfterAssert) assert(keyValues, segmentReopened)
        segmentReopened.close()

      case _: Segment =>
      //memory segment cannot be reopened
    }

    segment.close()
  }

}
