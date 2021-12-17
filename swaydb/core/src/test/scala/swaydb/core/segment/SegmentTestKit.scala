package swaydb.core.segment

import org.scalatest.matchers.should.Matchers._
import swaydb.{Error, IO}
import swaydb.Error.Segment.ExceptionHandler
import swaydb.config._
import swaydb.config.CoreConfigTestKit._
import swaydb.core.TestSweeper
import swaydb.core.TestSweeper._
import swaydb.core.level.seek._
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
import swaydb.core.segment.block.hashindex.HashIndexBlockConfig
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlockConfig
import swaydb.core.segment.block.values.ValuesBlockConfig
import swaydb.core.segment.data._
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.segment.data.merge.KeyValueGrouper
import swaydb.core.segment.data.KeyValueTestKit._
import swaydb.core.segment.data.Memory.PendingApply
import swaydb.core.segment.entry.id.BaseEntryIdFormatA
import swaydb.core.segment.entry.writer.EntryWriter
import swaydb.core.segment.io.{SegmentCompactionIO, SegmentReadIO}
import swaydb.core.segment.ref.SegmentRef
import swaydb.core.segment.ref.search.SegmentSearchTestKit._
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.util.DefIO
import swaydb.effect.EffectTestKit._
import swaydb.effect.IOValues._
import swaydb.serializers._
import swaydb.serializers.Default._
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.Slice
import swaydb.slice.SliceTestKit._
import swaydb.testkit.TestKit._
import swaydb.utils.IDGenerator

import java.nio.file.Path
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

object SegmentTestKit {

  val allBaseEntryIds = BaseEntryIdFormatA.baseIds

  implicit class SegmentIOImplicits(io: SegmentReadIO.type) {
    def random: SegmentReadIO =
      random(cacheOnAccess = randomBoolean())

    def random(cacheOnAccess: Boolean = randomBoolean(),
               includeReserved: Boolean = true): SegmentReadIO =
      SegmentReadIO(
        fileOpenIO = randomThreadSafeIOStrategy(cacheOnAccess, includeReserved),
        segmentBlockIO = _ => randomIOStrategy(cacheOnAccess, includeReserved),
        hashIndexBlockIO = _ => randomIOStrategy(cacheOnAccess, includeReserved),
        bloomFilterBlockIO = _ => randomIOStrategy(cacheOnAccess, includeReserved),
        binarySearchIndexBlockIO = _ => randomIOStrategy(cacheOnAccess, includeReserved),
        sortedIndexBlockIO = _ => randomIOStrategy(cacheOnAccess, includeReserved),
        valuesBlockIO = _ => randomIOStrategy(cacheOnAccess, includeReserved),
        segmentFooterBlockIO = _ => randomIOStrategy(cacheOnAccess, includeReserved)
      )
  }

  implicit class SegmentsImplicits(actual: Iterable[Segment]) {

    def shouldBe(expected: Iterable[Segment]): Unit =
      actual.zip(expected) foreach {
        case (left, right) =>
          left shouldBe right
      }

    def shouldHaveSameKeyValuesAs(expected: Iterable[Segment]): Unit =
      actual.flatMap(_.iterator(randomBoolean())).runRandomIO.get shouldBe expected.flatMap(_.iterator(randomBoolean())).runRandomIO.get
  }

  implicit class SegmentImplicits(actual: Segment) {

    def shouldBe(expected: Segment): Unit =
      shouldBe(expected = expected, ignoreReads = false)

    def shouldBeIgnoreReads(expected: Segment): Unit =
      shouldBe(expected = expected, ignoreReads = true)

    def shouldBe(expected: Segment, ignoreReads: Boolean): Unit = {
      actual.path shouldBe expected.path
      actual.segmentNumber shouldBe expected.segmentNumber
      actual.segmentSize shouldBe expected.segmentSize
      actual.minKey shouldBe expected.minKey
      actual.maxKey shouldBe expected.maxKey
      actual.hasRange shouldBe expected.hasRange

      actual.updateCount shouldBe expected.updateCount
      actual.putDeadlineCount shouldBe expected.putDeadlineCount
      actual.putCount shouldBe expected.putCount
      actual.rangeCount shouldBe expected.rangeCount
      actual.keyValueCount shouldBe expected.keyValueCount

      actual.hasBloomFilter() shouldBe expected.hasBloomFilter
      actual.minMaxFunctionId shouldBe expected.minMaxFunctionId
      actual.nearestPutDeadline shouldBe expected.nearestPutDeadline
      actual.persistent shouldBe actual.persistent
      actual.existsOnDisk() shouldBe expected.existsOnDisk()
      actual.segmentNumber shouldBe expected.segmentNumber
      actual.getClass shouldBe expected.getClass
      if (!ignoreReads)
        assertReads(Slice.from(expected.iterator(randomBoolean()), expected.keyValueCount).runRandomIO.get, actual)
    }

    def shouldContainAll(keyValues: Slice[KeyValue]): Unit =
      keyValues.foreach {
        keyValue =>
          actual.get(keyValue.key, ThreadReadState.random).runRandomIO.get.getUnsafe shouldBe keyValue
      }
  }

  def dump(segments: Iterable[Segment]): Iterable[String] =
    Seq(s"Segments: ${segments.size}") ++ {
      segments map {
        segment =>
          val stringInfos: Slice[String] =
            Slice.from(segment.iterator(randomBoolean()), segment.keyValueCount) mapToSlice {
              keyValue =>
                keyValue.toMemory() match {
                  case response: Memory =>
                    response match {
                      case fixed: Memory.Fixed =>
                        fixed match {
                          case Memory.Put(key, value, deadline, time) =>
                            s"""PUT - ${key.readInt()} -> ${value.toOptionC.map(_.readInt())}, ${deadline.map(_.hasTimeLeft())}, ${time.time.readLong()}"""

                          case Memory.Update(key, value, deadline, time) =>
                            s"""UPDATE - ${key.readInt()} -> ${value.toOptionC.map(_.readInt())}, ${deadline.map(_.hasTimeLeft())}, ${time.time.readLong()}"""

                          case Memory.Function(key, function, time) =>
                            s"""FUNCTION - ${key.readInt()} -> ${functionStore.get(function)}, ${time.time.readLong()}"""

                          case PendingApply(key, applies) =>
                            //                        s"""
                            //                           |${key.readInt()} -> ${functionStore.find(function)}, ${time.time.readLong()}
                            //                        """.stripMargin
                            "PENDING-APPLY"

                          case Memory.Remove(key, deadline, time) =>
                            s"""REMOVE - ${key.readInt()} -> ${deadline.map(_.hasTimeLeft())}, ${time.time.readLong()}"""
                        }
                      case Memory.Range(fromKey, toKey, fromValue, rangeValue) =>
                        s"""RANGE - ${fromKey.readInt()} -> ${toKey.readInt()}, $fromValue (${fromValue.toOptionS.map(Value.hasTimeLeft)}), $rangeValue (${Value.hasTimeLeft(rangeValue)})"""
                    }
                }
            }

          s"""
             |segment: ${segment.path}
             |${stringInfos.mkString("\n")}
             |""".stripMargin + "\n"
      }
    }


  implicit class ReopenSegment(segment: PersistentSegment)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                           timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                           sweeper: TestSweeper,
                                                           segmentIO: SegmentReadIO = SegmentReadIO.random) {

    import sweeper._

    implicit val keyOrders: SegmentKeyOrders =
      SegmentKeyOrders(keyOrder)

    def tryReopen: PersistentSegment =
      tryReopen(segment.path)

    def tryReopen(path: Path): PersistentSegment = {
      val reopenedSegment =
        PersistentSegment(
          path = path,
          formatId = segment.formatId,
          createdInLevel = segment.createdInLevel,
          segmentRefCacheLife = randomSegmentRefCacheLife(),
          mmap = MMAP.randomForSegment(),
          minKey = segment.minKey,
          maxKey = segment.maxKey,
          segmentSize = segment.segmentSize,
          minMaxFunctionId = segment.minMaxFunctionId,
          updateCount = segment.updateCount,
          rangeCount = segment.rangeCount,
          putCount = segment.putCount,
          putDeadlineCount = segment.putDeadlineCount,
          keyValueCount = segment.keyValueCount,
          nearestExpiryDeadline = segment.nearestPutDeadline,
          copiedFrom = someOrNone(segment)
        ).sweep()

      segment.close()
      reopenedSegment
    }

    def reopen: PersistentSegment =
      tryReopen.runRandomIO.get

    def reopen(path: Path): PersistentSegment =
      tryReopen(path).runRandomIO.get

    def get(key: Slice[Byte]): KeyValueOption =
      segment.get(key, ThreadReadState.random)

    def get(key: Int): KeyValueOption =
      segment.get(key, ThreadReadState.random)

    def higher(key: Int): KeyValueOption =
      segment.higher(key, ThreadReadState.random)

    def higher(key: Slice[Byte]): KeyValueOption =
      segment.higher(key, ThreadReadState.random)

    def lower(key: Int): KeyValueOption =
      segment.lower(key, ThreadReadState.random)

    def lower(key: Slice[Byte]): KeyValueOption =
      segment.lower(key, ThreadReadState.random)
  }


  implicit class HigherImplicits(higher: Higher.type) {
    def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                timeOrder: TimeOrder[Slice[Byte]],
                                currentReader: CurrentWalker,
                                nextReader: NextWalker,
                                functionStore: CoreFunctionStore): IO[swaydb.Error.Level, Option[KeyValue.Put]] =
      IO.Defer(Higher(key, ThreadReadState.random, Seek.Current.Read(Int.MinValue), Seek.Next.Read).toOptionPut).runIO
  }

  implicit class LowerImplicits(higher: Lower.type) {
    def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                timeOrder: TimeOrder[Slice[Byte]],
                                currentReader: CurrentWalker,
                                nextReader: NextWalker,
                                functionStore: CoreFunctionStore): IO[swaydb.Error.Level, Option[KeyValue.Put]] =
      IO.Defer(Lower(key, ThreadReadState.random, Seek.Current.Read(Int.MinValue), Seek.Next.Read).toOptionPut).runIO
  }

  implicit class AssignablesImplicits(keyValues: ListBuffer[Assignable]) {

    /**
     * Ensures that the [[keyValues]] contains only expanded [[KeyValue]]s
     * and no collections.
     */
    def expectKeyValues(): Iterable[KeyValue] =
      keyValues collect {
        case collection: Assignable.Collection =>
          fail(s"Expected KeyValue found ${collection.getClass} with ${collection.keyValueCount} key-values.")

        case keyValue: KeyValue =>
          keyValue

      }

    def expectSegments(): Iterable[Segment] =
      keyValues collect {
        case collection: Assignable.Collection =>
          collection match {
            case segment: Segment =>
              segment

            case other =>
              fail(s"Expected ${Segment.productPrefix} found ${other.getClass}.")
          }

        case keyValue: KeyValue =>
          fail(s"Expected ${Segment.productPrefix} found ${keyValue.getClass}.")
      }

    def expectSegmentRefs(): Iterable[SegmentRef] =
      keyValues collect {
        case collection: Assignable.Collection =>
          collection match {
            case segment: SegmentRef =>
              segment

            case other =>
              fail(s"Expected ${SegmentRef.productPrefix} found ${other.getClass}.")
          }

        case keyValue: KeyValue =>
          fail(s"Expected ${SegmentRef.productPrefix} found ${keyValue.getClass}.")
      }
  }

  implicit class TransientSegmentPersistentImplicits(segment: TransientSegment.Persistent) {

    def persist(pathDistributor: PathsDistributor,
                segmentRefCacheLife: SegmentRefCacheLife = randomSegmentRefCacheLife(),
                mmap: MMAP.Segment = MMAP.randomForSegment())(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                              idGenerator: IDGenerator,
                                                              segmentReadIO: SegmentReadIO,
                                                              timeOrder: TimeOrder[Slice[Byte]],
                                                              testCaseSweeper: TestSweeper): IO[Error.Segment, Slice[PersistentSegment]] =
      Slice(segment).persist(
        pathDistributor = pathDistributor,
        segmentRefCacheLife = segmentRefCacheLife,
        mmap = mmap
      )
  }

  implicit class TransientSegmentsImplicits(segments: Slice[TransientSegment.Persistent]) {

    def persist(pathDistributor: PathsDistributor,
                segmentRefCacheLife: SegmentRefCacheLife = randomSegmentRefCacheLife(),
                mmap: MMAP.Segment = MMAP.randomForSegment())(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                              idGenerator: IDGenerator,
                                                              segmentReadIO: SegmentReadIO,
                                                              timeOrder: TimeOrder[Slice[Byte]],
                                                              testCaseSweeper: TestSweeper): IO[Error.Segment, Slice[PersistentSegment]] = {
      //      import testCaseSweeper._
      //
      //      val persistedSegments =
      //        SegmentWritePersistentIO.persistTransient(
      //          pathsDistributor = pathDistributor,
      //          segmentRefCacheLife = segmentRefCacheLife,
      //          mmap = mmap,
      //          transient = segments
      //        )
      //
      //      persistedSegments.foreach(_.foreach(_.sweep()))
      //
      //      persistedSegments map {
      //        persistedSegments =>
      //          Slice.from(persistedSegments, persistedSegments.size)
      //      }
      ???
    }
  }


  implicit class TestSegmentImplicits(segment: Segment) {

    import swaydb.testkit.RunThis._

    def put(headGap: Iterable[KeyValue],
            tailGap: Iterable[KeyValue],
            newKeyValues: Iterator[Assignable],
            removeDeletes: Boolean,
            createdInLevel: Int,
            valuesConfig: ValuesBlockConfig,
            sortedIndexConfig: SortedIndexBlockConfig,
            binarySearchIndexConfig: BinarySearchIndexBlockConfig,
            hashIndexConfig: HashIndexBlockConfig,
            bloomFilterConfig: BloomFilterBlockConfig,
            segmentConfig: SegmentBlockConfig,
            pathDistributor: PathsDistributor,
            segmentRefCacheLife: SegmentRefCacheLife,
            mmapSegment: MMAP.Segment)(implicit idGenerator: IDGenerator,
                                       executionContext: ExecutionContext,
                                       keyOrder: KeyOrder[Slice[Byte]],
                                       segmentReadIO: SegmentReadIO,
                                       timeOrder: TimeOrder[Slice[Byte]],
                                       testCaseSweeper: TestSweeper,
                                       compactionActor: SegmentCompactionIO.Actor): DefIO[SegmentOption, Slice[Segment]] = {
      def toMemory(keyValue: KeyValue) = if (removeDeletes) KeyValueGrouper.toLastLevelOrNull(keyValue) else keyValue.toMemory()

      segment match {
        case segment: MemorySegment =>

          val putResult =
            segment.put(
              headGap = ListBuffer(Assignable.Stats(MergeStats.memoryBuilder(headGap)(toMemory))),
              tailGap = ListBuffer(Assignable.Stats(MergeStats.memoryBuilder(tailGap)(toMemory))),
              newKeyValues = newKeyValues,
              removeDeletes = removeDeletes,
              createdInLevel = createdInLevel,
              segmentConfig = segmentConfig
            ).awaitInf

          putResult.input match {
            case MemorySegment.Null =>
              DefIO(
                input = Segment.Null,
                output = putResult.output.toSlice
              )

            case segment: MemorySegment =>
              DefIO(
                input = segment,
                output = putResult.output.toSlice
              )
          }

        case segment: PersistentSegment =>

          val putResult =
            segment.put(
              headGap = ListBuffer(Assignable.Stats(MergeStats.persistentBuilder(headGap)(toMemory))),
              tailGap = ListBuffer(Assignable.Stats(MergeStats.persistentBuilder(tailGap)(toMemory))),
              newKeyValues = newKeyValues,
              removeDeletes = removeDeletes,
              createdInLevel = createdInLevel,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig,
              pathsDistributor = pathDistributor,
              segmentRefCacheLife = segmentRefCacheLife,
              mmap = mmapSegment
            ).awaitInf

          putResult.input match {
            case PersistentSegment.Null =>
              DefIO(
                input = Segment.Null,
                output = putResult.output.toSlice
              )

            case segment: PersistentSegment =>
              DefIO(
                input = segment,
                output = putResult.output.toSlice
              )
          }

      }
    }

    def refresh(removeDeletes: Boolean,
                createdInLevel: Int,
                valuesConfig: ValuesBlockConfig,
                sortedIndexConfig: SortedIndexBlockConfig,
                binarySearchIndexConfig: BinarySearchIndexBlockConfig,
                hashIndexConfig: HashIndexBlockConfig,
                bloomFilterConfig: BloomFilterBlockConfig,
                segmentConfig: SegmentBlockConfig,
                pathDistributor: PathsDistributor)(implicit idGenerator: IDGenerator,
                                                   executionContext: ExecutionContext,
                                                   keyOrder: KeyOrder[Slice[Byte]],
                                                   segmentReadIO: SegmentReadIO,
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   testCaseSweeper: TestSweeper): Slice[Segment] =
      segment match {
        case segment: MemorySegment =>
          segment.refresh(
            removeDeletes = removeDeletes,
            createdInLevel = createdInLevel,
            segmentConfig = segmentConfig
          ).output

        case segment: PersistentSegment =>
          val putResult =
            segment.refresh(
              removeDeletes = removeDeletes,
              createdInLevel = createdInLevel,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig
            ).awaitInf.output

          putResult.persist(pathDistributor).get
      }
  }

  def randomBuilder(enablePrefixCompressionForCurrentWrite: Boolean = randomBoolean(),
                    prefixCompressKeysOnly: Boolean = randomBoolean(),
                    compressDuplicateValues: Boolean = randomBoolean(),
                    enableAccessPositionIndex: Boolean = randomBoolean(),
                    optimiseForReverseIteration: Boolean = randomBoolean(),
                    allocateBytes: Int = 10000): EntryWriter.Builder = {
    val builder =
      EntryWriter.Builder(
        prefixCompressKeysOnly = prefixCompressKeysOnly,
        compressDuplicateValues = compressDuplicateValues,
        enableAccessPositionIndex = enableAccessPositionIndex,
        optimiseForReverseIteration = optimiseForReverseIteration,
        bytes = Slice.allocate[Byte](allocateBytes)
      )

    builder.enablePrefixCompressionForCurrentWrite = enablePrefixCompressionForCurrentWrite
    builder
  }

}
