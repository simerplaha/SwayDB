package swaydb.core.segment

import org.scalactic.Equality
import org.scalatest.exceptions.TestFailedException
import org.scalatest.matchers.should.Matchers._
import swaydb.config.{Atomic, MMAP, OptimiseWrites, SegmentRefCacheLife}
import swaydb.config.CoreConfigTestKit._
import swaydb.core.segment.data._
import swaydb.core.segment.io.{SegmentCompactionIO, SegmentReadIO}
import swaydb.{Error, IO}
import swaydb.core.level.zero.{LevelZero, LevelZeroLogCache}
import swaydb.core.log.LogEntry
import swaydb.core.segment.data.merge._
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.effect.EffectTestKit._
import swaydb.slice.{Reader, Slice, SliceReader}
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.TestKit.{randomBoolean, randomIntMax, someOrNone}
import swaydb.IOValues._
import swaydb.config.compaction.PushStrategy
import swaydb.core.segment.block.{Block, BlockCache}
import swaydb.core.segment.block.bloomfilter.{BloomFilterBlock, BloomFilterBlockConfig, BloomFilterBlockOffset, BloomFilterBlockState}
import swaydb.core.segment.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.core.segment.ref.search.{KeyMatcher, SegmentSearcher, ThreadReadState}
import swaydb.core.segment.ref.search.KeyMatcher.Result
import org.scalatest.OptionValues._
import swaydb.core.{SegmentBlocks, TestExecutionContext, TestSweeper}
import swaydb.core.file.FileReader
import swaydb.core.level.{Level, LevelRef, NextLevel}
import swaydb.core.level.seek._
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
import swaydb.core.segment.block.hashindex.HashIndexBlockConfig
import swaydb.core.segment.block.segment.{SegmentBlock, SegmentBlockCache, SegmentBlockConfig, SegmentBlockOffset}
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockConfig}
import swaydb.core.segment.block.values.ValuesBlockConfig
import swaydb.core.segment.block.SegmentBlockTestKit._
import swaydb.core.segment.cache.sweeper.MemorySweeper
import swaydb.core.segment.data.KeyValueTestKit._
import swaydb.core.segment.data.Memory.PendingApply
import swaydb.core.segment.entry.id.BaseEntryIdFormatA
import swaydb.core.segment.entry.writer.EntryWriter
import swaydb.core.segment.ref.SegmentRef
import swaydb.core.util.DefIO
import swaydb.effect.{Effect, IOStrategy}
import swaydb.slice.SliceTestKit._
import swaydb.utils.IDGenerator
import swaydb.Error.Segment.ExceptionHandler
import swaydb.core.segment.block.segment.SegmentBlockOffset.SegmentBlockOps
import swaydb.serializers._
import swaydb.serializers.Default._
import swaydb.SliceIOImplicits._
import swaydb.core.TestSweeper._
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockOffset.BloomFilterBlockOps
import swaydb.core.segment.data.merge.{KeyValueGrouper, KeyValueMerger}
import swaydb.testkit.RunThis._

import java.nio.file.{Path, Paths}
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration.Deadline
import scala.concurrent.ExecutionContext
import scala.util.{Random, Try}

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


  def assertSkipListMerge(newKeyValues: Iterable[KeyValue],
                          oldKeyValues: Iterable[KeyValue],
                          expected: Memory): Iterable[Memory] =
    assertSkipListMerge(newKeyValues, oldKeyValues, Slice(expected))

  def assertSkipListMerge(newKeyValues: Iterable[KeyValue],
                          oldKeyValues: Iterable[KeyValue],
                          expected: Iterable[KeyValue])(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                        timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long): Iterable[Memory] = {
    import swaydb.core.log.serialiser.LevelZeroLogEntryWriter.Level0LogEntryPutWriter
    implicit val optimiseWrites: OptimiseWrites = OptimiseWrites.random
    implicit val atomic: Atomic = Atomic.random
    val cache = LevelZeroLogCache.builder.create()
    (oldKeyValues ++ newKeyValues).map(_.toMemory()) foreach {
      memory =>
        //        if (randomBoolean())
        //          cache.writeNonAtomic(LogEntry.Put(memory.key, memory))
        //        else
        cache.writeAtomic(LogEntry.Put(memory.key, memory))
    }

    val cachedKeyValues = cache.skipList.values()
    cachedKeyValues shouldBe expected.map(_.toMemory()).toList
    cachedKeyValues
  }

  def assertMerge(newKeyValue: KeyValue,
                  oldKeyValue: KeyValue,
                  expected: Slice[Memory],
                  isLastLevel: Boolean = false)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                timeOrder: TimeOrder[Slice[Byte]]): Iterable[Memory] =
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), expected, isLastLevel)

  def assertMerge(newKeyValues: Slice[KeyValue],
                  oldKeyValues: Slice[KeyValue],
                  expected: Slice[KeyValue],
                  isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]]): Iterable[Memory] = {
    val builder = MergeStats.random()

    KeyValueMerger.merge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      stats = builder,
      isLastLevel = isLastLevel,
      initialiseIteratorsInOneSeek = randomBoolean()
    )

    val result = builder.keyValues

    if (expected.size == 0) {
      result.isEmpty shouldBe true
    } else {
      result should have size expected.size
      result.toList should contain inOrderElementsOf expected
    }
    result
  }

  //  def assertMerge(newKeyValue: KeyValue,
  //                  oldKeyValue: KeyValue,
  //                  expected: KeyValue,
  //                  lastLevelExpect: KeyValue)(implicit keyOrder: KeyOrder[Slice[Byte]],
  //                                             timeOrder: TimeOrder[Slice[Byte]]): Iterable[Memory] =
  //    assertMerge(newKeyValue, oldKeyValue, Slice(expected), Slice(lastLevelExpect))

  def assertMerge(newKeyValue: KeyValue,
                  oldKeyValue: KeyValue,
                  expected: KeyValue,
                  lastLevelExpect: KeyValueOption)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    //    println("*** Expected assert ***")
    assertMerge(newKeyValue, oldKeyValue, Slice(expected), lastLevelExpect.toOption.map(Slice(_)).getOrElse(Slice.empty))
    //println("*** Skip list assert ***")
    assertSkipListMerge(Slice(newKeyValue), Slice(oldKeyValue), Slice(expected))
  }

  def assertMerge(newKeyValues: Slice[KeyValue],
                  oldKeyValues: Slice[KeyValue],
                  expected: Slice[KeyValue],
                  lastLevelExpect: Slice[KeyValue])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                    timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    //    println("*** Expected assert ***")
    assertMerge(newKeyValues, oldKeyValues, expected, isLastLevel = false)
    //println("*** Expected last level ***")
    assertMerge(newKeyValues, oldKeyValues, lastLevelExpect, isLastLevel = true)
    //println("*** Skip list assert ***")
    assertSkipListMerge(newKeyValues, oldKeyValues, expected)
  }

  def assertMerge(newKeyValue: KeyValue,
                  oldKeyValue: KeyValue,
                  expected: Slice[KeyValue],
                  lastLevelExpect: Slice[KeyValue])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                    timeOrder: TimeOrder[Slice[Byte]]): Iterable[Memory] = {
    //    println("*** Last level = false ***")
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), expected, isLastLevel = false)
    //println("*** Last level = true ***")
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), lastLevelExpect, isLastLevel = true)
  }

  def assertMerge(newKeyValues: Slice[KeyValue],
                  oldKeyValues: Slice[KeyValue],
                  expected: Memory,
                  isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]]): Iterable[Memory] =
    assertMerge(newKeyValues, oldKeyValues, Slice(expected), isLastLevel)

  def assertMerge(newKeyValue: Memory.Function,
                  oldKeyValue: Memory.PendingApply,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    FunctionMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Function,
                  oldKeyValue: Memory.Fixed,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    FunctionMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Remove,
                  oldKeyValue: Memory.Fixed,
                  expected: KeyValue.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    RemoveMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Put,
                  oldKeyValue: Memory.Fixed,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    PutMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))

    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Update,
                  oldKeyValue: Memory.Fixed,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    UpdateMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Update,
                  oldKeyValue: Memory.PendingApply,
                  expected: KeyValue.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    UpdateMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))

    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Fixed,
                  oldKeyValue: Memory.PendingApply,
                  expected: Memory.PendingApply,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))
    //todo merge with persistent
  }


  implicit class SliceKeyValueImplicits(actual: Iterable[KeyValue]) {
    def shouldBe(expected: Iterable[KeyValue]): Unit = {
      val unzipActual = actual
      val unzipExpected = expected
      unzipActual.size shouldBe unzipExpected.size
      unzipActual.zip(unzipExpected) foreach {
        case (left, right) =>
          left shouldBe right
      }
    }

    def shouldBe(expected: Iterator[KeyValue]): Unit =
      actual shouldBe expected.toList
  }

  implicit val keyMatcherResultEquality: Equality[KeyMatcher.Result] =
    new Equality[KeyMatcher.Result] {
      override def areEqual(a: KeyMatcher.Result, other: Any): Boolean =
        a match {
          case result: Result.Matched =>
            other match {
              case other: Result.Matched =>
                other.result == result.result

              case _ =>
                false
            }

          case Result.BehindStopped =>
            other == Result.BehindStopped

          case Result.AheadOrNoneOrEnd =>
            other == Result.AheadOrNoneOrEnd

          case Result.BehindFetchNext =>
            other == Result.BehindFetchNext
        }
    }


  implicit class SegmentsImplicits(actual: Iterable[Segment]) {

    def shouldBe(expected: Iterable[Segment]): Unit =
      actual.zip(expected) foreach {
        case (left, right) =>
          left shouldBe right
      }

    def shouldHaveSameKeyValuesAs(expected: Iterable[Segment]): Unit =
      actual.flatMap(_.iterator(randomBoolean())).runRandomIO.right.value shouldBe expected.flatMap(_.iterator(randomBoolean())).runRandomIO.right.value
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
        assertReads(Slice.from(expected.iterator(randomBoolean()), expected.keyValueCount).runRandomIO.right.value, actual)
    }

    def shouldContainAll(keyValues: Slice[KeyValue]): Unit =
      keyValues.foreach {
        keyValue =>
          actual.get(keyValue.key, ThreadReadState.random).runRandomIO.right.value.getUnsafe shouldBe keyValue
      }
  }



  def assertHigher(keyValuesIterable: Iterable[KeyValue],
                   level: LevelRef): Unit = {
    val keyValues = keyValuesIterable.toSlice
    assertHigher(keyValues, getHigher = key => IO.Defer(level.higher(key, ThreadReadState.random).toOptionPut).runIO)
  }

  def assertLower(keyValuesIterable: Iterable[KeyValue],
                  level: LevelRef) = {
    val keyValues = keyValuesIterable.toSlice

    @tailrec
    def assertLowers(index: Int): Unit = {
      if (index > keyValues.size - 1) {
        //end
      } else if (index == 0) {
        level.lower(keyValues(0).key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
        assertLowers(index + 1)
      } else {
        try {
          val lower = level.lower(keyValues(index).key, ThreadReadState.random).runRandomIO.right.value.toOptionPut

          val expectedLowerKeyValue =
            (0 until index).reverse collectFirst {
              case i if unexpiredPuts(Slice(keyValues(i))).nonEmpty =>
                keyValues(i)
            }

          if (lower.nonEmpty) {
            expectedLowerKeyValue shouldBe defined
            lower.get.key shouldBe expectedLowerKeyValue.get.key
            lower.get.getOrFetchValue.runRandomIO.right.value shouldBe expectedLowerKeyValue.get.getOrFetchValue.asSliceOption()
          } else {
            expectedLowerKeyValue shouldBe empty
          }
        } catch {
          case exception: Exception =>
            exception.printStackTrace()
            fail(exception)
        }
        assertLowers(index + 1)
      }
    }

    assertLowers(0)
  }

  def assertGet(keyValues: Slice[Memory],
                rawSegmentReader: Reader,
                segmentIO: SegmentReadIO = SegmentReadIO.random)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                 blockCacheMemorySweeper: Option[MemorySweeper.Block]) = {
    implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = SegmentKeyOrders(keyOrder).partialKeyOrder
    val blocks = readBlocksFromReader(rawSegmentReader.copy()).get

    keyValues.par foreach {
      keyValue =>
        //        val key = keyValue.minKey.readInt()
        //        if (key % 100 == 0)
        //          println(s"Key: $key")
        SegmentSearcher.searchRandom(
          key = keyValue.key,
          start = Persistent.Null,
          end = Persistent.Null,
          keyValueCount = blocks.footer.keyValueCount,
          hashIndexReaderOrNull = blocks.hashIndexReader.map(_.copy()).orNull,
          binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.map(_.copy()).orNull,
          sortedIndexReader = blocks.sortedIndexReader.copy(),
          valuesReaderOrNull = blocks.valuesReader.map(_.copy()).orNull,
          hasRange = blocks.footer.hasRange
        ).runRandomIO.right.value.getS shouldBe keyValue
    }
  }

  def assertBloom(keyValues: Slice[Memory],
                  bloom: BloomFilterBlockState) = {
    val bloomFilter = Block.unblock[BloomFilterBlockOffset, BloomFilterBlock](bloom.compressibleBytes)

    keyValues.par.count {
      keyValue =>
        BloomFilterBlock.mightContain(
          comparableKey = keyValue.key,
          reader = bloomFilter
        )
    } should be >= (keyValues.size * 0.90).toInt

    assertBloomNotContains(bloom)
  }

  def assertBloom(keyValues: Slice[KeyValue],
                  segment: Segment) = {
    keyValues.par.count {
      keyValue =>
        IO.Defer(segment.mightContainKey(keyValue.key, ThreadReadState.random)).runRandomIO.right.value
    } shouldBe keyValues.size

    if (segment.hasBloomFilter() || segment.memory)
      assertBloomNotContains(segment)
  }

  def assertBloom(keyValues: Slice[Memory],
                  bloomFilterReader: UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock]) = {
    val unzipedKeyValues = keyValues

    unzipedKeyValues.par.count {
      keyValue =>
        BloomFilterBlock.mightContain(
          comparableKey = keyValue.key,
          reader = bloomFilterReader.copy()
        )
    } shouldBe unzipedKeyValues.size

    assertBloomNotContains(bloomFilterReader)
  }

  def assertBloomNotContains(bloomFilterReader: UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock]) =
    (1 to 1000).par.count {
      _ =>
        BloomFilterBlock.mightContain(randomBytesSlice(100), bloomFilterReader.copy()).runRandomIO.right.value
    } should be <= 300

  def assertBloomNotContains(segment: Segment) =
    if (segment.hasBloomFilter())
      (1 to 1000).par.count {
        _ =>
          segment.mightContainKey(randomBytesSlice(100), ThreadReadState.random).runRandomIO.right.value
      } should be < 1000

  def assertBloomNotContains(bloom: BloomFilterBlockState)(implicit ec: ExecutionContext = TestExecutionContext.executionContext) =
    runThisParallel(1000.times) {
      val bloomFilter = Block.unblock[BloomFilterBlockOffset, BloomFilterBlock](bloom.compressibleBytes)
      BloomFilterBlock.mightContain(
        comparableKey = randomBytesSlice(randomIntMax(1000) min 100),
        reader = bloomFilter.copy()
      ).runRandomIO.right.value shouldBe false
    }

  def assertReads(keyValues: Slice[KeyValue],
                  segment: Segment) = {
    val asserts = Seq(() => assertGet(keyValues, segment), () => assertHigher(keyValues, segment), () => assertLower(keyValues, segment))
    Random.shuffle(asserts).par.foreach(_ ())
  }

  def assertAllSegmentsCreatedInLevel(level: Level) =
    level.segments() foreach (_.createdInLevel.runRandomIO.right.value shouldBe level.levelNumber)

  def assertReads(keyValues: Iterable[KeyValue],
                  level: LevelRef) = {
    val asserts = Seq(() => assertGet(keyValues, level), () => assertHigher(keyValues, level), () => assertLower(keyValues, level))
    Random.shuffle(asserts).par.foreach(_ ())
  }

  def assertNoneReads(keyValues: Iterable[KeyValue],
                      level: LevelRef) = {
    val asserts = Seq(() => assertGetNone(keyValues, level), () => assertHigherNone(keyValues, level), () => assertLowerNone(keyValues, level))
    Random.shuffle(asserts).par.foreach(_ ())
  }

  def assertEmpty(keyValues: Iterable[KeyValue],
                  level: LevelRef) = {
    val asserts =
      Seq(
        () => assertGetNone(keyValues, level),
        () => assertHigherNone(keyValues, level),
        () => assertLowerNone(keyValues, level),
        () => assertEmptyHeadAndLast(level)
      )
    Random.shuffle(asserts).par.foreach(_ ())
  }

  def assertGetFromThisLevelOnly(keyValues: Iterable[KeyValue],
                                 level: Level) =
    keyValues foreach {
      keyValue =>
        try {
          val actual = level.getFromThisLevel(keyValue.key, ThreadReadState.random).runRandomIO.right.value.getUnsafe
          actual.getOrFetchValue shouldBe keyValue.getOrFetchValue
        } catch {
          case ex: Exception =>
            println(
              "Test failed for key: " + keyValue.key.readInt() +
                s" indexEntryDeadline: ${keyValue.toMemory.indexEntryDeadline.map(_.hasTimeLeft())}" +
                s" class: ${keyValue.getClass.getSimpleName}"
            )
            throw ex
        }
    }

  def assertEmptyHeadAndLast(level: LevelRef) =
    Seq(
      () => IO.Defer(level.head(ThreadReadState.random).toOptionPut).runIO.get shouldBe empty,
      () => IO.Defer(level.last(ThreadReadState.random).toOptionPut).runIO.get shouldBe empty
    ).runThisRandomlyInParallel

  def assertReads(keyValues: Slice[Memory],
                  segmentReader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                         blockCacheMemorySweeper: Option[MemorySweeper.Block]) = {

    //read fullIndex
    readAll(segmentReader.copy()).runRandomIO.right.value shouldBe keyValues
    //    //find each KeyValue using all Matchers
    assertGet(keyValues, segmentReader.copy())
    assertLower(keyValues, segmentReader.copy())
    assertHigher(keyValues, segmentReader.copy())
  }

  def assertGet(keyValues: Iterable[KeyValue],
                segment: Segment): Unit =
    runAssertGet(
      keyValues = keyValues,
      segment = segment,
      parallel = true
    )

  def assertGetSequential(keyValues: Iterable[KeyValue],
                          segment: Segment): Unit =
    runAssertGet(
      keyValues = keyValues,
      segment = segment,
      parallel = false
    )

  private def runAssertGet(keyValues: Iterable[KeyValue],
                           segment: Segment,
                           parallel: Boolean = true) = {
    val parallelKeyValues =
      if (parallel)
        keyValues.par
      else
        keyValues

    parallelKeyValues foreach {
      keyValue =>
        //        val intKey = keyValue.key.readInt()
        //        if (intKey % 1000 == 0)
        //          println("Get: " + intKey)
        try
          IO.Defer(segment.get(keyValue.key, ThreadReadState.random)).runRandomIO.value.getUnsafe shouldBe keyValue
        catch {
          case exception: Exception =>
            println(s"Failed to get: ${keyValue.key.readInt()}")
            throw exception
        }
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

  @tailrec
  def dump(level: NextLevel): Unit =
    level.nextLevel match {
      case Some(nextLevel) =>
        val data = Seq(s"\nLevel: ${level.rootPath}\n") ++ dump(level.segments())

        Effect.write(
          to = Paths.get(s"/Users/simerplaha/IdeaProjects/SwayDB/core/target/dump_Level_${level.levelNumber}.txt"),
          bytes = Slice.writeString(data.mkString("\n")).toByteBufferWrap()
        )

        dump(nextLevel)

      case None =>
        val data = Seq(s"\nLevel: ${level.rootPath}\n") ++ dump(level.segments())

        Effect.write(
          to = Paths.get(s"/Users/simerplaha/IdeaProjects/SwayDB/core/target/dump_Level_${level.levelNumber}.txt"),
          bytes = Slice.writeString(data.mkString("\n")).toByteBufferWrap()
        )
    }

  def assertGet(keyValues: Iterable[KeyValue],
                level: LevelRef) =
    keyValues foreach {
      keyValue =>
        try
          level.get(keyValue.key, ThreadReadState.random).runRandomIO.get.toOptionPut match {
            case Some(got) =>
              got shouldBe keyValue

            case None =>
              unexpiredPuts(Slice(keyValue)) should have size 0
          }
        catch {
          case ex: Throwable =>
            println(
              "Test failed for key: " + keyValue.key.readInt() +
                s" expired: ${keyValue.toMemory().indexEntryDeadline.map(_.hasTimeLeft())}" +
                s" class: ${keyValue.getClass.getSimpleName}"
            )
            fail(ex)
        }
    }

  def assertGetNone(keyValues: Iterable[KeyValue],
                    level: LevelRef) =
    keyValues foreach {
      keyValue =>
        try
          level.get(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
        catch {
          case ex: Exception =>
            println(
              "Test failed for key: " + keyValue.key.readInt() +
                s" indexEntryDeadline: ${keyValue.toMemory().indexEntryDeadline.map(_.hasTimeLeft())}" +
                s" class: ${keyValue.getClass.getSimpleName}"
            )
            throw ex
        }
    }

  def assertGetNone(keyValues: Iterable[KeyValue],
                    level: LevelZero) =
    keyValues.par foreach {
      keyValue =>
        level.get(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe None
    }

  def assertGetNone(keys: Range,
                    level: LevelRef) =
    keys.par foreach {
      key =>
        level.get(Slice.writeInt(key), ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
    }

  def assertGetNone(keys: List[Int],
                    level: LevelRef) =
    keys.par foreach {
      key =>
        level.get(Slice.writeInt(key), ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
    }

  def assertGetNoneButLast(keyValues: Iterable[KeyValue],
                           level: LevelRef) = {
    keyValues.dropRight(1).par foreach {
      keyValue =>
        level.get(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
    }

    keyValues
      .lastOption
      .map(_.key)
      .flatMap(level.get(_, ThreadReadState.random).runRandomIO.right.value.toOptionPut.map(_.toMemory())) shouldBe keyValues.lastOption
  }

  def assertGetNoneFromThisLevelOnly(keyValues: Iterable[KeyValue],
                                     level: Level) =
    keyValues foreach {
      keyValue =>
        level.getFromThisLevel(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOption shouldBe empty
    }

  /**
   * If all key-values are non put key-values then searching higher for each key-value
   * can result in a very long search time. Considering using shuffleTake which
   * randomly selects a batch to assert for None higher.
   */
  def assertHigherNone(keyValues: Iterable[KeyValue],
                       level: LevelRef,
                       shuffleTake: Option[Int] = None) = {
    val unzipedKeyValues = keyValues
    val keyValuesToAssert = shuffleTake.map(Random.shuffle(unzipedKeyValues).take) getOrElse unzipedKeyValues
    keyValuesToAssert foreach {
      keyValue =>
        try {
          //          println(keyValue.key.readInt())
          level.higher(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
          //          println
        } catch {
          case ex: Exception =>
            println(
              "Test failed for key: " + keyValue.key.readInt() +
                s" indexEntryDeadline: ${keyValue.toMemory().indexEntryDeadline.map(_.hasTimeLeft())}" +
                s" class: ${keyValue.getClass.getSimpleName}"
            )
            throw ex
        }
    }
  }

  def assertLowerNone(keyValues: Iterable[KeyValue],
                      level: LevelRef,
                      shuffleTake: Option[Int] = None) = {
    val unzipedKeyValues = keyValues
    val keyValuesToAssert = shuffleTake.map(Random.shuffle(unzipedKeyValues).take) getOrElse unzipedKeyValues
    keyValuesToAssert foreach {
      keyValue =>
        try {
          level.lower(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
        } catch {
          case ex: Exception =>
            println(
              "Test failed for key: " + keyValue.key.readInt() +
                s" indexEntryDeadline: ${keyValue.toMemory().indexEntryDeadline.map(_.hasTimeLeft())}" +
                s" class: ${keyValue.getClass.getSimpleName}"
            )
            throw ex
        }
    }
  }

  def assertLower(keyValues: Slice[Memory],
                  reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                  blockCacheMemorySweeper: Option[MemorySweeper.Block]) = {
    implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = SegmentKeyOrders(keyOrder).partialKeyOrder

    val blocks = readBlocksFromReader(reader.copy()).get

    @tailrec
    def assertLowers(index: Int): Unit = {
      //      println(s"assertLowers : ${index}")
      if (index > keyValues.size - 1) {
        //end
      } else if (index == 0) {
        keyValues(index) match {
          case range: Memory.Range =>
            SegmentSearcher.searchLower(
              key = range.fromKey,
              start = Persistent.Null,
              end = Persistent.Null,
              keyValueCount = blocks.footer.keyValueCount,
              binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReaderOrNull = blocks.valuesReader.orNull
            ).runRandomIO.right.value.toOption shouldBe empty

            (range.fromKey.readInt() + 1 to range.toKey.readInt()) foreach {
              key =>
                SegmentSearcher.searchLower(
                  key = Slice.writeInt(key),
                  start = Persistent.Null,
                  end = Persistent.Null,
                  keyValueCount = blocks.footer.keyValueCount,
                  binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
                  sortedIndexReader = blocks.sortedIndexReader,
                  valuesReaderOrNull = blocks.valuesReader.orNull
                ).runRandomIO.right.value.getUnsafe shouldBe range
            }

          case _ =>
            SegmentSearcher.searchLower(
              key = keyValues(index).key,
              start = Persistent.Null,
              end = Persistent.Null,
              keyValueCount = blocks.footer.keyValueCount,
              binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReaderOrNull = blocks.valuesReader.orNull
            ).runRandomIO.right.value.toOption shouldBe empty
        }
        assertLowers(index + 1)
      } else {
        val expectedLowerKeyValue = keyValues(index - 1)
        keyValues(index) match {
          case range: Memory.Range =>
            SegmentSearcher.searchLower(
              key = range.fromKey,
              start = Persistent.Null,
              end = Persistent.Null,
              keyValueCount = blocks.footer.keyValueCount,
              binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReaderOrNull = blocks.valuesReader.orNull
            ).runRandomIO.right.value.getUnsafe shouldBe expectedLowerKeyValue

            (range.fromKey.readInt() + 1 to range.toKey.readInt()) foreach {
              key =>
                SegmentSearcher.searchLower(
                  key = Slice.writeInt(key),
                  start = Persistent.Null,
                  end = Persistent.Null,
                  keyValueCount = blocks.footer.keyValueCount,
                  binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
                  sortedIndexReader = blocks.sortedIndexReader,
                  valuesReaderOrNull = blocks.valuesReader.orNull
                ).runRandomIO.right.value.getUnsafe shouldBe range
            }

          case _ =>
            SegmentSearcher.searchLower(
              key = keyValues(index).key,
              start = Persistent.Null,
              end = Persistent.Null,
              keyValueCount = blocks.footer.keyValueCount,
              binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReaderOrNull = blocks.valuesReader.orNull
            ).runRandomIO.right.value.getUnsafe shouldBe expectedLowerKeyValue
        }

        assertLowers(index + 1)
      }
    }

    assertLowers(0)
  }

  def assertHigher(keyValues: Slice[KeyValue],
                   reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                   blockCacheMemorySweeper: Option[MemorySweeper.Block]): Unit = {
    implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = SegmentKeyOrders(keyOrder).partialKeyOrder
    val blocks = readBlocksFromReader(reader).get
    assertHigher(
      keyValues,
      getHigher =
        key =>
          IO {
            SegmentSearcher.searchHigherRandomly(
              key = key,
              start = Persistent.Null,
              end = Persistent.Null,
              keyValueCount = blocks.footer.keyValueCount,
              binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.map(_.copy()).orNull,
              sortedIndexReader = blocks.sortedIndexReader.copy(),
              valuesReaderOrNull = blocks.valuesReader.map(_.copy()).orNull
            ).toOption
          }
    )
  }

  def assertLower(keyValues: Slice[KeyValue],
                  segment: Segment) = {

    @tailrec
    def assertLowers(index: Int): Unit = {
      if (index > keyValues.size - 1) {
        //end
      } else if (index == 0) {
        val actualKeyValue = keyValues(index)
        //        println(s"Lower: ${actualKeyValue.key.readInt()}")
        IO.Defer(segment.lower(actualKeyValue.key, ThreadReadState.random)).runRandomIO.right.value.toOption shouldBe empty
        assertLowers(index + 1)
      } else {
        val expectedLower = keyValues(index - 1)
        val keyValue = keyValues(index)
        //        val intKey = keyValue.key.readInt()
        //        if (intKey % 100 == 0)
        //          println(s"Lower: $intKey")
        try {
          val lower = IO.Defer(segment.lower(keyValue.key, ThreadReadState.random)).runRandomIO.right.value.getUnsafe
          lower shouldBe expectedLower
        } catch {
          case x: Exception =>
            x.printStackTrace()
            throw x
        }
        assertLowers(index + 1)
      }
    }

    assertLowers(0)
  }

  def assertHigher(keyValues: Slice[KeyValue],
                   segment: Segment): Unit =
    assertHigher(keyValues, getHigher = key => IO(IO.Defer(segment.higher(key, ThreadReadState.random)).runRandomIO.right.value.toOption))

  /**
   * Asserts that all key-values are returned in order when fetching higher in sequence.
   */
  def assertHigher(_keyValues: Iterable[KeyValue],
                   getHigher: Slice[Byte] => IO[swaydb.Error.Level, Option[KeyValue]]): Unit = {
    import KeyOrder.default._
    val keyValues = _keyValues.toArray

    //assert higher if the currently's read key-value is the last key-value
    def assertLast(keyValue: KeyValue) =
      keyValue match {
        case range: KeyValue.Range =>
          getHigher(range.fromKey).runRandomIO.right.value.value shouldBe range
          getHigher(range.toKey).runRandomIO.right.value shouldBe empty

        case keyValue =>
          getHigher(keyValue.key).runRandomIO.right.value shouldBe empty
      }

    //assert higher if the currently's read key-value is NOT the last key-value
    def assertNotLast(keyValue: KeyValue,
                      next: KeyValue,
                      nextNext: Option[KeyValue]) = {
      keyValue match {
        case range: KeyValue.Range =>
          try
            getHigher(range.fromKey).runRandomIO.right.value.value shouldBe range
          catch {
            case exception: Exception =>
              exception.printStackTrace()
              getHigher(range.fromKey).runRandomIO.right.value.value shouldBe range
              throw exception
          }
          val toKeyHigher = getHigher(range.toKey).runRandomIO.right.value
          //suppose this keyValue is Range (1 - 10), second is Put(10), third is Put(11), higher on Range's toKey(10) will return 11 and not 10.
          //but 10 will be return if the second key-value was a range key-value.
          //if the toKey is equal to expected higher's key, then the higher is the next 3rd key.
          next match {
            case next: KeyValue.Range =>
              toKeyHigher.value shouldBe next

            case _ =>
              //if the range's toKey is the same as next key, higher is next's next.
              //or if the next is group then
              if (next.key equiv range.toKey)
              //should be next next
                if (nextNext.isEmpty) //if there is no 3rd key, higher should be empty
                  toKeyHigher shouldBe empty
                else
                  try
                    toKeyHigher.value shouldBe nextNext.value
                  catch {
                    case exception: Exception =>
                      exception.printStackTrace()
                      val toKeyHigher = getHigher(range.toKey).runRandomIO.right.value
                      throw exception
                  }
              else
                try
                  toKeyHigher.value shouldBe next
                catch {
                  case exception: Exception =>
                    exception.printStackTrace()
                    val toKeyHigher = getHigher(range.toKey).runRandomIO.right.value
                    throw exception
                }
          }

        case _ =>
          Try(getHigher(keyValue.key).runRandomIO.right.value.value shouldBe next) recover {
            case _: TestFailedException =>
              unexpiredPuts(Slice(next)) should have size 0
          } get
      }
    }

    keyValues.indices foreach {
      index =>
        if (index == keyValues.length - 1) { //last index
          assertLast(keyValues(index))
        } else {
          val next = keyValues(index + 1)
          val nextNext = IO(keyValues(index + 2)).toOption
          assertNotLast(keyValues(index), next, nextNext)
        }
    }
  }

  def readAll(segment: TransientSegment.One)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): IO[swaydb.Error.Segment, Slice[KeyValue]] =
    readAll(segment.flattenSegmentBytes)

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

    readAll(segment.head.flattenSegmentBytes)
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

  def readAll(bytes: Slice[Byte])(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): IO[swaydb.Error.Segment, Slice[KeyValue]] =
    readAll(SliceReader(bytes))

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
    ).awaitInf mapToSlice {
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

  def randomIOStrategy(cacheOnAccess: Boolean = randomBoolean(),
                       includeReserved: Boolean = true): IOStrategy =
    if (randomBoolean())
      IOStrategy.SynchronisedIO(cacheOnAccess)
    else if (cacheOnAccess && includeReserved && randomBoolean())
      IOStrategy.AsyncIO(cacheOnAccess = true) //this not being stored will result in too many retries.
    else
      IOStrategy.ConcurrentIO(cacheOnAccess)

  def randomPushStrategy(): PushStrategy =
    if (randomBoolean())
      PushStrategy.Immediately
    else
      PushStrategy.OnOverflow

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

  def collapseMerge(newKeyValue: Memory.Fixed,
                    oldApplies: Slice[Value.Apply])(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =
    newKeyValue match {
      case PendingApply(_, newApplies) =>
        //PendingApplies on PendingApplies are never merged. They are just stashed in sequence of their time.
        Memory.PendingApply(newKeyValue.key, oldApplies ++ newApplies)

      case _ =>
        var count = 0
        //reversing so that order is where newer are at the head.
        val reveredApplied = oldApplies.reverse.toList
        reveredApplied.foldLeft(newKeyValue: KeyValue.Fixed) {
          case (newer, older) =>
            count += 1
            //merge as though applies were normal fixed key-values. The result should be the same.
            FixedMerger(newer, older.toMemory(newKeyValue.key)).runRandomIO.right.value match {
              case newPendingApply: KeyValue.PendingApply =>
                val resultApplies = newPendingApply.getOrFetchApplies.runRandomIO.right.value.reverse.toList ++ reveredApplied.drop(count)
                val result =
                  if (resultApplies.size == 1)
                    resultApplies.head.toMemory(newKeyValue.key)
                  else
                    Memory.PendingApply(key = newKeyValue.key, resultApplies.reverse.toSlice)
                return result

              case other =>
                other
            }
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
      tryReopen.runRandomIO.right.value

    def reopen(path: Path): PersistentSegment =
      tryReopen(path).runRandomIO.right.value

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

          putResult.persist(pathDistributor).value
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
