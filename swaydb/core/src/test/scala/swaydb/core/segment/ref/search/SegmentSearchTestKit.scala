package swaydb.core.segment.ref.search

import org.scalactic.Equality
import org.scalatest.OptionValues._
import org.scalatest.exceptions.TestFailedException
import org.scalatest.matchers.should.Matchers._
import swaydb.Error.Segment.ExceptionHandler
import swaydb.core.segment._
import swaydb.core.segment.block._
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockOffset.BloomFilterBlockOps
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.data._
import swaydb.core.segment.io.SegmentReadIO
import swaydb.effect.IOValues._
import swaydb.serializers._
import swaydb.serializers.Default._
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.TestKit._
import swaydb.IO
import swaydb.core.level.{Level, LevelRef}
import swaydb.core.segment.block.bloomfilter.{BloomFilterBlock, BloomFilterBlockOffset, BloomFilterBlockState}
import swaydb.core.segment.cache.sweeper.MemorySweeper
import swaydb.core.segment.ref.search.KeyMatcher.Result
import swaydb.core.segment.SegmentTestKit._
import swaydb.core.segment.block.BlockTestKit._
import swaydb.core.segment.data.KeyValueTestKit._
import swaydb.core.TestExecutionContext
import swaydb.core.level.seek._
import swaydb.slice.{Reader, Slice}
import swaydb.slice.SliceTestKit._
import swaydb.testkit.RunThis._

import scala.annotation.tailrec
import scala.collection.parallel.CollectionConverters._
import scala.concurrent.ExecutionContext
import scala.util.{Random, Try}

object SegmentSearchTestKit {

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
        level.lower(keyValues(0).key, ThreadReadState.random).runRandomIO.get.toOptionPut shouldBe empty
        assertLowers(index + 1)
      } else {
        try {
          val lower = level.lower(keyValues(index).key, ThreadReadState.random).runRandomIO.get.toOptionPut

          val expectedLowerKeyValue =
            (0 until index).reverse collectFirst {
              case i if unexpiredPuts(Slice(keyValues(i))).nonEmpty =>
                keyValues(i)
            }

          if (lower.nonEmpty) {
            expectedLowerKeyValue shouldBe defined
            lower.get.key shouldBe expectedLowerKeyValue.get.key
            lower.get.getOrFetchValue.runRandomIO.get shouldBe expectedLowerKeyValue.get.getOrFetchValue.asSliceOption()
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
        ).runRandomIO.get.getS shouldBe keyValue
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
        IO.Defer(segment.mightContainKey(keyValue.key, ThreadReadState.random)).runRandomIO.get
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
        BloomFilterBlock.mightContain(randomBytesSlice(100), bloomFilterReader.copy()).runRandomIO.get
    } should be <= 300

  def assertBloomNotContains(segment: Segment) =
    if (segment.hasBloomFilter())
      (1 to 1000).par.count {
        _ =>
          segment.mightContainKey(randomBytesSlice(100), ThreadReadState.random).runRandomIO.get
      } should be < 1000

  def assertBloomNotContains(bloom: BloomFilterBlockState)(implicit ec: ExecutionContext = TestExecutionContext.executionContext) =
    runThisParallel(1000.times) {
      val bloomFilter = Block.unblock[BloomFilterBlockOffset, BloomFilterBlock](bloom.compressibleBytes)
      BloomFilterBlock.mightContain(
        comparableKey = randomBytesSlice(randomIntMax(1000) min 100),
        reader = bloomFilter.copy()
      ).runRandomIO.get shouldBe false
    }

  def assertReads(keyValues: Slice[KeyValue],
                  segment: Segment) = {
    val asserts = Seq(() => assertGet(keyValues, segment), () => assertHigher(keyValues, segment), () => assertLower(keyValues, segment))
    Random.shuffle(asserts).par.foreach(_ ())
  }

  def assertAllSegmentsCreatedInLevel(level: Level) =
    level.segments() foreach (_.createdInLevel.runRandomIO.get shouldBe level.levelNumber)

  def assertGetFromThisLevelOnly(keyValues: Iterable[KeyValue],
                                 level: Level) =
    keyValues foreach {
      keyValue =>
        try {
          val actual = level.getFromThisLevel(keyValue.key, ThreadReadState.random).runRandomIO.get.getUnsafe
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
    BlockTestKit.readAll(segmentReader.copy()).runRandomIO.get shouldBe keyValues
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
          IO.Defer(segment.get(keyValue.key, ThreadReadState.random)).runRandomIO.get.getUnsafe shouldBe keyValue
        catch {
          case exception: Exception =>
            println(s"Failed to get: ${keyValue.key.readInt()}")
            throw exception
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
            ).runRandomIO.get.toOption shouldBe empty

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
                ).runRandomIO.get.getUnsafe shouldBe range
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
            ).runRandomIO.get.toOption shouldBe empty
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
            ).runRandomIO.get.getUnsafe shouldBe expectedLowerKeyValue

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
                ).runRandomIO.get.getUnsafe shouldBe range
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
            ).runRandomIO.get.getUnsafe shouldBe expectedLowerKeyValue
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
        IO.Defer(segment.lower(actualKeyValue.key, ThreadReadState.random)).runRandomIO.get.toOption shouldBe empty
        assertLowers(index + 1)
      } else {
        val expectedLower = keyValues(index - 1)
        val keyValue = keyValues(index)
        //        val intKey = keyValue.key.readInt()
        //        if (intKey % 100 == 0)
        //          println(s"Lower: $intKey")
        try {
          val lower = IO.Defer(segment.lower(keyValue.key, ThreadReadState.random)).runRandomIO.get.getUnsafe
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
    assertHigher(keyValues, getHigher = key => IO(IO.Defer(segment.higher(key, ThreadReadState.random)).runRandomIO.get.toOption))

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
          getHigher(range.fromKey).runRandomIO.get.value shouldBe range
          getHigher(range.toKey).runRandomIO.get shouldBe empty

        case keyValue =>
          getHigher(keyValue.key).runRandomIO.get shouldBe empty
      }

    //assert higher if the currently's read key-value is NOT the last key-value
    def assertNotLast(keyValue: KeyValue,
                      next: KeyValue,
                      nextNext: Option[KeyValue]) = {
      keyValue match {
        case range: KeyValue.Range =>
          try
            getHigher(range.fromKey).runRandomIO.get.value shouldBe range
          catch {
            case exception: Exception =>
              exception.printStackTrace()
              getHigher(range.fromKey).runRandomIO.get.value shouldBe range
              throw exception
          }
          val toKeyHigher = getHigher(range.toKey).runRandomIO.get
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
                      val toKeyHigher = getHigher(range.toKey).runRandomIO.get
                      throw exception
                  }
              else
                try
                  toKeyHigher.value shouldBe next
                catch {
                  case exception: Exception =>
                    exception.printStackTrace()
                    val toKeyHigher = getHigher(range.toKey).runRandomIO.get
                    throw exception
                }
          }

        case _ =>
          Try(getHigher(keyValue.key).runRandomIO.get.value shouldBe next) recover {
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

}
