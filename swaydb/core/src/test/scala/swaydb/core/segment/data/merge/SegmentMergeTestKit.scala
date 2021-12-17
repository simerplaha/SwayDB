package swaydb.core.segment.data.merge


import org.scalatest.matchers.should.Matchers._
import swaydb.config.{Atomic, OptimiseWrites}
import swaydb.config.CoreConfigTestKit._
import swaydb.core.level.zero.LevelZeroLogCache
import swaydb.core.log.LogEntry
import swaydb.core.segment.data._
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.segment.data.KeyValueTestKit._
import swaydb.core.segment.CoreFunctionStore
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.TestKit.randomBoolean

object SegmentMergeTestKit {

  def assertSkipListMerge(newKeyValues: Iterable[KeyValue],
                          oldKeyValues: Iterable[KeyValue],
                          expected: Memory)(implicit coreFunctionStore: CoreFunctionStore): Iterable[Memory] =
    assertSkipListMerge(newKeyValues, oldKeyValues, Slice(expected))

  def assertSkipListMerge(newKeyValues: Iterable[KeyValue],
                          oldKeyValues: Iterable[KeyValue],
                          expected: Iterable[KeyValue])(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                        timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                        coreFunctionStore: CoreFunctionStore): Iterable[Memory] = {
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
                                                timeOrder: TimeOrder[Slice[Byte]],
                                                coreFunctionStore: CoreFunctionStore): Iterable[Memory] =
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), expected, isLastLevel)

  def assertMerge(newKeyValues: Slice[KeyValue],
                  oldKeyValues: Slice[KeyValue],
                  expected: Slice[KeyValue],
                  isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]],
                                        coreFunctionStore: CoreFunctionStore): Iterable[Memory] = {
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
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   coreFunctionStore: CoreFunctionStore): Unit = {
    //    println("*** Expected assert ***")
    assertMerge(newKeyValue, oldKeyValue, Slice(expected), lastLevelExpect.toOption.map(Slice(_)).getOrElse(Slice.empty))
    //println("*** Skip list assert ***")
    assertSkipListMerge(Slice(newKeyValue), Slice(oldKeyValue), Slice(expected))
  }

  def assertMerge(newKeyValues: Slice[KeyValue],
                  oldKeyValues: Slice[KeyValue],
                  expected: Slice[KeyValue],
                  lastLevelExpect: Slice[KeyValue])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                    timeOrder: TimeOrder[Slice[Byte]],
                                                    coreFunctionStore: CoreFunctionStore): Unit = {
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
                                                    timeOrder: TimeOrder[Slice[Byte]],
                                                    coreFunctionStore: CoreFunctionStore): Iterable[Memory] = {
    //    println("*** Last level = false ***")
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), expected, isLastLevel = false)
    //println("*** Last level = true ***")
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), lastLevelExpect, isLastLevel = true)
  }

  def assertMerge(newKeyValues: Slice[KeyValue],
                  oldKeyValues: Slice[KeyValue],
                  expected: Memory,
                  isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]],
                                        coreFunctionStore: CoreFunctionStore): Iterable[Memory] =
    assertMerge(newKeyValues, oldKeyValues, Slice(expected), isLastLevel)

  def assertMerge(newKeyValue: Memory.Function,
                  oldKeyValue: Memory.PendingApply,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   coreFunctionStore: CoreFunctionStore): Unit = {
    FunctionMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Function,
                  oldKeyValue: Memory.Fixed,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   coreFunctionStore: CoreFunctionStore): Unit = {
    FunctionMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Remove,
                  oldKeyValue: Memory.Fixed,
                  expected: KeyValue.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   coreFunctionStore: CoreFunctionStore): Unit = {
    RemoveMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Put,
                  oldKeyValue: Memory.Fixed,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   coreFunctionStore: CoreFunctionStore): Unit = {
    PutMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))

    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Update,
                  oldKeyValue: Memory.Fixed,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   coreFunctionStore: CoreFunctionStore): Unit = {
    UpdateMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Update,
                  oldKeyValue: Memory.PendingApply,
                  expected: KeyValue.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   coreFunctionStore: CoreFunctionStore): Unit = {
    UpdateMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))

    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Fixed,
                  oldKeyValue: Memory.PendingApply,
                  expected: Memory.PendingApply,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   coreFunctionStore: CoreFunctionStore): Unit = {
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))
    //todo merge with persistent
  }


}
