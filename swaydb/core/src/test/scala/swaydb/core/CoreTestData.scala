///*
// * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package swaydb.core
//
//import org.scalatest.OptionValues._
//import org.scalatest.matchers.should.Matchers._
//import swaydb.Error.Segment.ExceptionHandler
//import swaydb.IO.ExceptionHandler.Nothing
//import swaydb.effect.IOValues._
//import swaydb.core.compression.CoreCompression
//import swaydb.core.compression.CompressionTestKit._
//import swaydb.config._
//import swaydb.config.accelerate.Accelerator
//import swaydb.config.compaction.{LevelMeter, LevelThrottle}
//import swaydb.config.storage.{Level0Storage, LevelStorage}
//import swaydb.core.CommonAssertions._
//import swaydb.core.TestSweeper._
//import swaydb.core.cache.Cache
//import swaydb.core.file.CoreFile
//import swaydb.core.level.seek._
//import swaydb.core.level.zero.LevelZero
//import swaydb.core.level.zero.LevelZero.LevelZeroLog
//import swaydb.core.level.{Level, NextLevel}
//import swaydb.core.segment._
//import swaydb.core.segment.assigner.Assignable
//import swaydb.core.segment.block._
//import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockOffset.BinarySearchIndexBlockOps
//import swaydb.core.segment.block.binarysearch.{BinarySearchEntryFormat, BinarySearchIndexBlockConfig}
//import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
//import swaydb.core.segment.block.bloomfilter.BloomFilterBlockOffset.BloomFilterBlockOps
//import swaydb.core.segment.block.hashindex.HashIndexBlockOffset.HashIndexBlockOps
//import swaydb.core.segment.block.hashindex.{HashIndexBlockConfig, HashIndexEntryFormat}
//import swaydb.core.segment.block.reader.{BlockedReader, UnblockedReader}
//import swaydb.core.segment.block.segment.SegmentBlockOffset.SegmentBlockOps
//import swaydb.core.segment.block.segment.footer.SegmentFooterBlockOffset.SegmentFooterBlockOps
//import swaydb.core.segment.block.segment.transient.TransientSegment
//import swaydb.core.segment.block.segment.{SegmentBlock, SegmentBlockConfig, SegmentBlockOffset}
//import swaydb.core.segment.block.sortedindex.SortedIndexBlockConfig
//import swaydb.core.segment.block.sortedindex.SortedIndexBlockOffset.SortedIndexBlockOps
//import swaydb.core.segment.block.values.ValuesBlockOffset.ValuesBlockOps
//import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockConfig, ValuesBlockOffset}
//import swaydb.core.segment.data.Value.{FromValue, FromValueOption, RangeValue}
//import swaydb.core.segment.data._
//import swaydb.core.segment.data.merge.stats.MergeStats
//import swaydb.core.segment.data.merge.{KeyValueGrouper, KeyValueMerger}
//import swaydb.core.segment.entry.id.BaseEntryIdFormatA
//import swaydb.core.segment.entry.writer.EntryWriter
//import swaydb.core.segment.io.{SegmentCompactionIO, SegmentReadIO, SegmentWritePersistentIO}
//import swaydb.core.segment.ref.SegmentRef
//import swaydb.core.segment.ref.search.ThreadReadState
//import swaydb.core.skiplist.AtomicRanges
//import swaydb.core.util.DefIO
//import swaydb.effect.{Dir, IOAction, IOStrategy}
//import swaydb.serializers.Default._
//import swaydb.serializers._
//import swaydb.slice.order.{KeyOrder, TimeOrder}
//import swaydb.slice.{MaxKey, Slice, SliceOption, SliceRO}
//import swaydb.testkit.RunThis.FutureImplicits
//import swaydb.testkit.TestKit._
//import swaydb.utils.StorageUnits._
//import swaydb.utils.{Aggregator, FiniteDurations, IDGenerator, OperatingSystem}
//import swaydb.{ActorConfig, Error, Glass, IO}
//
//import java.nio.file.Path
//import java.util.concurrent.atomic.AtomicInteger
//import scala.collection.compat._
//import scala.collection.mutable.ListBuffer
//import scala.concurrent.ExecutionContext
//import scala.concurrent.duration._
//import scala.reflect.ClassTag
//import scala.util.Random
//import org.scalactic.Equality
//import org.scalatest.OptionValues._
//import org.scalatest.exceptions.TestFailedException
//import org.scalatest.matchers.should.Matchers._
//import swaydb.{Bag, Error, Glass, IO}
//import swaydb.Error.Segment.ExceptionHandler
//import swaydb.effect.IOValues._
//import swaydb.config.{Atomic, OptimiseWrites}
//import swaydb.config.compaction.PushStrategy
//import swaydb.core.CoreTestData._
//import swaydb.core.file.sweeper.bytebuffer.ByteBufferCommand
//import swaydb.core.level.{Level, LevelRef, NextLevel}
//import swaydb.core.level.zero.{LevelZero, LevelZeroLogCache}
//import swaydb.core.log.{LogEntry, Logs}
//import swaydb.core.log.serialiser.LogEntryWriter
//import swaydb.core.segment._
//import swaydb.core.segment.block._
//import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
//import swaydb.core.segment.block.bloomfilter.{BloomFilterBlock, BloomFilterBlockConfig, BloomFilterBlockOffset, BloomFilterBlockState}
//import swaydb.core.segment.block.hashindex.HashIndexBlockConfig
//import swaydb.core.segment.block.reader.{BlockRefReader, UnblockedReader}
//import swaydb.core.segment.block.segment.{SegmentBlock, SegmentBlockCache, SegmentBlockConfig, SegmentBlockOffset}
//import swaydb.core.segment.block.segment.SegmentBlockOffset.SegmentBlockOps
//import swaydb.core.segment.block.segment.transient.TransientSegment
//import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockConfig}
//import swaydb.core.segment.block.values.ValuesBlockConfig
//import swaydb.core.segment.cache.sweeper.MemorySweeper
//import swaydb.core.segment.data._
//import swaydb.core.segment.data.Memory.PendingApply
//import swaydb.core.segment.data.Value.FromValue
//import swaydb.core.segment.data.merge._
//import swaydb.core.segment.data.merge.stats.MergeStats
//import swaydb.core.segment.io.SegmentReadIO
//import swaydb.core.segment.ref.search.{KeyMatcher, SegmentSearcher, ThreadReadState}
//import swaydb.core.segment.ref.search.KeyMatcher.Result
//import swaydb.core.segment.serialiser.{RangeValueSerialiser, ValueSerialiser}
//import swaydb.core.skiplist.SkipListConcurrent
//import swaydb.effect.{Effect, IOStrategy}
//import swaydb.serializers._
//import swaydb.serializers.Default._
//import swaydb.SliceIOImplicits._
//import swaydb.core.file.FileReader
//import swaydb.slice.{Reader, Slice, SliceOption, SliceReader}
//import swaydb.slice.order.{KeyOrder, TimeOrder}
//import swaydb.testkit.RunThis._
//import swaydb.testkit.TestKit._
//import swaydb.utils.Aggregator
//
//import java.nio.file.Paths
//import scala.annotation.tailrec
//import scala.collection.mutable.ListBuffer
//import scala.collection.parallel.CollectionConverters._
//import scala.concurrent.ExecutionContext
//import scala.concurrent.duration._
//import scala.reflect.ClassTag
//import scala.util.{Random, Try}
//
//object CoreTestData {
//
//  val unit: Unit = ()
//
//  /**
//   * Sequential time bytes generator.
//   */
//
//  implicit val functionStore: CoreFunctionStore = CoreFunctionStore.memory()
//
//  val functionIdGenerator = new AtomicInteger(0)
//
//
//
//}
