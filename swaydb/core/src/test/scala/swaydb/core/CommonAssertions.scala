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
//object CommonAssertions {
//
//
//}
//
