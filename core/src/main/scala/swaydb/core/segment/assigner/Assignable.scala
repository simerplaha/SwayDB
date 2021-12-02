/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.assigner

import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.segment.data.{KeyValue, Memory}
import swaydb.slice.{MaxKey, Slice}
import swaydb.utils.Aggregator

import scala.collection.mutable.ListBuffer

/**
 * Something that can be assigned to a Segment for merge.
 *
 * Current types
 *  - [[swaydb.core.segment.Segment]]
 *  - [[swaydb.core.log.Log[Slice[Byte], Memory, LevelZeroMapCache]]
 *  - [[swaydb.core.segment.data.KeyValue]]
 */
sealed trait Assignable {
  def key: Slice[Byte]
}

object Assignable {

  val emptyIterable = Iterable.empty[Assignable]
  val emptyIterator: Iterator[Assignable] = Iterator.empty

  implicit val nothingCreator: Aggregator.Creator[Assignable, Nothing] =
    Aggregator.Creator.nothing[Assignable]()

  implicit def listBufferAssignableCreator: Aggregator.Creator[Assignable, ListBuffer[Assignable]] =
    Aggregator.Creator.listBuffer()

  /**
   * A Gap can either be a [[Collection]] of a series of key-values i.e. [[Stats]].
   *
   * This type is used by assignment to collects stats which performing assignments.
   */
  sealed trait Gap[+A]

  case class Stats[A <: MergeStats[Memory, ListBuffer]](stats: A) extends Gap[A]

  /**
   * A [[Collection]] is a collection of key-values like [[swaydb.core.segment.Segment]]
   * and [[swaydb.core.log.Log[Slice[Byte], Memory, LevelZeroMapCache]].
   *
   * [[swaydb.core.log.Log]] can be created using [[Collection.fromMap]].
   *
   * This type is needed for cases where we can assign a group of key-values to a
   * [[swaydb.core.segment.Segment]] without need to assign each key-value saving
   * CPU times and IO for cases where key-values are persistent - [[swaydb.core.segment.PersistentSegment]].
   */
  trait Collection extends Assignable with Gap[Nothing] {
    def maxKey: MaxKey[Slice[Byte]]
    def iterator(inOneSeek: Boolean): Iterator[KeyValue]
    def keyValueCount: Int
  }

  trait Point extends Assignable {
    def toMemory(): Memory
  }

}
