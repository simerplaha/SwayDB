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

import swaydb.Aggregator
import swaydb.core.data.Memory
import swaydb.core.merge.stats.{MergeStats, MergeStatsCreator}

import scala.collection.mutable.ListBuffer

/**
 * An [[Aggregator]] that also builds [[MergeStats]] for gap key-values to help Segments pre-compute merge statistics
 * which [[swaydb.core.segment.assigner.Assignment]] uses.
 */
object GapAggregator {

  def create[S >: Null <: MergeStats[Memory, ListBuffer]](removeDeletes: Boolean)(implicit mergeStatsCreator: MergeStatsCreator[S]): Aggregator.Creator[Assignable, ListBuffer[Assignable.Gap[S]]] =
    creator[S](mergeStatsCreator.create(removeDeletes))

  @inline private def creator[B >: Null <: MergeStats[Memory, ListBuffer]](createNewGap: => B): Aggregator.Creator[Assignable, ListBuffer[Assignable.Gap[B]]] =
    () =>
      GapAggregator.aggregator(createNewGap)

  /**
   * Aggregate assignments such that all [[Assignable.Collection]] are added without expanding
   * and if a [[Assignable.Point]] is added then start a new [[MergeStats.Segment]] instance and
   * add new key-values to that instance.
   */
  private def aggregator[B >: Null <: MergeStats[Memory, ListBuffer]](createNewGap: => B): Aggregator[Assignable, ListBuffer[Assignable.Gap[B]]] =
    new Aggregator[Assignable, ListBuffer[Assignable.Gap[B]]] {
      val buffer = ListBuffer.empty[Assignable.Gap[B]]

      var lastStatsOrNull: B = _

      override def add(item: Assignable): Unit =
        item match {
          case collection: Assignable.Collection =>
            lastStatsOrNull = null
            buffer += collection

          case point: Assignable.Point =>
            if (lastStatsOrNull == null) {
              lastStatsOrNull = createNewGap
              buffer += Assignable.Stats(lastStatsOrNull)
            }

            lastStatsOrNull add point.toMemory()
        }

      override def result: ListBuffer[Assignable.Gap[B]] =
        buffer
    }
}
