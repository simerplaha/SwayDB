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

package swaydb.core.segment.defrag

import swaydb.core.segment.Segment
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.segment.ref.SegmentRef

import scala.collection.mutable.ListBuffer

private object DefragCommon {

  @inline def isSegmentSmall(segment: Segment)(implicit segmentConfig: SegmentBlockConfig): Boolean =
    segment.segmentSize < segmentConfig.minSize

  @inline def isSegmentRefSmall(ref: SegmentRef)(implicit segmentConfig: SegmentBlockConfig): Boolean =
    ref.keyValueCount < segmentConfig.maxCount

  @inline def lastStatsOrNull[S >: Null](fragments: ListBuffer[TransientSegment.Fragment[S]]): S =
    fragments.lastOption match {
      case Some(stats: TransientSegment.Stats[S]) =>
        stats.stats

      case Some(_) | None =>
        null
    }
}
