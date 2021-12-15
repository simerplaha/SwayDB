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

package swaydb.config.repairAppendix

sealed trait RepairResult[+T]

object RepairResult {
  sealed trait Repaired extends RepairResult[Nothing]
  case object Repaired extends Repaired

  case class OverlappingSegments[K](segmentInfo: SegmentInfo[K],
                                    overlappingSegmentInfo: SegmentInfo[K]) extends RepairResult[K]
}
