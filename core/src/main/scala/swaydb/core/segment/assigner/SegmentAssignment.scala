/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.assigner

import swaydb.Aggregator
import swaydb.core.segment.ref.SegmentRef

import scala.collection.mutable.ListBuffer

/**
 * Stores assignment information of key-values.
 *
 * @param segment    Segment this assignment belong sto
 * @param headGap    Head key-values that can be added directly to the Segment without merge
 * @param midOverlap Overlapping key-values that require merge.
 * @param tailGap    Tail key-values that can be added directly to the Segment without merge
 * @tparam GAP [[Aggregator]]'s result type that will store all gap key-values.
 * @tparam SEG Target Segment to which key-values should be assigned to.
 *             This can be a [[swaydb.core.segment.Segment]] or [[SegmentRef]]
 */
case class SegmentAssignment[+GAP, SEG](segment: SEG,
                                        headGap: Aggregator[Assignable, GAP],
                                        midOverlap: ListBuffer[Assignable],
                                        tailGap: Aggregator[Assignable, GAP])
