/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.level.compaction.task.assigner

import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.level.compaction.task.CompactionDataType

import scala.collection.SortedSet

class TaskAssigner_fillOnOverflow_Spec extends AnyWordSpec with Matchers with MockFactory {

  "fillOverflow" when {
    "no overflow" in {
      implicit val dataType = mock[CompactionDataType[Int]] //never invoked
      TaskAssigner.fillOverflow[Int](0, List(1, 2)) shouldBe empty
    }

    "half overflow" in {
      implicit val dataType = mock[CompactionDataType[Int]]
      (dataType.segmentSize _).expects(1) returning 1

      TaskAssigner.fillOverflow[Int](1, List(1, 2)) shouldBe SortedSet(1)
    }

    "full overflow" in {
      implicit val dataType = mock[CompactionDataType[Int]]
      (dataType.segmentSize _).expects(1) returning 1
      (dataType.segmentSize _).expects(2) returning 1

      //3 does not get added
      TaskAssigner.fillOverflow[Int](2, List(1, 2, 3)) shouldBe SortedSet(1, 2)
    }

    "inconsistent sizes" in {
      implicit val dataType = mock[CompactionDataType[Int]]
      //one has size 2
      (dataType.segmentSize _).expects(1) returning 2
      //two has size 3
      (dataType.segmentSize _).expects(2) returning 3

      //overflow is 3 so 1 does not satisfy there 2 will also get added but 3 never does.
      TaskAssigner.fillOverflow[Int](3, List(1, 2, 3)) shouldBe SortedSet(1, 2)
    }
  }
}
