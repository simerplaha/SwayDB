/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.segment.ref.search

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.{Persistent, Time}
import swaydb.data.RunThis._
import swaydb.data.slice.Slice
import swaydb.data.util.TupleOrNone
import swaydb.serializers.Default._
import swaydb.serializers._

import java.nio.file.Paths

class SegmentReadStateSpec extends AnyWordSpec with Matchers {

  def previousKeyValue =
    Persistent.Put(
      //create sliced key
      key = Slice.fill[Byte](10)(1.toByte).drop(9),
      deadline = None,
      valuesReaderOrNull = null,
      time = Time.empty,
      nextIndexOffset = 100,
      nextKeySize = 100,
      indexOffset = 0,
      valueOffset = 0,
      valueLength = 0,
      sortedIndexAccessPosition = 1,
      previousIndexOffset = 0,
    )

  def nextKeyValue =
    Persistent.Put(
      //create sliced key
      key = Slice.fill[Byte](10)(1.toByte).dropRight(8),
      deadline = None,
      valuesReaderOrNull = null,
      time = Time.empty,
      nextIndexOffset = 200,
      nextKeySize = 200,
      indexOffset = 100,
      valueOffset = 0,
      valueLength = 0,
      sortedIndexAccessPosition = 1,
      previousIndexOffset = 1
    )

  "createOnSuccessSequentialRead" in {
    val threadState = ThreadReadState.random
    val path = Paths.get("1")

    val previous = previousKeyValue

    val forKey = randomBytes().take(2)

    SegmentReadState.createOnSuccessSequentialRead(
      path = path,
      forKey = forKey,
      readState = threadState,
      found = previous
    )

    val segmentState = threadState.getSegmentState(path)
    segmentState.getS.keyValue._1.toList shouldBe forKey.toList
    segmentState.getS.keyValue._1.underlyingArraySize shouldBe 2
    segmentState.getS.keyValue._2 shouldBe previous
    segmentState.getS.keyValue._2.key.underlyingArraySize shouldBe 1
    segmentState.getS.isSequential shouldBe true
  }

  "mutateOnSuccessSequentialRead" in {
    val threadState = ThreadReadState.random
    val path = Paths.get("1")

    val next = nextKeyValue

    val forKey = randomBytes().take(3)

    SegmentReadState.createOnSuccessSequentialRead(
      path = path,
      forKey = randomBytesSlice(),
      readState = threadState,
      found = next
    )

    val segmentState = threadState.getSegmentState(path).getS

    SegmentReadState.mutateOnSuccessSequentialRead(
      path = path,
      forKey = forKey,
      readState = threadState,
      found = nextKeyValue,
      segmentState = segmentState
    )

    val readSegmentState = threadState.getSegmentState(path).getS
    readSegmentState.getS.keyValue._1.toList shouldBe forKey.toList
    segmentState.getS.keyValue._1.underlyingArraySize shouldBe 3
    readSegmentState.getS.keyValue._2 shouldBe nextKeyValue
    readSegmentState.getS.keyValue._2.key.underlyingArraySize shouldBe 2
    readSegmentState.getS.isSequential shouldBe true
  }

  "createAfterRandomRead" when {
    "result is Null" should {
      "not create an entry" in {
        val threadState = ThreadReadState.random
        val path = Paths.get("1")

        SegmentReadState.createAfterRandomRead(
          path = path,
          forKey = randomBytesSlice(),
          threadState = threadState,
          foundOption = Persistent.Null,
          start = previousKeyValue
        )

        threadState.getSegmentState(path).toOptionS shouldBe empty
      }
    }

    "start is defined and is sequential" should {
      "create an entry with isSequential set to true" in {
        val threadState = ThreadReadState.random
        val path = Paths.get("1")

        val previous = previousKeyValue
        val next = nextKeyValue

        val forKey = randomBytesSlice(5).take(2)

        SegmentReadState.createAfterRandomRead(
          path = path,
          forKey = forKey,
          threadState = threadState,
          foundOption = next,
          start = previous
        )

        val segmentState = threadState.getSegmentState(path).getS
        segmentState.keyValue._1 shouldBe forKey
        segmentState.keyValue._1.underlyingArraySize shouldBe 2
        segmentState.keyValue._2 shouldBe next
        segmentState.keyValue._1.underlyingArraySize shouldBe 2
        segmentState.isSequential shouldBe true
      }
    }

    "start is defined and is not sequential" should {
      "create an entry with isSequential set to false" in {
        val threadState = ThreadReadState.random
        val path = Paths.get("1")

        val previous = previousKeyValue
        val next = nextKeyValue

        val forKey = randomBytesSlice().take(5)

        SegmentReadState.createAfterRandomRead(
          path = path,
          forKey = forKey,
          threadState = threadState,
          foundOption = next.copy(indexOffset = 500),
          start = previous
        )

        val segmentState = threadState.getSegmentState(path).getS
        segmentState.keyValue._1 shouldBe forKey
        segmentState.keyValue._1.underlyingArraySize shouldBe 5
        segmentState.keyValue._2 shouldBe next
        segmentState.keyValue._2.key.underlyingArraySize shouldBe 2
        segmentState.isSequential shouldBe false
      }
    }
  }

  "mutateAfterRandomRead" when {
    "found was Null" in {
      runThis(10.times) {
        val threadState = ThreadReadState.random
        val path = Paths.get("1")

        val previous = previousKeyValue

        val forKey1 = randomBytesSlice()

        threadState.setSegmentState(
          path = path,
          nextIndexOffset =
            new SegmentReadState(
              keyValue = (forKey1, previous),
              lower = TupleOrNone.None,
              isSequential = randomBoolean()
            )
        )

        val forKey2 = randomBytesSlice()

        SegmentReadState.mutateAfterRandomRead(
          path = path,
          forKey = forKey2,
          threadState = threadState,
          foundOption = Persistent.Null,
          segmentState = threadState.getSegmentState(path).getS
        )

        val segmentState = threadState.getSegmentState(path).getS
        segmentState.keyValue._1 shouldBe forKey1
        segmentState.keyValue._2 shouldBe previous
        segmentState.isSequential shouldBe false
      }
    }

    "found was defined but result was not sequential" in {
      runThis(10.times) {
        val threadState = ThreadReadState.random
        val path = Paths.get("1")

        val previous = previousKeyValue
        val next = nextKeyValue.copy(indexOffset = 1000)

        threadState.setSegmentState(
          path = path,
          nextIndexOffset =
            new SegmentReadState(
              keyValue = (randomBytesSlice(), previous),
              lower = TupleOrNone.None,
              isSequential = randomBoolean()
            )
        )

        val forKey = randomBytesSlice().take(4)

        SegmentReadState.mutateAfterRandomRead(
          path = path,
          forKey = forKey,
          threadState = threadState,
          foundOption = next,
          segmentState = threadState.getSegmentState(path).getS
        )

        val segmentState = threadState.getSegmentState(path).getS
        segmentState.keyValue._1 shouldBe forKey
        segmentState.keyValue._1.underlyingArraySize shouldBe 4
        segmentState.keyValue._2 shouldBe next
        segmentState.keyValue._2.key.underlyingArraySize shouldBe 2
        segmentState.isSequential shouldBe false
      }
    }

    "found was defined but result was sequential" in {
      runThis(10.times) {
        val threadState = ThreadReadState.random
        val path = Paths.get("1")

        val previous = previousKeyValue
        val next = nextKeyValue

        threadState.setSegmentState(
          path = path,
          nextIndexOffset =
            new SegmentReadState(
              keyValue = (randomBytesSlice(), previous),
              lower = TupleOrNone.None,
              isSequential = randomBoolean()
            )
        )

        val forKey = randomBytesSlice().take(5)

        SegmentReadState.mutateAfterRandomRead(
          path = path,
          forKey = forKey,
          threadState = threadState,
          foundOption = next,
          segmentState = threadState.getSegmentState(path).getS
        )

        val segmentState = threadState.getSegmentState(path).getS
        segmentState.keyValue._1 shouldBe forKey
        segmentState.keyValue._1.underlyingArraySize shouldBe 5
        segmentState.keyValue._2 shouldBe next
        segmentState.keyValue._2.key.underlyingArraySize shouldBe 2
        segmentState.isSequential shouldBe true
      }
    }
  }

}
