/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.segment

import java.nio.file.Paths

import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.RunThis._
import org.scalatest.{FlatSpec, Matchers, WordSpec}
import swaydb.core.data.{Persistent, Time}
import swaydb.data.slice.Slice
import swaydb.serializers._
import swaydb.serializers.Default._

class SegmentReadStateSpec extends WordSpec with Matchers {

  def previousKeyValue =
    Persistent.Put(
      //create sliced key
      key = Slice.fill[Byte](10)(1.toByte).drop(9),
      deadline = None,
      valuesReaderNullable = null,
      time = Time.empty,
      nextIndexOffset = 100,
      nextKeySize = 100,
      indexOffset = 0,
      valueOffset = 0,
      valueLength = 0,
      sortedIndexAccessPosition = 1
    )

  def nextKeyValue =
    Persistent.Put(
      //create sliced key
      key = Slice.fill[Byte](10)(1.toByte).dropRight(8),
      deadline = None,
      valuesReaderNullable = null,
      time = Time.empty,
      nextIndexOffset = 200,
      nextKeySize = 200,
      indexOffset = 100,
      valueOffset = 0,
      valueLength = 0,
      sortedIndexAccessPosition = 1
    )

  "createOnSuccessSequentialRead" in {
    val threadState = ThreadReadState.random
    val path = Paths.get("1")

    val previous = previousKeyValue

    SegmentReadState.createOnSuccessSequentialRead(
      path = path,
      readState = threadState,
      found = previous
    )

    val segmentState = threadState.getSegmentState(path)
    segmentState.getS.keyValue shouldBe previous
    segmentState.getS.keyValue.key.underlyingArraySize shouldBe 1
    segmentState.getS.isSequential shouldBe true
  }

  "mutateOnSuccessSequentialRead" in {
    val threadState = ThreadReadState.random
    val path = Paths.get("1")

    val previous = previousKeyValue
    val next = nextKeyValue

    SegmentReadState.createOnSuccessSequentialRead(
      path = path,
      readState = threadState,
      found = previous
    )

    val segmentState = threadState.getSegmentState(path).getS

    SegmentReadState.mutateOnSuccessSequentialRead(
      path = path,
      readState = threadState,
      found = nextKeyValue,
      segmentState = segmentState
    )

    val readSegmentState = threadState.getSegmentState(path).getS
    readSegmentState.getS.keyValue shouldBe nextKeyValue
    readSegmentState.getS.keyValue.key.underlyingArraySize shouldBe 2
    readSegmentState.getS.isSequential shouldBe true
  }

  "createAfterRandomRead" when {
    "result is Null" should {
      "not create an entry" in {
        val threadState = ThreadReadState.random
        val path = Paths.get("1")

        SegmentReadState.createAfterRandomRead(
          path = path,
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

        SegmentReadState.createAfterRandomRead(
          path = path,
          threadState = threadState,
          foundOption = next,
          start = previous
        )

        val segmentState = threadState.getSegmentState(path).getS
        segmentState.keyValue shouldBe next
        segmentState.keyValue.key.underlyingArraySize shouldBe 2
        segmentState.isSequential shouldBe true
      }
    }

    "start is defined and is not sequential" should {
      "create an entry with isSequential set to false" in {
        val threadState = ThreadReadState.random
        val path = Paths.get("1")

        val previous = previousKeyValue
        val next = nextKeyValue

        SegmentReadState.createAfterRandomRead(
          path = path,
          threadState = threadState,
          foundOption = next.copy(indexOffset = 500),
          start = previous
        )

        val segmentState = threadState.getSegmentState(path).getS
        segmentState.keyValue shouldBe next
        segmentState.keyValue.key.underlyingArraySize shouldBe 2
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

        threadState.setSegmentState(path, new SegmentReadState(previous, Persistent.Null, randomBoolean()))

        SegmentReadState.mutateAfterRandomRead(
          path = path,
          threadState = threadState,
          foundOption = Persistent.Null,
          segmentState = threadState.getSegmentState(path).getS
        )

        val segmentState = threadState.getSegmentState(path).getS
        segmentState.keyValue shouldBe previous
        segmentState.isSequential shouldBe false
      }
    }

    "found was defined but result was not sequential" in {
      runThis(10.times) {
        val threadState = ThreadReadState.random
        val path = Paths.get("1")

        val previous = previousKeyValue
        val next = nextKeyValue.copy(indexOffset = 1000)

        threadState.setSegmentState(path, new SegmentReadState(previous, Persistent.Null, randomBoolean()))

        SegmentReadState.mutateAfterRandomRead(
          path = path,
          threadState = threadState,
          foundOption = next,
          segmentState = threadState.getSegmentState(path).getS
        )

        val segmentState = threadState.getSegmentState(path).getS
        segmentState.keyValue shouldBe next
        segmentState.keyValue.key.underlyingArraySize shouldBe 2
        segmentState.isSequential shouldBe false
      }
    }

    "found was defined but result was sequential" in {
      runThis(10.times) {
        val threadState = ThreadReadState.random
        val path = Paths.get("1")

        val previous = previousKeyValue
        val next = nextKeyValue

        threadState.setSegmentState(path, new SegmentReadState(previous, Persistent.Null, randomBoolean()))

        SegmentReadState.mutateAfterRandomRead(
          path = path,
          threadState = threadState,
          foundOption = next,
          segmentState = threadState.getSegmentState(path).getS
        )

        val segmentState = threadState.getSegmentState(path).getS
        segmentState.keyValue shouldBe next
        segmentState.keyValue.key.underlyingArraySize shouldBe 2
        segmentState.isSequential shouldBe true
      }
    }
  }

}
