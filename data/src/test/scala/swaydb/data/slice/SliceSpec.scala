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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.data.slice

import org.scalatest.OptionValues._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.data.MaxKey
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.{ByteSizeOf, ScalaByteOps}

import scala.util.Random

class SliceSpec extends AnyWordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default
  implicit val scalaByte = ScalaByteOps

  def randomByte() = (Random.nextInt(256) - 128).toByte

  "A Slice" should {
    "be created by specifying it's length" in {
      val slice = Slice.create[Int](10)
      slice.allocatedSize shouldBe 10
      slice.size shouldBe 0
      slice.fromOffset shouldBe 0
      slice.toOffset shouldBe 9
    }

    "be created by specifying it's length and isFull" in {
      val slice = Slice.create[Int](10, isFull = true)
      slice.allocatedSize shouldBe 10
      slice.size shouldBe 10
      slice.fromOffset shouldBe 0
      slice.toOffset shouldBe 9
    }

    "be created from an Array" in {
      val array = Array.fill[Byte](10)(1)
      val slice = Slice[Byte](array)
      slice.allocatedSize shouldBe 10
      slice.size shouldBe 10
      slice.fromOffset shouldBe 0
      slice.toOffset shouldBe 9
    }

    "be created from another Slice" in {
      val array = Array.fill[Int](3)(Random.nextInt())
      val slice1 = Slice[Int](array)
      slice1.size shouldBe 3
      slice1.allocatedSize shouldBe 3

      val slice2 = slice1.slice(1, 2)
      slice2.allocatedSize shouldBe 2
      slice2.size shouldBe 2
      slice2.fromOffset shouldBe 1
      slice2.toOffset shouldBe 2
      slice2.toList should contain inOrderElementsOf List(array(1), array(2))
      slice2.underlyingArraySize shouldBe slice1.size

      val slice2Copy = slice2.unslice()
      slice2Copy.allocatedSize shouldBe 2
      slice2Copy.size shouldBe 2
      slice2Copy.underlyingArraySize shouldBe 2
    }

    "be sliced for a partially written slice" in {
      //slice0 is (10, 10, null, null)
      val slice0 = Slice.create[Int](4)
      slice0 add 10
      slice0 add 10 //second slice starts here
      slice0.size shouldBe 2

      //slice1 = (10, 10)
      val slice1 = slice0.slice(0, 1)
      slice1.size shouldBe 2
      slice1.toArray shouldBe Array(10, 10)
      slice1.underlyingArraySize shouldBe 4

      //slice1 = (10, null)
      val slice2 = slice0.slice(1, 2)
      slice2.size shouldBe 1
      slice2.toArray shouldBe Array(10)

      //slice1 = (null, null)
      val slice3 = slice0.slice(2, 3)
      slice3.size shouldBe 0
      slice3.toArray shouldBe empty

      //slice4 = (10, 10, null, null)
      val slice4 = slice0.slice(0, 3)
      slice4.size shouldBe 2
      slice4.toArray shouldBe Array(10, 10)
    }

    "be sliced if the original slice is full written" in {
      //slice0 = (1, 2, 3, 4)
      val slice0 = Slice.create[Int](4)
      slice0 add 1
      slice0 add 2
      slice0 add 3
      slice0 add 4
      slice0.size shouldBe 4

      //slice1 = (1, 2)
      val slice1 = slice0.slice(0, 1)
      slice1.size shouldBe 2
      slice1.toArray shouldBe Array(1, 2)

      //slice1 = (2, 3)
      val slice2 = slice0.slice(1, 2)
      slice2.size shouldBe 2
      slice2.toArray shouldBe Array(2, 3)

      //slice1 = (3, 4)
      val slice3 = slice0.slice(2, 3)
      slice3.size shouldBe 2
      slice3.toArray shouldBe Array(3, 4)

      //slice4 = (1, 2, 3, 4)
      val slice4 = slice0.slice(0, 3)
      slice4.size shouldBe 4
      slice4.toArray shouldBe Array(1, 2, 3, 4)
    }

    "return empty when creating a sub Slice with invalid offsets" in {
      val slice1 = Slice(1, 2, 3)
      slice1.size shouldBe 3
      slice1.slice(0, 3) shouldBe Slice(1, 2, 3)
      slice1.slice(3, 100) shouldBe empty
      slice1.slice(10, 3) shouldBe empty

      //valid subslice 2
      val slice2 = slice1.slice(1, 2)
      slice2.size shouldBe 2
      slice2 shouldBe Slice(2, 3)
      slice2.slice(100, 100) shouldBe empty
    }

    "throw ArrayIndexOutOfBoundsException when inserting items outside the Slice offset" in {
      val slice = Slice.create[Byte](1)
      slice.allocatedSize shouldBe 1
      slice.size shouldBe 0
      slice.fromOffset shouldBe 0
      slice.toOffset shouldBe 0

      slice.add(1).size shouldBe 1
      assertThrows[ArrayIndexOutOfBoundsException] {
        slice.add(1)
      }
      slice.size shouldBe 1
    }

    "throw ArrayIndexOutOfBoundsException when adding items outside it's offset and when the Slice is a sub slice" in {
      val slice1 = Slice.fill(4)(Random.nextInt())
      slice1.size shouldBe 4

      val slice2 = slice1.slice(1, 2)
      slice2.size shouldBe 2

      slice2.size shouldBe 2
      slice2.head shouldBe slice1(1)
      slice2.last shouldBe slice1(2)
      assertThrows[ArrayIndexOutOfBoundsException] {
        slice2.add(0)
      }
    }

    "be read by it's index position" in {
      val array = Array.fill(5)(Random.nextInt())
      val slice = Slice(array)

      Range.inclusive(0, 4).foreach {
        index =>
          slice.getC(index) shouldBe array(index)
      }

      val subSlice = slice.slice(1, 2)
      subSlice.head shouldBe array(1)
      subSlice.last shouldBe array(2)

      val subSlice2 = subSlice.slice(0, 0)
      subSlice2.head shouldBe subSlice.head
      subSlice2.last shouldBe subSlice.head
    }

    "drop head elements" in {
      val slice = Slice(1, 2, 3, 4, 5)
      slice.size shouldBe 5

      val newSlice = slice drop 2
      newSlice.size shouldBe 3
      newSlice.toList shouldBe Seq(3, 4, 5)

      val newSlice2 = newSlice.slice(1, 2).drop(1)
      newSlice2.toList should contain only 5
    }

    "drop last elements" in {
      val slice = Slice(1, 2, 3, 4, 5)
      slice.size shouldBe 5

      val newSlice = slice dropRight 2
      newSlice.size shouldBe 3
      newSlice.toList shouldBe Seq(1, 2, 3)

      val newSlice2 = newSlice.slice(1, 2).dropRight(1)
      newSlice2.toList should contain only 2
    }

    "drop last elements when the Slice have only one element" in {
      val slice = Slice.fill(1)(randomByte())
      slice.size shouldBe 1

      val newSlice = slice dropRight 1
      newSlice shouldBe empty
    }

    "take first and last elements" in {
      val slice = Slice.create(5).add(0).add(1).add(2).add(3).add(4)
      slice.size shouldBe 5

      (slice take 2) should contain only(0, 1)
      (slice takeRight 2) should contain only(3, 4)

      (slice.slice(1, 3) take 2) should contain only(1, 2)
      (slice.slice(2, 4) takeRight 2) should contain only(3, 4)
    }

    "be splittable" in {
      val slice = Slice.fill(4)(randomByte())

      val (head1, tail1) = slice.splitAt(0)
      head1.isEmpty shouldBe true
      tail1.size shouldBe 4
      head1.underlyingArraySize shouldBe 0
      tail1.underlyingArraySize shouldBe slice.size

      val (head2, tail2) = slice.splitAt(1)
      head2.size shouldBe 1
      tail2.size shouldBe 3
      head2.underlyingArraySize shouldBe slice.size
      tail2.underlyingArraySize shouldBe slice.size

      val (head3, tail3) = slice.splitAt(2)
      head3.size shouldBe 2
      tail3.size shouldBe 2
      head3.underlyingArraySize shouldBe slice.size
      tail3.underlyingArraySize shouldBe slice.size

      val (head4, tail4) = slice.splitAt(3)
      head4.size shouldBe 3
      tail4.size shouldBe 1
      head4.underlyingArraySize shouldBe slice.size
      tail4.underlyingArraySize shouldBe slice.size

      val (head5, tail5) = slice.splitAt(slice.size - 2)
      head5.size shouldBe 2
      tail5.size shouldBe 2
      head5.underlyingArraySize shouldBe slice.size
      tail5.underlyingArraySize shouldBe slice.size
    }

    "update original slice with moveWritePosition when splits are updated" in {
      val originalSlice = Slice.create[Int](2)
      val (split1, split2) = originalSlice.splitInnerArrayAt(1)
      split1.allocatedSize shouldBe 1
      split2.size shouldBe 0

      split1.add(100)
      split2.add(200)

      split1.size shouldBe 1
      split2.size shouldBe 1

      originalSlice.moveWritePosition(2)
      originalSlice should contain only(100, 200)
      originalSlice.toArray shouldBe Array(100, 200)
    }

    "group elements" in {
      val slice = Slice((1 to 100).toArray)

      //even equal slices
      val groupsOf5 = slice.grouped(5).toArray
      groupsOf5 should have size 5
      groupsOf5.foreach(_.underlyingArraySize shouldBe slice.size)
      groupsOf5(0).toList shouldBe (1 to 20)
      groupsOf5(1).toList shouldBe (21 to 40)
      groupsOf5(2).toList shouldBe (41 to 60)
      groupsOf5(3).toList shouldBe (61 to 80)
      groupsOf5(4).toList shouldBe (81 to 100)

      //odd slices
      val groupsOf3 = slice.grouped(3).toArray
      groupsOf3 should have size 3
      groupsOf3.foreach(_.underlyingArraySize shouldBe slice.size)
      groupsOf3(0).toList shouldBe (1 to 33)
      groupsOf3(1).toList shouldBe (34 to 66)
      groupsOf3(2).toList shouldBe (67 to 100)
    }
  }

  "A sub Slice" should {
    "be read in between it's offset positions and not from the original array" in {
      val slice = Slice.fill(5)(Random.nextInt())
      val subSlice = slice.slice(2, 3)
      subSlice(0) shouldBe slice(2)
      subSlice(1) shouldBe slice(3)

      subSlice.map(int => int) should contain allOf(slice(2), slice(3))
    }

    "should return head and last element in the sub slice" in {
      val slice = Slice.fill(5)(Random.nextInt())
      val subSlice = slice.slice(2, 3)
      subSlice.head shouldBe slice(2)
      subSlice.last shouldBe slice(3)
    }
  }

  "A Byte Slice (Slice[Byte])" can {
    "write and read Integers" in {
      val slice = Slice.create[Byte](ByteSizeOf.int * 2)
      slice addInt Int.MaxValue
      slice addInt Int.MinValue

      val reader = slice.createReader()
      reader.readInt() shouldBe Int.MaxValue
      reader.readInt() shouldBe Int.MinValue
    }

    "write and read Long" in {
      val slice = Slice.create[Byte](ByteSizeOf.long * 2)
      slice addLong Long.MaxValue
      slice addLong Long.MinValue

      val reader = slice.createReader()
      reader.readLong() shouldBe Long.MaxValue
      reader.readLong() shouldBe Long.MinValue
    }

    "write and read Unsigned Integer" in {
      val slice = Slice.create[Byte](ByteSizeOf.int + 1)
      slice addUnsignedInt Int.MaxValue
      slice.createReader().readUnsignedInt() shouldBe Int.MaxValue
    }

    "write and read Unsigned Long" in {
      val slice = Slice.create[Byte](ByteSizeOf.long + 1)
      slice addUnsignedLong Long.MaxValue
      slice.createReader().readUnsignedLong() shouldBe Long.MaxValue
    }

    "write and read String" in {
      val slice = Slice.create[Byte](10000)
      slice addStringUTF8 "This is a string"
      slice.close().createReader().readRemainingAsString() shouldBe "This is a string"
    }

    "write and read remaining string String" in {
      val slice = Slice.create[Byte](10000)

      slice addInt 1
      slice addLong 2L
      slice addUnsignedInt 3
      slice addUnsignedLong 4L
      slice addSignedInt -3
      slice addSignedLong -4L
      slice addStringUTF8 "This is a string"

      val reader = slice.close().createReader()
      reader.readInt() shouldBe 1
      reader.readLong() shouldBe 2L
      reader.readUnsignedInt() shouldBe 3
      reader.readUnsignedLong() shouldBe 4L
      reader.readSignedInt() shouldBe -3
      reader.readSignedLong() shouldBe -4L
      reader.readRemainingAsString() shouldBe "This is a string"
    }

    "write and read String of specified size" in {
      val slice = Slice.create[Byte](10000)
      slice addStringUTF8 "This is a string"

      val reader = slice.close().createReader()
      reader.readString(8) shouldBe "This is "
      reader.readString(8) shouldBe "a string"
    }
  }

  "write multiple with addAll" in {
    Slice.create[Int](4)
      .add(1)
      .add(2)
      .addAll(Slice(3, 4)).toList shouldBe List(1, 2, 3, 4)
  }

  "addAll should fail if Slice does not have capacity" in {
    assertThrows[ArrayIndexOutOfBoundsException] {
      Slice.create[Int](3)
        .add(1)
        .add(2)
        .addAll(Slice(3, 4))
    }
  }

  "None ++ Some(Slice[T](...))" in {
    val merged: Iterable[Slice[Int]] = Some(Slice[Int](1, 2, 3)) ++ None
    merged.flatten
      .toList should contain inOrderOnly(1, 2, 3)
  }

  "++ empty slices" in {
    val merged: Slice[Int] = Slice.empty[Int] ++ Slice.empty[Int]
    merged shouldBe empty
    merged.size shouldBe 0
    merged.isEmpty shouldBe true
    merged.isFull shouldBe true
  }

  "++ empty and non empty slices" in {
    val merged: Slice[Int] = Slice.empty[Int] ++ Slice(1)
    merged should contain only 1
    merged.size shouldBe 1
    merged.isEmpty shouldBe false
    merged.isFull shouldBe true
  }

  "++ non empty and empty slices" in {
    val merged: Slice[Int] = Slice(1) ++ Slice.empty[Int]
    merged should contain only 1
    merged.size shouldBe 1
    merged.isEmpty shouldBe false
    merged.isFull shouldBe true
  }

  "++ non empty" in {
    val merged: Slice[Int] = Slice(1, 2, 3) ++ Slice(4, 5, 6)
    merged.isEmpty shouldBe false
    merged.isFull shouldBe true
    merged.toList should contain inOrderOnly(1, 2, 3, 4, 5, 6)
  }

  "within" when {
    implicit def toSlice(int: Int): Slice[Byte] = Slice.writeInt[Byte](int)

    implicit def toSliceMaxKey(int: MaxKey.Fixed[Int]): MaxKey[Slice[Byte]] = MaxKey.Fixed(Slice.writeInt[Byte](int.maxKey))

    implicit def toSliceMaxKeyRange(int: MaxKey.Range[Int]): MaxKey[Slice[Byte]] = MaxKey.Range(Slice.writeInt[Byte](int.fromKey), Slice.writeInt[Byte](int.maxKey))

    "max key is Fixed" in {
      //0
      //  1 - 10
      Slice.within(key = 0, minKey = 1, maxKey = MaxKey.Fixed(10)) shouldBe false
      //  1
      //  1 - 10
      Slice.within(key = 1, minKey = 1, maxKey = MaxKey.Fixed(10)) shouldBe true
      //    5
      //  1 - 10
      Slice.within(key = 5, minKey = 1, maxKey = MaxKey.Fixed(10)) shouldBe true
      //      10
      //  1 - 10
      Slice.within(key = 10, minKey = 1, maxKey = MaxKey.Fixed(10)) shouldBe true
      //        11
      //  1 - 10
      Slice.within(key = 11, minKey = 1, maxKey = MaxKey.Fixed(10)) shouldBe false
    }

    "max key is Range" in {
      //0
      //  1 - (10 - 20)
      Slice.within(key = 0, minKey = 1, maxKey = MaxKey.Range(10, 20)) shouldBe false
      //  1
      //  1 - (10 - 20)
      Slice.within(key = 1, minKey = 1, maxKey = MaxKey.Range(10, 20)) shouldBe true
      //    5
      //  1 - (10 - 20)
      Slice.within(key = 5, minKey = 1, maxKey = MaxKey.Range(10, 20)) shouldBe true
      //      10
      //  1 - (10 - 20)
      Slice.within(key = 10, minKey = 1, maxKey = MaxKey.Range(10, 20)) shouldBe true
      //        11
      //  1 - (10 - 20)
      Slice.within(key = 11, minKey = 1, maxKey = MaxKey.Range(10, 20)) shouldBe true
      //           19
      //  1 - (10 - 20)
      Slice.within(key = 19, minKey = 1, maxKey = MaxKey.Range(10, 20)) shouldBe true
      //            20
      //  1 - (10 - 20)
      Slice.within(key = 20, minKey = 1, maxKey = MaxKey.Range(10, 20)) shouldBe false
      //                21
      //  1 - (10 - 20)
      Slice.within(key = 21, minKey = 1, maxKey = MaxKey.Range(10, 20)) shouldBe false
    }
  }

  "reverse" should {
    "iterate in reverse" in {
      val slice = Slice(1, 2, 3, 4)
      slice.reverse.toList should contain inOrderOnly(4, 3, 2, 1)
    }

    "iterate of slices" in {
      val slice = Slice(1, 2, 3, 4, 5, 6)

      slice.take(2).reverse.toList should contain inOrderOnly(2, 1)
      slice.drop(2).take(2).reverse.toList should contain inOrderOnly(4, 3)
      slice.drop(4).take(2).reverse.toList should contain inOrderOnly(6, 5)
      slice.dropRight(2).reverse.toList should contain inOrderOnly(4, 3, 2, 1)
      slice.dropRight(0).reverse.toList should contain inOrderOnly(6, 5, 4, 3, 2, 1)

      slice.slice(0, 5).reverse.toList should contain inOrderOnly(6, 5, 4, 3, 2, 1)
    }

    "partially complete" in {
      val slice = Slice.create[Int](10)
      (1 to 6) foreach slice.add

      slice.reverse.toList should contain inOrderOnly(6, 5, 4, 3, 2, 1)
      val slice1 = slice.take(2)
      val slice2 = slice.drop(2).take(2)
      val slice3 = slice.drop(4).take(2)

      slice1.reverse.toList should contain inOrderOnly(2, 1)
      slice2.reverse.toList should contain inOrderOnly(4, 3)
      slice3.reverse.toList should contain inOrderOnly(6, 5)
    }

    "on empty" in {
      Slice.create[Int](10).reverse.toList shouldBe empty
    }
  }

  "minMax" should {
    val oneTwoInclusive = (Slice.writeInt[Byte](1), Slice.writeInt[Byte](2), true)
    val threeFourInclusive = (Slice.writeInt[Byte](3), Slice.writeInt[Byte](4), true)

    val oneTwoExclusive = (Slice.writeInt[Byte](1), Slice.writeInt[Byte](2), false)
    val threeFourExclusive = (Slice.writeInt[Byte](3), Slice.writeInt[Byte](4), false)

    "return one or the other on none" in {
      Slice.minMax(Some(oneTwoInclusive), None) should contain(oneTwoInclusive)
      Slice.minMax(None, Some(threeFourInclusive)) should contain(threeFourInclusive)

      Slice.minMax(Some(oneTwoExclusive), None) should contain(oneTwoExclusive)
      Slice.minMax(None, Some(threeFourExclusive)) should contain(threeFourExclusive)
    }

    "return none if nones" in {
      Slice.minMax(None, None) shouldBe empty
    }

    "return min and max" in {
      //1 - 1
      //1 - 1
      Slice.minMax(
        Some((Slice.writeInt[Byte](1), Slice.writeInt[Byte](1), true)),
        Some((Slice.writeInt[Byte](1), Slice.writeInt[Byte](1), true))) should contain((Slice.writeInt[Byte](1), Slice.writeInt[Byte](1), true))

      Slice.minMax(
        Some((Slice.writeInt[Byte](1), Slice.writeInt[Byte](1), false)),
        Some((Slice.writeInt[Byte](1), Slice.writeInt[Byte](1), true))) should contain((Slice.writeInt[Byte](1), Slice.writeInt[Byte](1), true))

      Slice.minMax(
        Some((Slice.writeInt[Byte](1), Slice.writeInt[Byte](1), true)),
        Some((Slice.writeInt[Byte](1), Slice.writeInt[Byte](1), false))) should contain((Slice.writeInt[Byte](1), Slice.writeInt[Byte](1), true))

      Slice.minMax(
        Some((Slice.writeInt[Byte](1), Slice.writeInt[Byte](1), false)),
        Some((Slice.writeInt[Byte](1), Slice.writeInt[Byte](1), false))) should contain((Slice.writeInt[Byte](1), Slice.writeInt[Byte](1), false))

      //1 - 5
      //  3 - 10
      Slice.minMax(
        Some((Slice.writeInt[Byte](1), Slice.writeInt[Byte](3), true)),
        Some((Slice.writeInt[Byte](3), Slice.writeInt[Byte](10), true))) should contain((Slice.writeInt[Byte](1), Slice.writeInt[Byte](10), true))

      Slice.minMax(
        Some((Slice.writeInt[Byte](1), Slice.writeInt[Byte](3), false)),
        Some((Slice.writeInt[Byte](3), Slice.writeInt[Byte](10), true))) should contain((Slice.writeInt[Byte](1), Slice.writeInt[Byte](10), true))

      Slice.minMax(
        Some((Slice.writeInt[Byte](1), Slice.writeInt[Byte](3), true)),
        Some((Slice.writeInt[Byte](3), Slice.writeInt[Byte](10), false))) should contain((Slice.writeInt[Byte](1), Slice.writeInt[Byte](10), false))

      Slice.minMax(
        Some((Slice.writeInt[Byte](1), Slice.writeInt[Byte](3), false)),
        Some((Slice.writeInt[Byte](3), Slice.writeInt[Byte](10), false))) should contain((Slice.writeInt[Byte](1), Slice.writeInt[Byte](10), false))

      //  3 - 10
      //1 - 5
      Slice.minMax(
        Some((Slice.writeInt[Byte](3), Slice.writeInt[Byte](10), true)),
        Some((Slice.writeInt[Byte](1), Slice.writeInt[Byte](3), true))) should contain((Slice.writeInt[Byte](1), Slice.writeInt[Byte](10), true))

      Slice.minMax(
        Some((Slice.writeInt[Byte](3), Slice.writeInt[Byte](10), false)),
        Some((Slice.writeInt[Byte](1), Slice.writeInt[Byte](3), true))) should contain((Slice.writeInt[Byte](1), Slice.writeInt[Byte](10), false))

      Slice.minMax(
        Some((Slice.writeInt[Byte](3), Slice.writeInt[Byte](10), true)),
        Some((Slice.writeInt[Byte](1), Slice.writeInt[Byte](3), false))) should contain((Slice.writeInt[Byte](1), Slice.writeInt[Byte](10), true))

      Slice.minMax(
        Some((Slice.writeInt[Byte](3), Slice.writeInt[Byte](10), false)),
        Some((Slice.writeInt[Byte](1), Slice.writeInt[Byte](3), false))) should contain((Slice.writeInt[Byte](1), Slice.writeInt[Byte](10), false))
    }
  }

  "take from index" in {
    val slice = Slice(1, 2, 3, 4, 5, 6)
    slice.take(0, 0) shouldBe Slice.empty[Int]
    slice.take(0, 1) shouldBe Slice(1)
    slice.take(0, 2) shouldBe Slice(1, 2)
    slice.take(0, 3) shouldBe Slice(1, 2, 3)
    slice.take(0, 4) shouldBe Slice(1, 2, 3, 4)
    slice.take(0, 5) shouldBe Slice(1, 2, 3, 4, 5)
    slice.take(0, 6) shouldBe Slice(1, 2, 3, 4, 5, 6)
    slice.take(0, 7) shouldBe Slice(1, 2, 3, 4, 5, 6)

    val grouped = Slice(1, 2, 3, 4, 5, 6).groupedSlice(2)
    grouped should have size 2

    //1, 2, 3
    val slice1 = grouped(0)
    slice1.take(0, 0) shouldBe Slice.empty[Int]
    slice1.take(0, 1) shouldBe Slice(1)
    slice1.take(0, 2) shouldBe Slice(1, 2)
    slice1.take(0, 3) shouldBe Slice(1, 2, 3)
    slice1.take(0, 4) shouldBe Slice(1, 2, 3)

    //4, 5, 6
    val slice2 = grouped(1)
    slice2.take(0, 0) shouldBe Slice.empty[Int]
    slice2.take(0, 1) shouldBe Slice(4)
    slice2.take(0, 2) shouldBe Slice(4, 5)
    slice2.take(0, 3) shouldBe Slice(4, 5, 6)
    slice2.take(0, 4) shouldBe Slice(4, 5, 6)
  }

  "manually adjusting slice random testing 1" in {
    val slice = Slice.create[Int](10)

    slice.moveWritePosition(3)

    slice.size shouldBe 3
    slice add 4
    slice(3) shouldBe 4
    slice.size shouldBe 4
    slice addAll Slice(5, 6, 7, 8, 9, 10)
    slice.size shouldBe 10

    slice.head shouldBe 0
    slice.last shouldBe 10

    slice.moveWritePosition(0)
    slice.size shouldBe 10

    slice.slice(0, 2).isFull shouldBe true
    slice.slice(2, 5).isFull shouldBe true
    slice.slice(5, 9).isFull shouldBe true
    slice.slice(0, 9).isFull shouldBe true
    slice.take(Int.MaxValue).isFull shouldBe true
  }

  "manually adjusting slice random testing 2" in {
    val slice = Slice.create[Int](10)

    slice.moveWritePosition(5)
    slice add 6
    slice.size shouldBe 6
    slice.moveWritePosition(0)
    slice add 1
    slice.size shouldBe 6
    slice add 2
    slice add 3
    slice add 4
    slice add 5
    slice.size shouldBe 6

    slice.slice(5, 6).isEmpty shouldBe false
    slice.slice(5, 6).size shouldBe 1
    slice.slice(5, 7).size shouldBe 1
    slice.slice(5, 8).size shouldBe 1
    slice.slice(5, 9).size shouldBe 1

    slice.slice(6, 7).isEmpty shouldBe true
    slice.slice(7, 8).isEmpty shouldBe true
    slice.slice(9, 9).isEmpty shouldBe true
  }

  "manually adjusting slice random testing with addAll" in {
    val slice = Slice.create[Int](10)

    slice moveWritePosition 5
    slice addAll Slice(1, 2, 3, 4)
    slice.size shouldBe 9
    //move the same position and write again. Size should remain the same
    slice moveWritePosition 5
    slice addAll Slice(1, 2, 3, 4)
    slice.size shouldBe 9

    slice add 1
    slice.size shouldBe 10

    assertThrows[ArrayIndexOutOfBoundsException] {
      slice add 1
    }
    slice.size shouldBe 10

    slice.last shouldBe 1
    slice moveWritePosition 9
    slice add 2
    slice.last shouldBe 2
    slice.size shouldBe 10
  }

  "closing an empty slice" in {
    val close0 = Slice.create(0).close()
    close0.size shouldBe 0
    close0.size shouldBe 0
    close0.fromOffset shouldBe 0
    close0.toList shouldBe List.empty

    val close1 = Slice.create(1).close()
    close1.size shouldBe 0
    close1.size shouldBe 0
    close1.fromOffset shouldBe 0
    close1.toList shouldBe List.empty
  }

  "moved a closed sub slice" in {
    val slice = Slice.create[Int](10)
    val subSlice = slice.slice(0, 4).close()

    //can only write to a subslice
    (5 to 20) foreach {
      i =>
        assertThrows[ArrayIndexOutOfBoundsException] {
          subSlice.moveWritePosition(i)
        }
    }
    slice add 1
    subSlice shouldBe empty
    slice should contain only 1
  }

  "equals" in {
    val slice = Slice.fill(10)(1)
    slice == Slice.fill(10)(1) shouldBe true

    slice.dropHead() == slice shouldBe false
    slice.dropHead() == Slice.fill(9)(1) shouldBe true

    Slice.empty == Slice.empty shouldBe true
  }

  "toOptionUnsliced" in {
    Slice.empty.toOptionUnsliced() shouldBe None
    Slice.emptyEmptyBytes.toOptionUnsliced() shouldBe None
    Slice(1, 2, 3).take(0).toOptionUnsliced() shouldBe None
    Slice(1, 2, 3).drop(3).toOptionUnsliced() shouldBe None
    Slice(1, 2, 3).drop(1).toOptionUnsliced() shouldBe defined
    Slice(1, 2, 3).drop(1).drop(1).toOptionUnsliced() shouldBe defined
    Slice(1, 2, 3).drop(1).drop(1).drop(1).toOptionUnsliced() shouldBe None
  }

  "toOption" in {
    Slice.emptyBytes.toOption shouldBe empty
    Slice(1, 2, 3).toOption shouldBe Some(Slice(1, 2, 3))

    val slice = Slice(1, 2, 3)
    val slice1 = slice.take(1).toOption.get
    slice1 should have size 1
    slice1 shouldBe Slice(1)
    slice1.underlyingArraySize shouldBe 3
  }

  "indexOf" when {

    "empty" in {
      Slice.emptyBytes.indexOf(0) shouldBe empty
      Slice.emptyBytes.indexOf(1) shouldBe empty
    }

    "single" in {
      val bytes = Slice(1)

      bytes.indexOf(0) shouldBe empty
      bytes.indexOf(1) shouldBe Some(0)
    }

    "many" in {
      val bytes = Slice(1, 2, 3, 4, 5)

      bytes.indexOf(0) shouldBe empty
      bytes.indexOf(1) shouldBe Some(0)
      bytes.indexOf(2) shouldBe Some(1)
      bytes.indexOf(3) shouldBe Some(2)
      bytes.indexOf(4) shouldBe Some(3)
      bytes.indexOf(5) shouldBe Some(4)
      bytes.indexOf(6) shouldBe empty
    }
  }

  "dropTo" when {
    "empty" in {
      Slice.emptyBytes.dropTo(1) shouldBe empty
      Slice.emptyBytes.dropTo(Byte.MaxValue) shouldBe empty
      Slice.emptyBytes.dropTo(Byte.MinValue) shouldBe empty
    }

    "single" in {
      val bytes = Slice(1)

      bytes.dropTo(1).value shouldBe empty
      bytes.dropTo(2) shouldBe empty
    }

    "many" in {
      val bytes = Slice(1, 2, 3, 4, 5)

      bytes.dropTo(0) shouldBe empty
      bytes.dropTo(1).value shouldBe Slice(2, 3, 4, 5)
      bytes.dropTo(2).value shouldBe Slice(3, 4, 5)
      bytes.dropTo(3).value shouldBe Slice(4, 5)
      bytes.dropTo(4).value shouldBe Slice(5)
      bytes.dropTo(5).value shouldBe empty
      bytes.dropTo(6) shouldBe empty
    }
  }

  "dropUntil" when {
    "empty" in {
      Slice.emptyBytes.dropUntil(1) shouldBe empty
      Slice.emptyBytes.dropUntil(Byte.MaxValue) shouldBe empty
      Slice.emptyBytes.dropUntil(Byte.MinValue) shouldBe empty
    }

    "single" in {
      val bytes = Slice(1)

      bytes.dropUntil(1).value shouldBe bytes
      bytes.dropUntil(2) shouldBe empty
    }

    "many" in {
      val bytes = Slice(1, 2, 3, 4, 5)

      bytes.dropUntil(0) shouldBe empty
      bytes.dropUntil(1).value shouldBe Slice(1, 2, 3, 4, 5)
      bytes.dropUntil(2).value shouldBe Slice(2, 3, 4, 5)
      bytes.dropUntil(3).value shouldBe Slice(3, 4, 5)
      bytes.dropUntil(4).value shouldBe Slice(4, 5)
      bytes.dropUntil(5).value shouldBe Slice(5)
      bytes.dropUntil(6) shouldBe empty
    }
  }

  "hashCode" should {
    "be same for partially and fully written slice" in {
      val partiallyWritten = Slice.create[Int](100)
      partiallyWritten.add(1)
      partiallyWritten.add(2)
      partiallyWritten.add(3)
      partiallyWritten.add(4)
      partiallyWritten.add(5)

      val bytes =
        Seq(
          Slice(1, 2, 3, 4, 5),
          partiallyWritten
        )

      partiallyWritten.underlyingArraySize shouldBe 100

      bytes foreach {
        bytes =>
          bytes.hashCode() shouldBe bytes.##
          bytes.drop(1).hashCode() shouldBe Slice(2, 3, 4, 5).##
          bytes.drop(2).hashCode() shouldBe Slice(3, 4, 5).##
          bytes.drop(3).hashCode() shouldBe Slice(4, 5).##
          bytes.drop(4).hashCode() shouldBe Slice(5).##
          bytes.drop(5).hashCode() shouldBe Slice[Int]().##

          bytes.dropRight(1).hashCode() shouldBe Slice(1, 2, 3, 4).##
          bytes.dropRight(2).hashCode() shouldBe Slice(1, 2, 3).##
          bytes.dropRight(3).hashCode() shouldBe Slice(1, 2).##
          bytes.dropRight(4).hashCode() shouldBe Slice(1).##
          bytes.dropRight(5).hashCode() shouldBe Slice[Int]().##
      }
    }
  }

  "head and last" when {
    "empty" in {
      val slice = Slice.empty[Int]
      slice.headOrNull.asInstanceOf[Integer] shouldBe null
      slice.lastOrNull.asInstanceOf[Integer] shouldBe null
      slice.headOption shouldBe empty
      slice.lastOption shouldBe empty
    }
  }

  "existsFor" in {
    val slice = Slice.range(1, 100)

    //[]
    slice.existsFor(0, _ => fail("should not have run")) shouldBe false
    slice.existsFor(-1, _ => fail("should not have run")) shouldBe false
    slice.existsFor(Int.MinValue, _ => fail("should not have run")) shouldBe false

    slice.existsFor(1, _ == 1) shouldBe true
    slice.existsFor(9, _ % 10 == 0) shouldBe false
    slice.existsFor(10, _ % 10 == 0) shouldBe true
    slice.existsFor(20, _ == 20) shouldBe true
  }

  "range of ints" in {
    val range = Slice.range(1, 10)
    range.underlyingArraySize shouldBe 10
    range.toList shouldBe (1 to 10)
  }

  "range of bytes" in {
    val range = Slice.range(1.toByte, 10.toByte)
    range.underlyingArraySize shouldBe 10
    range.toList shouldBe (1 to 10)
  }

  "closeWritten" when {
    "empty" in {
      val slice = Slice.empty[Int]
      val (left, right) = slice.splitUnwritten()

      left.isEmpty shouldBe true
      left.underlyingArraySize shouldBe 0
      assertThrows[ArrayIndexOutOfBoundsException](left add 11)

      right.isEmpty shouldBe true
      right.underlyingArraySize shouldBe 0
      assertThrows[ArrayIndexOutOfBoundsException](right add 11)
    }

    "return empty for unwritten bytes" in {
      val slice = Slice.create[Int](10)
      val (left, right) = slice.splitUnwritten()

      left.isEmpty shouldBe true
      left.underlyingArraySize shouldBe 0
      assertThrows[ArrayIndexOutOfBoundsException](left add 11)

      right.isEmpty shouldBe true
      right.fromOffset shouldBe 0
      right.underlyingArraySize shouldBe 10
      right add 1
      right should contain only 1

      val (written, unwritten) = right.splitUnwritten()
      written should contain only 1
      unwritten.underlyingArraySize shouldBe 10
      unwritten.currentWritePosition shouldBe 1
      unwritten add 2
      unwritten add 3
      unwritten should contain only(2, 3)
    }

    "close written bytes" in {
      val slice = Slice.create[Int](10)

      (1 to 5) foreach slice.add

      val (written, unwritten) = slice.splitUnwritten()

      written.size shouldBe 5
      written should contain allElementsOf (1 to 5)
      unwritten.isEmpty shouldBe true
      unwritten.underlyingArraySize shouldBe 10
      unwritten.currentWritePosition shouldBe 5
      unwritten add 6
      unwritten add 7
      unwritten should contain only(6, 7)

      val (written2, unwritten2) = unwritten.splitUnwritten()
      written2 should contain only(6, 7)
      unwritten2.underlyingArraySize shouldBe 10
      unwritten2.currentWritePosition shouldBe 7
      unwritten2 add 8
      unwritten2 add 9
      unwritten2 should contain only(8, 9)

      val (written3, unwritten4) = unwritten2.splitUnwritten()
      written3 should contain only(8, 9)
      unwritten4.underlyingArraySize shouldBe 10
      unwritten4.currentWritePosition shouldBe 9
      unwritten4 add 10
      unwritten4 should contain only 10

      val (written4, unwritten5) = unwritten4.splitUnwritten()
      written4 should contain only 10
      unwritten5.underlyingArraySize shouldBe 0
      assertThrows[ArrayIndexOutOfBoundsException] {
        unwritten5 add 11
      }
    }
  }
}
