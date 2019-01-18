/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.data.slice

import org.scalatest.{Matchers, WordSpec}
import scala.util.Random
import swaydb.data.order.KeyOrder
import swaydb.data.repairAppendix.MaxKey
import swaydb.data.util.ByteSizeOf

class SliceSpec extends WordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default

  def randomByte() = (Random.nextInt(256) - 128).toByte

  "A Slice" should {
    "be created by specifying it's length" in {
      val slice = Slice.create(10)
      slice.size shouldBe 10
      slice.written shouldBe 0
      slice.fromOffset shouldBe 0
      slice.toOffset shouldBe 9
    }

    "be created from an Array" in {
      val array = Array.fill[Byte](10)(1)
      val slice = Slice[Byte](array)
      slice.size shouldBe 10
      slice.written shouldBe 10
      slice.fromOffset shouldBe 0
      slice.toOffset shouldBe 9
    }

    "be created from another Slice" in {
      val array = Array.fill[Int](3)(Random.nextInt())
      val slice1 = Slice[Int](array)
      slice1.size shouldBe 3

      val slice2 = slice1.slice(1, 2)
      slice2.size shouldBe 2
      slice2.written shouldBe 2
      slice2.fromOffset shouldBe 1
      slice2.toOffset shouldBe 2
      slice2.toList should contain inOrderElementsOf List(array(1), array(2))
      slice2.underlyingArraySize shouldBe slice1.size

      val slice2Copy = slice2.unslice()
      slice2Copy.size shouldBe 2
      slice2Copy.written shouldBe 2
      slice2Copy.underlyingArraySize shouldBe 2
    }

    "be sliced for a partially written slice" in {
      //slice0 is (10, 10, null, null)
      val slice0 = Slice.create[Int](4)
      slice0 add 10
      slice0 add 10 //second slice starts here
      slice0.written shouldBe 2

      //slice1 = (10, 10)
      val slice1 = slice0.slice(0, 1)
      slice1.written shouldBe 2
      slice1.toArray shouldBe Array(10, 10)
      slice1.underlyingArraySize shouldBe 4

      //slice1 = (10, null)
      val slice2 = slice0.slice(1, 2)
      slice2.written shouldBe 1
      slice2.toArray shouldBe Array(10, 0)

      //slice1 = (null, null)
      val slice3 = slice0.slice(2, 3)
      slice3.written shouldBe 0
      slice3.toArray shouldBe Array(0, 0)

      //slice4 = (10, 10, null, null)
      val slice4 = slice0.slice(0, 3)
      slice4.written shouldBe 2
      slice4.toArray shouldBe Array(10, 10, 0, 0)
    }

    "be sliced if the original slice is full written" in {
      //slice0 = (1, 2, 3, 4)
      val slice0 = Slice.create[Int](4)
      slice0 add 1
      slice0 add 2
      slice0 add 3
      slice0 add 4
      slice0.written shouldBe 4

      //slice1 = (1, 2)
      val slice1 = slice0.slice(0, 1)
      slice1.written shouldBe 2
      slice1.toArray shouldBe Array(1, 2)

      //slice1 = (2, 3)
      val slice2 = slice0.slice(1, 2)
      slice2.written shouldBe 2
      slice2.toArray shouldBe Array(2, 3)

      //slice1 = (3, 4)
      val slice3 = slice0.slice(2, 3)
      slice3.written shouldBe 2
      slice3.toArray shouldBe Array(3, 4)

      //slice4 = (1, 2, 3, 4)
      val slice4 = slice0.slice(0, 3)
      slice4.written shouldBe 4
      slice4.toArray shouldBe Array(1, 2, 3, 4)
    }

    "throw ArrayIndexOutOfBoundsException when creating a sub Slice with invalid offsets" in {
      val slice1 = Slice.fill(3)(Random.nextInt())
      slice1.size shouldBe 3
      assertThrows[ArrayIndexOutOfBoundsException] {
        slice1.slice(0, 3)
      }
      //valid subslice 2
      val slice2 = slice1.slice(1, 2)
      slice2.size shouldBe 2

      assertThrows[ArrayIndexOutOfBoundsException] {
        slice2.slice(0, 2)
      }
    }

    "throw ArrayIndexOutOfBoundsException when inserting items outside the Slice offset" in {
      val slice = Slice.create[Byte](1)
      slice.size shouldBe 1
      slice.written shouldBe 0
      slice.fromOffset shouldBe 0
      slice.toOffset shouldBe 0

      slice.add(1).written shouldBe 1
      assertThrows[ArrayIndexOutOfBoundsException] {
        slice.add(1)
      }
      slice.written shouldBe 1
    }

    "throw ArrayIndexOutOfBoundsException when adding items outside it's offset and when the Slice is a sub slice" in {
      val slice1 = Slice.fill(4)(Random.nextInt())
      slice1.written shouldBe 4

      val slice2 = slice1.slice(1, 2)
      slice2.written shouldBe 2

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
          slice.get(index) shouldBe array(index)
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

    "update original slice when splits are updated" in {
      val originalSlice = Slice.create[Int](2)
      val (split1, split2) = originalSlice.splitAt(1)
      split1.size shouldBe 1
      split2.size shouldBe 1

      split1.add(100)
      split2.add(200)

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
      slice addIntUnsigned Int.MaxValue
      slice.createReader().readIntUnsigned() shouldBe Int.MaxValue
    }

    "write and read Unsigned Long" in {
      val slice = Slice.create[Byte](ByteSizeOf.long + 1)
      slice addLongUnsigned Long.MaxValue
      slice.createReader().readLongUnsigned() shouldBe Long.MaxValue
    }

    "write and read String" in {
      val slice = Slice.create[Byte](10000)
      slice addString "This is a string"
      slice.close().createReader().readRemainingAsString() shouldBe "This is a string"
    }

    "write and read remaining string String" in {
      val slice = Slice.create[Byte](10000)

      slice addInt 1
      slice addLong 2L
      slice addIntUnsigned 3
      slice addLongUnsigned 4L
      slice addIntSigned -3
      slice addLongSigned -4L
      slice addString "This is a string"

      val reader = slice.close().createReader()
      reader.readInt() shouldBe 1
      reader.readLong() shouldBe 2L
      reader.readIntUnsigned() shouldBe 3
      reader.readLongUnsigned() shouldBe 4L
      reader.readIntSigned() shouldBe -3
      reader.readLongSigned() shouldBe -4L
      reader.readRemainingAsString() shouldBe "This is a string"
    }

    "write and read String of specified size" in {
      val slice = Slice.create[Byte](10000)
      slice addString "This is a string"

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
    merged.written shouldBe 0
    merged.isEmpty shouldBe true
    merged.isFull shouldBe true
  }

  "++ empty and non empty slices" in {
    val merged: Slice[Int] = Slice.empty[Int] ++ Slice(1)
    merged should contain only 1
    merged.written shouldBe 1
    merged.isEmpty shouldBe false
    merged.isFull shouldBe true
  }

  "++ non empty and empty slices" in {
    val merged: Slice[Int] = Slice(1) ++ Slice.empty[Int]
    merged should contain only 1
    merged.written shouldBe 1
    merged.isEmpty shouldBe false
    merged.isFull shouldBe true
  }

  "within" when {
    implicit def toSlice(int: Int): Slice[Byte] = Slice.writeInt(int)

    implicit val order = Ordering.by[Slice[Byte], Int](_.readInt())(Ordering.Int)

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

}
