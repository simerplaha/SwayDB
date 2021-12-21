package swaydb.slice

import org.scalatest.matchers.should.Matchers._
import swaydb.testkit.TestKit._

import scala.collection.compat.IterableOnce
import scala.reflect.ClassTag

object SliceTestKit {

  implicit class SliceTestSliceByteImplicits(actual: Slice[Byte]) {
    def shouldBeCut(): Unit =
      actual.underlyingArraySize shouldBe actual.toArrayCopy[Byte].length
  }

  implicit class OptionSliceByteImplicits(actual: Option[Slice[Byte]]) {
    def shouldBeCut(): Unit =
      actual foreach (_.shouldBeCut())
  }

  implicit class ToSlice[T: ClassTag](items: IterableOnce[T]) {
    def toSlice: Slice[T] = {
      val listItems = items.iterator.toList
      val slice = Slice.allocate[T](listItems.size)
      listItems foreach slice.add
      slice
    }
  }

  def genStringOption(): Option[Slice[Byte]] =
    if (randomBoolean())
      Some(Slice.writeString(randomString()))
    else
      None

  def genStringSliceOptional(): SliceOption[Byte] =
    if (randomBoolean())
      Slice.writeString(randomString())
    else
      Slice.Null

  def genByteChunks(size: Int = 10, sizePerChunk: Int = 10): Slice[Slice[Byte]] = {
    val slice = Slice.allocate[Slice[Byte]](size)
    (1 to size) foreach {
      _ =>
        slice add Slice.wrap(randomBytes(sizePerChunk))
    }
    slice
  }

  def genBytesSlice(size: Int = 10): Slice[Byte] =
    Slice.wrap(randomBytes(size))

  def genBytesSliceOption(size: Int = 10): Option[Slice[Byte]] =
    genBytesSliceOptional(size).toOptionC

  def genBytesSliceOptional(size: Int = 10): SliceOption[Byte] =
    if (randomBoolean() || size == 0)
      Slice.Null
    else
      genBytesSlice(size)

  def someByteSlice(size: Int = 10): Option[Slice[Byte]] =
    if (size == 0)
      None
    else
      Some(genBytesSlice(size))

}
