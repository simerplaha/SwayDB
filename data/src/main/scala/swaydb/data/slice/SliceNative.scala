package swaydb.data.slice

import swaydb.data.unsafe.{Unsafe, UnsafeMemory}

import scala.annotation.unchecked.uncheckedVariance
import scala.reflect.ClassTag

object SliceNative {

  val emptyBytes = of[Byte](0)

  @inline final def empty[A: ClassTag](implicit memory: UnsafeMemory[A]) =
    of[A](0)

  @inline final def of[A: ClassTag](length: Int, isFull: Boolean = false)(implicit memory: UnsafeMemory[A]): SliceNative[A] =
    new SliceNative[A](
      address = Unsafe.get.allocateMemory(memory.size * length),
      fromOffset = 0,
      toOffset = if (length == 0) -1 else length - 1,
      written = if (isFull) length else 0
    )

  @inline final def apply[A: ClassTag](data: Array[A])(implicit memory: UnsafeMemory[A]): SliceNative[A] =
    if (data.length == 0) {
      of[A](0)
    } else {
      val address = memory.allocate(memory.size * data.length)

      data.foldLeft(address) {
        case (address, data) =>
          memory.put(address, data)
          address + memory.size
      }

      new SliceNative[A](
        address = address,
        fromOffset = 0,
        toOffset = data.length - 1,
        written = data.length
      )
    }

  @inline final def apply[A: ClassTag](data: A*)(implicit memory: UnsafeMemory[A]): SliceNative[A] =
    SliceNative(data.toArray)

}

/**
 * [[SliceNative]] is similar to [[Slice]]. Difference is that [[SliceNative]] stores data
 * off-heap with [[sun.misc.Unsafe]]
 *
 * [[free]] should be called to deallocate data or else [[finalize]] will deallocate data
 * when this [[SliceNative]] instance is cleaned ensuring that memory is always released.
 *
 * @param address memory location of stored data.
 */
class SliceNative[+T: ClassTag](val address: Long,
                                val fromOffset: Int,
                                val toOffset: Int,
                                private var written: Int,
                                @volatile private var memoryFreed: Boolean = false)(implicit memory: UnsafeMemory[T]) extends Iterable[T] { self =>

  private var writePosition = fromOffset + written

  val allocatedSize =
    toOffset - fromOffset + 1

  override def size: Int =
    written

  override def isEmpty =
    size == 0

  override def nonEmpty =
    !isEmpty

  def isFull =
    size == allocatedSize

  def add(item: T@uncheckedVariance): SliceNative[T] = {
    if (writePosition < fromOffset || writePosition > toOffset) throw new ArrayIndexOutOfBoundsException(writePosition)
    memory.put(address + writePosition, item)
    writePosition += 1
    written = (writePosition - fromOffset) max written
    self
  }

  /**
   * Create a new SliceNative for the offsets.
   *
   * @param fromOffset start offset
   * @param toOffset   end offset
   * @return SliceNative for the given offsets
   */
  override def slice(fromOffset: Int, toOffset: Int): SliceNative[T] =
    if (toOffset < 0) {
      SliceNative.empty[T]
    } else {
      //overflow check
      var fromOffsetAdjusted = fromOffset + this.fromOffset
      var toOffsetAdjusted = fromOffsetAdjusted + (toOffset - fromOffset)

      if (fromOffsetAdjusted < this.fromOffset)
        fromOffsetAdjusted = this.fromOffset

      if (toOffsetAdjusted > this.toOffset)
        toOffsetAdjusted = this.toOffset

      if (fromOffsetAdjusted > toOffsetAdjusted) {
        SliceNative.empty[T]
      } else {
        val actualWritePosition = this.fromOffset + written //in-case the slice was manually moved.
        val sliceWritePosition =
          if (actualWritePosition <= fromOffsetAdjusted) //not written
            0
          else if (actualWritePosition > toOffsetAdjusted) //fully written
            toOffsetAdjusted - fromOffsetAdjusted + 1
          else //partially written
            actualWritePosition - fromOffsetAdjusted

        new SliceNative[T](
          address = address,
          fromOffset = fromOffsetAdjusted,
          toOffset = toOffsetAdjusted,
          written = sliceWritePosition
        )
      }
    }

  override def iterator = new Iterator[T] {
    private val writtenPosition = fromOffset + self.size - 1
    private var index = fromOffset

    override def hasNext: Boolean =
      index <= toOffset && index <= writtenPosition

    override def next(): T = {
      val next = memory.get(address, index)
      index += 1
      next
    }
  }

  def free(): Unit = {
    memory.free(address)
    memoryFreed = true
  }

  override def finalize(): Unit =
    if (!memoryFreed)
      memory.free(address)
}
