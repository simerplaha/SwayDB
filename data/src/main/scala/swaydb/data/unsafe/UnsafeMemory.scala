package swaydb.data.unsafe

import swaydb.utils.ByteSizeOf

/**
 * Unsafe API for target data types
 */
sealed trait UnsafeMemory[T] {
  val size: Int

  def get(address: Long): T

  def put(address: Long, data: T): Unit

  def free(address: Long): Unit =
    Unsafe.get.freeMemory(address)

  def allocate(bytes: Long): Long =
    Unsafe.get.allocateMemory(bytes)

}

object UnsafeMemory {

  implicit object ByteUnsafeMemory extends UnsafeMemory[Byte] {
    override val size: Int =
      ByteSizeOf.byte

    override def get(address: Long): Byte =
      Unsafe.get.getByte(address)

    override def put(address: Long, data: Byte): Unit =
      Unsafe.get.putByte(address, data)

  }

  implicit object IntUnsafeMemory extends UnsafeMemory[Int] {
    override val size: Int =
      ByteSizeOf.int

    override def get(address: Long): Int =
      Unsafe.get.getInt(address)

    override def put(address: Long, data: Int): Unit =
      Unsafe.get.putInt(address, data)
  }
}
