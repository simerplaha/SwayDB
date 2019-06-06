
package swaydb.core.util

import java.nio.ByteBuffer

import swaydb.core.data.KeyValue
import swaydb.core.io.reader.Reader
import swaydb.data.IO
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

/**
  * Credit: Follows bit shifting logic from https://github.com/alexandrnikitin/bloom-filter-scala.
  */
object BloomFilter {

  val empty =
    new BloomFilter(
      startOffset = 0,
      _maxStartOffset = 0,
      numberOfBits = 0,
      numberOfHashes = 0,
      buffer = ByteBuffer.allocate(0)
    )

  val byteBufferStartOffset = ByteSizeOf.int + ByteSizeOf.int + ByteSizeOf.int

  def apply(numberOfItems: Int, falsePositiveRate: Double): BloomFilter = {
    val numberOfBits = optimalNumberOfBits(numberOfItems, falsePositiveRate)
    val numberOfHashes = optimalNumberOfHashes(numberOfItems, numberOfBits)
    val buffer = ByteBuffer.allocate(byteBufferStartOffset + numberOfBits)
    buffer.putInt(0)
    buffer.putInt(numberOfBits)
    buffer.putInt(numberOfHashes)
    new BloomFilter(
      _maxStartOffset = 0,
      startOffset = byteBufferStartOffset,
      numberOfBits = numberOfBits,
      numberOfHashes = numberOfHashes,
      buffer = buffer
    )
  }

  def optimalNumberOfBits(numberOfItems: Long, falsePositiveRate: Double): Int =
    math.ceil(-1 * numberOfItems * math.log(falsePositiveRate) / math.log(2) / math.log(2)).toInt

  def optimalNumberOfHashes(numberOfItems: Int, numberOfBits: Long): Int =
    math.ceil(numberOfBits / numberOfItems * math.log(2)).toInt

  def apply(slice: Slice[Byte]): IO[BloomFilter] = {
    val reader = Reader(slice)
    for {
      maxStartOffset <- reader.readInt()
      numberOfBits <- reader.readInt()
      numberOfHashes <- reader.readInt()
    } yield {
      new BloomFilter(
        startOffset = byteBufferStartOffset,
        _maxStartOffset = maxStartOffset,
        numberOfBits = numberOfBits,
        numberOfHashes = numberOfHashes,
        buffer = slice.toByteBuffer
      )
    }
  }

  def byteSize(numberOfItems: Int, falsePositiveRate: Double): Int =
    BloomFilter.optimalNumberOfBits(
      numberOfItems = numberOfItems,
      falsePositiveRate = falsePositiveRate
    ) + ByteSizeOf.int + ByteSizeOf.int

  /**
    * Initialise bloomFilter if key-values do no contain remove range.
    */
  def init(keyValues: Iterable[KeyValue.WriteOnly],
           bloomFilterFalsePositiveRate: Double): Option[BloomFilter] =
    keyValues.lastOption flatMap {
      last =>
        if (last.stats.hasRemoveRange)
          None
        else
          Some(
            BloomFilter(
              numberOfItems = last.stats.bloomFilterItemsCount,
              falsePositiveRate = bloomFilterFalsePositiveRate
            )
          )
    }
}

class BloomFilter(val startOffset: Int,
                  private var _maxStartOffset: Int,
                  val numberOfBits: Int,
                  val numberOfHashes: Int,
                  buffer: ByteBuffer) {

  def maxStartOffset: Int =
    _maxStartOffset

  private def get(index: Long): Boolean =
    (buffer.getLong(startOffset + ((index >>> 6) * 8L).toInt) & (1L << index)) != 0

  private def set(index: Long): Unit = {
    val offset = startOffset + (index >>> 6) * 8L
    _maxStartOffset = _maxStartOffset max offset.toInt
    val long = buffer.getLong(offset.toInt)
    if ((long & (1L << index)) == 0)
      buffer.putLong(offset.toInt, long | (1L << index))
  }

  def add(item: Slice[Byte]): Unit = {
    val hash = MurmurHash3Generic.murmurhash3_x64_64(item, 0, item.size, 0)
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    var i = 0
    while (i < numberOfHashes) {
      val computedHash = hash1 + i * hash2
      set((computedHash & Long.MaxValue) % numberOfBits)
      i += 1
    }
  }

  def mightContain(item: Slice[Byte]): Boolean = {
    val hash = MurmurHash3Generic.murmurhash3_x64_64(item, 0, item.size, 0)
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    var i = 0
    while (i < numberOfHashes) {
      val computedHash = hash1 + i * hash2
      if (!get((computedHash & Long.MaxValue) % numberOfBits))
        return false
      i += 1
    }
    true
  }

  def toSlice: Slice[Byte] = {
    buffer.putInt(0, maxStartOffset)
    Slice(buffer.array()).slice(0, maxStartOffset + 7)
  }

  override def hashCode(): Int =
    buffer.hashCode()
}
