package swaydb.core.util

import swaydb.IO
import swaydb.core.data.Transient
import swaydb.data.MaxKey
import swaydb.data.slice.Slice

private[core] object KeyCompressor {

  /**
   * @return (minKey, maxKey, fullKey)
   */
  def compress(head: Option[Transient],
               last: Transient): (Slice[Byte], MaxKey[Slice[Byte]], Slice[Byte]) =
    (head, last) match {
      case (Some(keyValue), fixed: Transient.Fixed) =>
        val fullKey = Bytes.compressJoin(keyValue.key, fixed.key, 0.toByte)
        (keyValue.key, MaxKey.Fixed(fixed.key), fullKey)

      case (Some(keyValue), range: Transient.Range) =>
        val maxKey = Bytes.compressJoin(range.fromKey, range.toKey)
        val fullKey = Bytes.compressJoin(keyValue.key, maxKey, 1.toByte)
        (keyValue.key, MaxKey.Range(range.fromKey, range.toKey), fullKey)

      case (None, fixed: Transient.Fixed) =>
        (fixed.key, MaxKey.Fixed(fixed.key), fixed.key append 2.toByte)

      case (None, range: Transient.Range) =>
        val mergedKey = Bytes.compressJoin(range.fromKey, range.toKey, 3.toByte)
        (range.fromKey, MaxKey.Range(range.fromKey, range.toKey), mergedKey)
    }

  def decompress(key: Slice[Byte]): IO[swaydb.Error.Segment, (Slice[Byte], MaxKey[Slice[Byte]])] =
    key.lastOption match {
      case Some(byte) =>
        if (byte == 0)
          Bytes.decompressJoin(key.dropRight(1)) map {
            case (minKey, maxKey) =>
              (minKey, MaxKey.Fixed(maxKey))
          }
        else if (byte == 1)
          Bytes.decompressJoin(key.dropRight(1)) flatMap {
            case (minKey, rangeMaxKey) =>
              Bytes.decompressJoin(rangeMaxKey) map {
                case (fromKey, toKey) =>
                  (minKey, MaxKey.Range(fromKey, toKey))
              }
          }
        else if (byte == 2) {
          val keyWithoutId = key.dropRight(1)
          IO.Right[swaydb.Error.Segment, (Slice[Byte], MaxKey[Slice[Byte]])](keyWithoutId, MaxKey.Fixed(keyWithoutId))
        }
        else if (byte == 3)
          Bytes.decompressJoin(key.dropRight(1)) map {
            case (minKey, maxKey) =>
              (minKey, MaxKey.Range(minKey, maxKey))
          }
        else
          IO.failed[swaydb.Error.Segment, (Slice[Byte], MaxKey[Slice[Byte]])](s"Invalid byte: $byte")

      case None =>
        IO.failed[swaydb.Error.Segment, (Slice[Byte], MaxKey[Slice[Byte]])]("Key is empty")
    }
}
