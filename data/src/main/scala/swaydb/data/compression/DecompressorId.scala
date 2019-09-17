/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.data.compression

import swaydb.Compression
import swaydb.macros.SealedList

import scala.util.Random

/**
 * These IDs are used internally by core and are not exposed to the outside. To create an
 * lZ4 compression configuration use [[Compression]] instead.
 */
private[swaydb] sealed trait DecompressorId {
  val id: Int
}

private[swaydb] object DecompressorId {

  private[swaydb] sealed trait LZ4DecompressorId extends DecompressorId

  private[swaydb] case object LZ4FastestInstance {
    private[swaydb] case object FastDecompressor extends LZ4DecompressorId {
      override val id: Int = 0
    }
    private[swaydb] case object SafeDecompressor extends LZ4DecompressorId {
      override val id: Int = 1
    }
  }

  private[swaydb] case object LZ4FastestJavaInstance {
    private[swaydb] case object FastDecompressor extends LZ4DecompressorId {
      override val id: Int = 2
    }
    private[swaydb] case object SafeDecompressor extends LZ4DecompressorId {
      override val id: Int = 3
    }
  }

  private[swaydb] case object LZ4NativeInstance {
    private[swaydb] case object FastDecompressor extends LZ4DecompressorId {
      override val id: Int = 4
    }
    private[swaydb] case object SafeDecompressor extends LZ4DecompressorId {
      override val id: Int = 5
    }
  }

  private[swaydb] case object LZ4SafeInstance {
    private[swaydb] case object FastDecompressor extends LZ4DecompressorId {
      override val id: Int = 6
    }
    private[swaydb] case object SafeDecompressor extends LZ4DecompressorId {
      override val id: Int = 7
    }
  }

  private[swaydb] case object LZ4UnsafeInstance {
    private[swaydb] case object FastDecompressor extends LZ4DecompressorId {
      override val id: Int = 8
    }
    private[swaydb] case object SafeDecompressor extends LZ4DecompressorId {
      override val id: Int = 9
    }
  }

  private[swaydb] case object Snappy {
    private[swaydb] case object Default extends DecompressorId {
      override val id: Int = 10
    }
  }
  private[swaydb] case object UnCompressedGroup extends DecompressorId {
    override val id: Int = 11
  }

  private[swaydb] def lz4Decompressors(): Map[Int, LZ4DecompressorId] =
    SealedList.list[LZ4DecompressorId] map {
      compressionType =>
        compressionType.id -> compressionType
    } toMap

  private[swaydb] def otherDecompressors(): Map[Int, DecompressorId] =
    SealedList.list[DecompressorId] map {
      compressionType =>
        compressionType.id -> compressionType
    } toMap

  private[swaydb] val decompressors: Map[Int, DecompressorId] =
    lz4Decompressors() ++ otherDecompressors()

  private[swaydb] def apply(id: Int): Option[DecompressorId] =
    decompressors.get(id)

  def randomLZ4(): (Int, LZ4DecompressorId) =
    Random.shuffle(DecompressorId.lz4Decompressors()).head

  def randomLZ4Id(): LZ4DecompressorId =
    Random.shuffle(DecompressorId.lz4Decompressors()).head._2

  def randomLZ4IntId(): Int =
    Random.shuffle(DecompressorId.lz4Decompressors()).head._1

  def random(): (Int, DecompressorId) =
    Random.shuffle(DecompressorId.decompressors).head

  def randomIntId(): Int =
    Random.shuffle(DecompressorId.decompressors).head._1
}
