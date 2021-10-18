/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.data.compression

import swaydb.macros.Sealed

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
  private[swaydb] case object UnCompressed extends DecompressorId {
    override val id: Int = 11
  }

  private[swaydb] def lz4Decompressors(): Map[Int, LZ4DecompressorId] =
    Sealed.list[LZ4DecompressorId] map {
      compressionType =>
        compressionType.id -> compressionType
    } toMap

  private[swaydb] def otherDecompressors(): Map[Int, DecompressorId] =
    Sealed.list[DecompressorId] map {
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
