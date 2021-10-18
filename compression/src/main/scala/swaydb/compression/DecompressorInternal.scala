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

package swaydb.compression

import net.jpountz.lz4.{LZ4Factory, LZ4FastDecompressor, LZ4SafeDecompressor}
import org.xerial.snappy
import swaydb.data.compression.{DecompressorId, LZ4Decompressor, LZ4Instance}
import swaydb.data.slice.Slice

/**
 * Internal types that have 1 to 1 mapping with the more configurable swaydb.Decompressor types.
 */
private[swaydb] sealed trait DecompressorInternal {

  val id: Int

  def decompress(slice: Slice[Byte],
                 decompressLength: Int): Slice[Byte]
}

private[swaydb] object DecompressorInternal {

  private[swaydb] sealed trait LZ4 extends DecompressorInternal

  def apply(id: Int): DecompressorInternal =
    DecompressorId(id) match {
      case Some(id) =>
        apply(id)

      case None =>
        throw swaydb.Exception.InvalidDataId(id)
    }

  def apply(instance: LZ4Instance,
            decompressor: LZ4Decompressor): DecompressorInternal.LZ4 =
    DecompressorInternal(
      id =
        decompressorId(
          lz4Instance = instance,
          lZ4Decompressor = decompressor
        )
    )

  private def decompressorId(lz4Instance: LZ4Instance,
                             lZ4Decompressor: LZ4Decompressor): DecompressorId.LZ4DecompressorId =
    (lz4Instance, lZ4Decompressor) match {
      //@formatter:off
      case (LZ4Instance.Fastest, LZ4Decompressor.Fast) =>     DecompressorId.LZ4FastestInstance.FastDecompressor
      case (LZ4Instance.Fastest, LZ4Decompressor.Safe) =>     DecompressorId.LZ4FastestInstance.SafeDecompressor
      case (LZ4Instance.FastestJava, LZ4Decompressor.Fast) => DecompressorId.LZ4FastestJavaInstance.FastDecompressor
      case (LZ4Instance.FastestJava, LZ4Decompressor.Safe) => DecompressorId.LZ4FastestJavaInstance.SafeDecompressor
      case (LZ4Instance.Native, LZ4Decompressor.Fast) =>      DecompressorId.LZ4NativeInstance.FastDecompressor
      case (LZ4Instance.Native, LZ4Decompressor.Safe) =>      DecompressorId.LZ4NativeInstance.SafeDecompressor
      case (LZ4Instance.Safe, LZ4Decompressor.Fast) =>        DecompressorId.LZ4SafeInstance.FastDecompressor
      case (LZ4Instance.Safe, LZ4Decompressor.Safe) =>        DecompressorId.LZ4SafeInstance.SafeDecompressor
      case (LZ4Instance.Unsafe, LZ4Decompressor.Fast) =>      DecompressorId.LZ4UnsafeInstance.FastDecompressor
      case (LZ4Instance.Unsafe, LZ4Decompressor.Safe) =>      DecompressorId.LZ4UnsafeInstance.SafeDecompressor
      //@formatter:on
    }

  def apply(id: DecompressorId): DecompressorInternal =
    id match {
      //@formatter:off
      case DecompressorId.Snappy.Default =>         Snappy
      case DecompressorId.UnCompressed =>           UnCompressed
      case id: DecompressorId.LZ4DecompressorId =>  DecompressorInternal(id)
      //@formatter:on
    }

  def apply(id: DecompressorId.LZ4DecompressorId): DecompressorInternal.LZ4 =
    id match {
      //@formatter:off
      case DecompressorId.LZ4FastestInstance.FastDecompressor =>      LZ4Fast(id.id, LZ4Factory.fastestInstance().fastDecompressor())
      case DecompressorId.LZ4FastestInstance.SafeDecompressor =>      LZ4Safe(id.id, LZ4Factory.fastestInstance().safeDecompressor())
      case DecompressorId.LZ4FastestJavaInstance.FastDecompressor =>  LZ4Fast(id.id, LZ4Factory.fastestJavaInstance().fastDecompressor())
      case DecompressorId.LZ4FastestJavaInstance.SafeDecompressor =>  LZ4Safe(id.id, LZ4Factory.fastestJavaInstance().safeDecompressor())
      case DecompressorId.LZ4NativeInstance.FastDecompressor =>       LZ4Fast(id.id, LZ4Factory.nativeInstance().fastDecompressor())
      case DecompressorId.LZ4NativeInstance.SafeDecompressor =>       LZ4Safe(id.id, LZ4Factory.nativeInstance().safeDecompressor())
      case DecompressorId.LZ4SafeInstance.FastDecompressor =>         LZ4Fast(id.id, LZ4Factory.safeInstance().fastDecompressor())
      case DecompressorId.LZ4SafeInstance.SafeDecompressor =>         LZ4Safe(id.id, LZ4Factory.safeInstance().safeDecompressor())
      case DecompressorId.LZ4UnsafeInstance.FastDecompressor =>       LZ4Fast(id.id, LZ4Factory.unsafeInstance().fastDecompressor())
      case DecompressorId.LZ4UnsafeInstance.SafeDecompressor =>       LZ4Safe(id.id, LZ4Factory.unsafeInstance().safeDecompressor())
      //@formatter:on
    }

  private[swaydb] case class LZ4Fast(id: Int,
                                     decompressor: LZ4FastDecompressor) extends DecompressorInternal.LZ4 {

    override def decompress(slice: Slice[Byte],
                            decompressLength: Int): Slice[Byte] =
      Slice(decompressor.decompress(slice.toArray, decompressLength))
  }

  private[swaydb] case class LZ4Safe(id: Int,
                                     decompressor: LZ4SafeDecompressor) extends DecompressorInternal.LZ4 {

    override def decompress(slice: Slice[Byte],
                            decompressLength: Int): Slice[Byte] =
      Slice(decompressor.decompress(slice.toArray, decompressLength))
  }

  private[swaydb] case object UnCompressed extends DecompressorInternal {

    override val id: Int = DecompressorId.UnCompressed.id

    override def decompress(slice: Slice[Byte],
                            decompressLength: Int): Slice[Byte] =
      slice
  }

  private[swaydb] case object Snappy extends DecompressorInternal {

    override val id: Int = DecompressorId.Snappy.Default.id

    override def decompress(slice: Slice[Byte],
                            decompressLength: Int): Slice[Byte] =
      Slice(snappy.Snappy.uncompress(slice.toArray))
  }
}
