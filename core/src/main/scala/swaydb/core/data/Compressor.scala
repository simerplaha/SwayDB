/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.data

import com.typesafe.scalalogging.LazyLogging
import swaydb.compression.CompressionInternal
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.slice.Slice

private[core] object Compressor extends LazyLogging {

  case class Result(compressedBytes: Slice[Byte], compressionUsed: CompressionInternal)

  def compress(bytes: Slice[Byte],
               compressions: Seq[CompressionInternal],
               keyValueCount: Int): IO[Option[Result]] =
    compressions.untilSome(_.compressor.compress(bytes)) flatMap {
      case None =>
        logger.warn(s"Unable to apply valid compressor for keyBytes: ${bytes.size}. Ignoring key & value compression for $keyValueCount key-values.")
        IO.none

      case Some((compressedBytes, compression)) =>
        logger.debug(s"Keys successfully compressed with Compression: ${compression.getClass.getSimpleName}. ${bytes.size}.bytes compressed to ${compressedBytes.size}.bytes")
        IO.Success(
          Some(
            Result(
              compressedBytes = compressedBytes,
              compressionUsed = compression
            )
          )
        )
    }

  case class ValueCompressionResult(compressedValues: Slice[Byte],
                                    valuesCompression: CompressionInternal)

  case class GroupCompressionResult(compressedIndex: Slice[Byte],
                                    indexCompression: CompressionInternal,
                                    valuesCompressionResult: Option[ValueCompressionResult])
  def compress(indexBytes: Slice[Byte],
               indexCompressions: Seq[CompressionInternal],
               valueBytes: Option[Slice[Byte]],
               valueCompressions: Seq[CompressionInternal],
               keyValueCount: Int): IO[Option[GroupCompressionResult]] =
    indexCompressions.untilSome(_.compressor.compress(indexBytes)) flatMap {
      case None =>
        logger.warn(s"Unable to apply valid compressor for keyBytes: ${indexBytes.size}. Ignoring key & value compression for $keyValueCount key-values.")
        IO.none

      case Some((compressedKeys, keyCompression)) =>
        logger.debug(s"Keys successfully compressed with Compression: ${keyCompression.getClass.getSimpleName}. ${indexBytes.size}.bytes compressed to ${compressedKeys.size}.bytes")
        valueBytes match {
          case Some(valueBytes) if valueBytes.nonEmpty =>
            valueCompressions.untilSome(_.compressor.compress(valueBytes)) flatMap { //if values exists do compressed.
              case None => //if unable to compress values from all the input compression configurations, return None so that compression continues on larger key-value bytes.
                logger.warn(s"Unable to apply valid compressor for valueBytes of ${valueBytes.size}.bytes. Ignoring value compression for $keyValueCount key-values.")
                IO.none //break out because values were not compressed.

              case Some((compressedValueBytes, valueCompression)) =>
                logger.debug(s"Values successfully compressed with Compression: ${valueCompression.getClass.getSimpleName}. ${valueBytes.size}.bytes compressed to ${compressedValueBytes.size}.bytes")
                IO.Success(
                  Some(
                    GroupCompressionResult(
                      compressedIndex = compressedKeys,
                      indexCompression = keyCompression,
                      valuesCompressionResult =
                        Some(
                          ValueCompressionResult(
                            compressedValues = compressedValueBytes,
                            valuesCompression = valueCompression
                          )
                        )
                    )
                  )
                )
            }

          case None | Some(_) =>
            logger.debug(s"No values in ${indexBytes.size}: key-values. Ignoring value compression for $keyValueCount key-values.")
            IO.Success(
              Some(
                GroupCompressionResult(
                  compressedIndex = compressedKeys,
                  indexCompression = keyCompression,
                  valuesCompressionResult = None
                )
              )
            )
        }
    }
}
