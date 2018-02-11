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

import java.nio.charset.{Charset, StandardCharsets}

import swaydb.data.util.ByteUtil

import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

private[swaydb] trait Reader { self =>

  def get(): Try[Int]

  def read(size: Int): Try[Slice[Byte]]

  def size: Try[Long]

  def isLoaded: Try[Boolean]

  def hasMore: Try[Boolean]

  def hasAtLeast(size: Long): Try[Boolean]

  def getPosition: Int

  def moveTo(position: Long): Reader

  def skip(skip: Long): Reader =
    moveTo(getPosition + skip)

  def readInt(): Try[Int] =
    ByteUtil.readInt(self)

  def readIntUnsigned(): Try[Int] =
    ByteUtil.readUnsignedInt(self)

  def readIntSigned(): Try[Int] =
    ByteUtil.readSignedInt(self)

  def readLong(): Try[Long] =
    ByteUtil.readLong(self)

  def readLongUnsigned(): Try[Long] =
    ByteUtil.readUnsignedLong(self)

  def readLongSigned(): Try[Long] =
    ByteUtil.readSignedLong(self)

  def readString(charset: Charset = StandardCharsets.UTF_8): Try[String] =
    ByteUtil.readString(self, charset)

  def copy(): Reader

  @tailrec
  final def foldLeftTry[R: ClassTag](result: R)(f: (R, Reader) => Try[R]): Try[R] =
    hasMore match {
      case Failure(exception) =>
        Failure(exception)

      case Success(yes) if yes =>
        f(result, self) match {
          case Success(newResult) =>
            foldLeftTry(newResult)(f)

          case Failure(exception) =>
            Failure(exception)
        }

      case _ =>
        Success(result)
    }
}