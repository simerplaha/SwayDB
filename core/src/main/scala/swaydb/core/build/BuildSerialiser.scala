/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.build

import java.nio.file.Path

import swaydb.core.util.CRC32
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

sealed trait BuildSerialiser {

  def write(build: Build.Info): Slice[Byte]

  def read(bytes: Slice[Byte], file: Path): Build.Info

}

object BuildSerialiser extends BuildSerialiser {
  override def write(buildInfo: Build.Info): Slice[Byte] = {
    val versionBytes = Slice.create[Byte](1 + ByteSizeOf.int * 3)

    versionBytes add Build.formatId

    versionBytes addInt buildInfo.major
    versionBytes addInt buildInfo.minor
    versionBytes addInt buildInfo.revision

    val crc = CRC32.forBytes(versionBytes)

    val slice = Slice.create[Byte](ByteSizeOf.long + versionBytes.size)
    slice addLong crc
    slice addAll versionBytes
  }


  override def read(bytes: Slice[Byte], file: Path): Build.Info = {
    val crc = bytes.readLong()
    val versionBytes = bytes.drop(ByteSizeOf.long)
    val versionBytesCRC = CRC32.forBytes(versionBytes)

    assert(versionBytesCRC == crc, s"$file has invalid CRC. $versionBytesCRC != $crc")

    val versionReader = versionBytes.createReader()
    val formatId = versionReader.get()
    assert(formatId == Build.formatId, s"$file has invalid formatId. $formatId != ${Build.formatId}")

    val major = versionReader.readInt()
    val minor = versionReader.readInt()
    val revision = versionReader.readInt()

    Build.Info(major = major, minor = minor, revision = revision)
  }
}
