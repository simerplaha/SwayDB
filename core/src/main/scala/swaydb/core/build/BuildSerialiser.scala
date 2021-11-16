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

package swaydb.core.build

import swaydb.core.util.CRC32
import swaydb.data.DataType
import swaydb.slice.Slice
import swaydb.utils.ByteSizeOf

import java.nio.file.Path

sealed trait BuildSerialiser {

  def write(build: Build.Info): Slice[Byte]

  def read(bytes: Slice[Byte], file: Path): Build.Info

}

object BuildSerialiser extends BuildSerialiser {
  override def write(buildInfo: Build.Info): Slice[Byte] = {
    val versionBytes = Slice.of[Byte](1 + 1 + ByteSizeOf.int * 3)

    versionBytes add Build.formatId

    versionBytes add buildInfo.dataType.id

    versionBytes addInt buildInfo.version.major
    versionBytes addInt buildInfo.version.minor
    versionBytes addInt buildInfo.version.revision

    val crc = CRC32.forBytes(versionBytes)

    val slice = Slice.of[Byte](ByteSizeOf.long + versionBytes.size)
    slice addLong crc
    slice addAll versionBytes
  }


  override def read(bytes: Slice[Byte], file: Path): Build.Info = {
    val crc = bytes.readLong()
    val versionBytes = bytes.drop(ByteSizeOf.long)
    val versionBytesCRC = CRC32.forBytes(versionBytes)

    assert(versionBytesCRC == crc, s"Invalid CRC. $versionBytesCRC != $crc in file: $file")

    val versionReader = versionBytes.createReader()
    val formatId = versionReader.get()
    assert(formatId == Build.formatId, s"Invalid formatId. $formatId != ${Build.formatId} in file: $file")

    val dataTypeId = versionReader.get()

    DataType(dataTypeId) match {
      case Some(dataType) =>
        val major = versionReader.readInt()
        val minor = versionReader.readInt()
        val revision = versionReader.readInt()
        val version = Build.Version(major = major, minor = minor, revision = revision)
        Build.Info(version, dataType)

      case None =>
        throw new IllegalStateException(s"Invalid data-type id '$dataTypeId' in file: $file.")
    }
  }
}
