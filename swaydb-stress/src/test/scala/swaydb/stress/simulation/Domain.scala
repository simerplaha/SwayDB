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

package swaydb.stress.simulation

import swaydb.data.slice.Slice
import swaydb.data.util.ScalaByteOps
import swaydb.serializers.Serializer
import swaydb.data.util.ByteOps._

sealed trait Domain
object Domain {

  case class User(name: String) extends Domain
  case class Product(name: String) extends Domain

  implicit object DomainSerializer extends Serializer[Domain] {
    override def write(data: Domain): Slice[Byte] =
      data match {
        case User(name) =>
          Slice
            .create[Byte](1000)
            .addInt(1)
            .addStringUTF8(name)
            .close()

        case Product(name) =>
          Slice
            .create[Byte](1000)
            .addInt(2)
            .addStringUTF8(name)
            .close()
      }

    override def read(data: Slice[Byte]): Domain = {
      val reader = data.createReader()
      val dataId = reader.readInt()

      val result =
        if (dataId == 1) {
          val userName = reader.readRemainingAsString()
          User(userName)
        } else {
          val productName = reader.readRemainingAsString()
          Product(productName)
        }
      result
    }
  }
}
