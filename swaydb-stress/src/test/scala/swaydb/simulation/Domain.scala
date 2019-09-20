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

package swaydb.simulation

import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

sealed trait Domain
object Domain {
  case class User(name: String) extends Domain
  case class Product(name: String) extends Domain

  implicit object DomainSerializer extends Serializer[Domain] {
    override def write(data: Domain): Slice[Byte] =
      data match {
        case User(name) =>
          Slice
            .create(1000)
            .addInt(1)
            .addString(name)
            .close()

        case Product(name) =>
          Slice
            .create(1000)
            .addInt(2)
            .addString(name)
            .close()
      }

    override def read(data: Slice[Byte]): Domain = {
      val reader = data.createReaderUnsafe()
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
