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

package swaydb.stress.simulation

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
            .of[Byte](1000)
            .addInt(1)
            .addStringUTF8(name)
            .close()

        case Product(name) =>
          Slice
            .of[Byte](1000)
            .addInt(2)
            .addStringUTF8(name)
            .close()
      }

    override def read(slice: Slice[Byte]): Domain = {
      val reader = slice.createReader()
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
