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

package swaydb.serializers

import boopickle.Default.{Pickle, Unpickle}
import boopickle.{PickleState, Pickler}
import swaydb.slice.Slice

object BooPickle {

  def apply[A](implicit picker: Pickler[A]): Serializer[A] =
    new swaydb.serializers.Serializer[A] {
      override def write(data: A): Slice[Byte] = {
        implicit val state: PickleState = PickleState.pickleStateSpeed
        val buffer = Pickle.intoBytes(data)
        Slice.ofScala(buffer, 0, buffer.limit() - 1)
      }

      override def read(slice: Slice[Byte]): A =
        Unpickle[A].fromBytes(slice.toByteBufferWrap)
    }
}
