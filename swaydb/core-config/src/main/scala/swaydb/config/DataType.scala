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

package swaydb.config

import swaydb.macros.MacroSealed

sealed trait DataType {
  def id: Byte
  def name: String
}

object DataType {

  def map(): DataType = Map //for java
  case object Map extends DataType {
    override def id: Byte = 1
    def name = productPrefix
  }

  def set(): DataType = Set //for java
  case object Set extends DataType {
    override def id: Byte = 2
    def name = productPrefix
  }

  case object SetMap extends DataType {
    override def id: Byte = 3
    def name = productPrefix
  }

  case object Queue extends DataType {
    override def id: Byte = 4
    def name = productPrefix
  }

  case object MultiMap extends DataType {
    override def id: Byte = 5
    def name = productPrefix
  }

  case object Custom extends DataType {
    override def id: Byte = 6
    def name = productPrefix
  }

  def all =
    MacroSealed.array[DataType]

  def apply(id: Byte): Option[DataType] =
    all.find(_.id == id)
}
