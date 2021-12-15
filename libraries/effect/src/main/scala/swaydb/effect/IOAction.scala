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
package swaydb.effect

sealed trait IOAction {
  def isCompressed: Boolean
  def isReadDataOverview: Boolean
  def isReadCompressedData: Boolean
  def isReadUncompressedData: Boolean
}

object IOAction {

  sealed trait ReadDataOverview extends IOAction {
    override def isCompressed: Boolean = false
    override def isReadDataOverview: Boolean = true
    override def isReadCompressedData: Boolean = false
    override def isReadUncompressedData: Boolean = false
  }
  case object ReadDataOverview extends ReadDataOverview

  sealed trait DecompressAction extends IOAction
  case class ReadCompressedData(compressedSize: Int, decompressedSize: Int) extends DecompressAction {
    override def isCompressed: Boolean = true
    override def isReadDataOverview: Boolean = false
    override def isReadCompressedData: Boolean = true
    override def isReadUncompressedData: Boolean = false
  }

  case class ReadUncompressedData(size: Int) extends DecompressAction {
    override def isCompressed: Boolean = false
    override def isReadDataOverview: Boolean = false
    override def isReadCompressedData: Boolean = false
    override def isReadUncompressedData: Boolean = true
  }
}
