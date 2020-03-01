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
 */

package swaydb.generator

object BuilderGenerator extends App {

  val string =
    """
      |private var mapSize: Int = 4.mb,
      |                         private var minSegmentSize: Int = 2.mb,
      |                         private var maxKeyValuesPerSegment: Int = Int.MaxValue,
      |                         private var deleteSegmentsEventually: Boolean = true,
      |                         private var fileCache: FileCache.Enable = DefaultConfigs.fileCache(),
      |                         private var threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
      |                         private var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
      |                         private var levelZeroThrottle: JavaFunction[LevelZeroMeter, FiniteDuration] = (DefaultConfigs.levelZeroThrottle _).asJava,
      |                         private var lastLevelThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.lastLevelThrottle _).asJava,
      |                         private var byteComparator: KeyComparator[ByteSlice] = null,
      |                         private var typedComparator: KeyComparator[K] = null
      |""".stripMargin

  val regex = """var (.+):\s(.+?)\s=""".r

  val functions =
    regex
      .findAllMatchIn(string)
      .foldLeft("") {
        case (string, matched) =>
          val varName = matched.group(1)
          val functionName = "set" + varName.head.toUpper + varName.drop(1).mkString
          val varType = matched.group(2)
          val withFunction =
            s"""def $functionName($varName: $varType) = {
               |   this.$varName = $varName
               |   this
               |}
               |""".stripMargin

          s"""$string
             |$withFunction""".stripMargin

      }

  println(functions)

}
