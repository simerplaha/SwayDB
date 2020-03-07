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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.entry.generators

import java.io.PrintWriter
import java.nio.file.{Files, Paths}

import swaydb.core.segment.format.a.entry.id._
import swaydb.core.segment.format.a.entry.reader.base.BaseEntryReader

import scala.jdk.CollectionConverters._

/**
 * Generates if conditions for all readers.
 */
object IfConditionGenerator extends App {
  implicit class Implicits(entryId: BaseEntryId) {
    def name =
      entryId.getClass.getName.dropRight(1).replaceAll("swaydb.core.segment.format.a.entry.id.", "").replaceAll("\\$", ".")
  }

  val failed = "throw swaydb.Exception.InvalidKeyValueId(baseId)"

  def generateBinarySearchConditions(ids: List[BaseEntryId]): String = {
    if (ids.size == 1) {
      val typedId = ids.head

      val targetFunction =
        s"reader(${typedId.name})"

      val ifCondition = s"if (baseId == ${typedId.baseId}) \n$targetFunction"

      s"$ifCondition \nelse \n$failed"
    } else if (ids.size == 2) {
      val typedId1 = ids.head
      val typedId2 = ids.last

      val targetFunction1 =
        s"reader(${typedId1.name})"

      val targetFunction2 =
        s"reader(${typedId2.name})"

      val ifCondition = s"if (baseId == ${typedId1.baseId}) \n$targetFunction1"
      val elseIfCondition = s"else if (baseId == ${typedId2.baseId}) \n$targetFunction2"

      s"$ifCondition \n$elseIfCondition \nelse \n$failed"
    } else {
      val mid = ids(ids.size / 2)

      //      println(ids.map(_.id).mkString(", "))
      //      println("Mid:" + mid.id)

      s"if(baseId == ${mid.baseId})" + {
        s"\nreader(${mid.name})"
      } + {
        s"\nelse if(baseId < ${mid.baseId})\n" +
          generateBinarySearchConditions(ids.takeWhile(_.baseId < mid.baseId))
      } + {
        s"\nelse if(baseId > ${mid.baseId})\n" +
          generateBinarySearchConditions(ids.dropWhile(_.baseId <= mid.baseId))
      } + {
        s"\nelse \n$failed"
      }
    }
  }

  def write(fileNumber: Int, ids: List[BaseEntryId], uncompressedOnly: Boolean): Unit = {
    val conditions = generateBinarySearchConditions(ids)
    val baseEntryReaderClass = classOf[BaseEntryReader].getSimpleName

    val targetIdClass =
      if (uncompressedOnly)
        Paths.get(s"${System.getProperty("user.dir")}/core/src/main/scala/swaydb/core/segment/format/a/entry/reader/base/${baseEntryReaderClass}Uncompressed.scala")
      else
        Paths.get(s"${System.getProperty("user.dir")}/core/src/main/scala/swaydb/core/segment/format/a/entry/reader/base/$baseEntryReaderClass$fileNumber.scala")
    val allLines = Files.readAllLines(targetIdClass).asScala
    val writer = new PrintWriter(targetIdClass.toFile)

    val conditionStartIndex = allLines.zipWithIndex.find { case (line, index) => line.contains("//GENERATED") }.get._2
    val newLines =
      allLines.take(conditionStartIndex) ++
        Seq("\t//GENERATED CONDITIONS") ++
        Seq(
          conditions,
          s"\n val minID = ${ids.head.baseId}",
          s"val maxID = ${ids.last.baseId}"
        ) ++
        Seq("}")

    writer.write(newLines.mkString("\n"))
    writer.close()
  }

  def keyIdsGrouped: Iterator[(Int, List[BaseEntryIdFormatA])] = {
    val ids = BaseEntryIdFormatA.baseIds
    ids.grouped(ids.size / 4).zipWithIndex map {
      case (entries, index) =>
        index + 1 -> entries
    }
  }

  keyIdsGrouped foreach {
    case (fileNumber, ids) =>
      write(fileNumber, ids, false)
  }

  def isUncompressedId(id: BaseEntryIdFormatA) = {
    val tokens = id.getClass.getName.split("\\$").filter(_.contains("Compressed"))
    if (tokens.length == 1) //values can be fullyCompressed but still be non-prefixCompressed when only the value bytes are removed but valueOffset and valueLength of previous are copied over.
      tokens.contains("ValueFullyCompressed")
    else
      tokens.length == 0
  }

  def uncompressedIds =
    BaseEntryIdFormatA
      .baseIds
      .filter(isUncompressedId)
      .sortBy(_.baseId)

  write(
    fileNumber = -1,
    ids = uncompressedIds,
    uncompressedOnly = true
  )
}
