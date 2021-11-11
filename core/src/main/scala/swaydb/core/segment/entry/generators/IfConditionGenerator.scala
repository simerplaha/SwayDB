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

package swaydb.core.segment.entry.generators

import swaydb.core.segment.entry.id._
import swaydb.core.segment.entry.reader.base.BaseEntryReader
import swaydb.effect.Effect

import java.io.PrintWriter
import java.nio.file.Paths
import scala.jdk.CollectionConverters._

/**
 * Generates if conditions for all readers.
 */
object IfConditionGenerator extends App {
  implicit class Implicits(entryId: BaseEntryId) {
    def name =
      entryId.getClass.getName.dropRight(1).replaceAll("swaydb.core.segment.entry.id.", "").replaceAll("\\$", ".")
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
    val allLines = Effect.readAllLines(targetIdClass).asScala
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
