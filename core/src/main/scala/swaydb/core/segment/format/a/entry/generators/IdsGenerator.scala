///*
// * Copyright (C) 2018 Simer Plaha (@simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.core.segment.format.a.entry.generators
//
//import java.io.PrintWriter
//import java.nio.file.Paths
//import swaydb.core.io.file.IO
//import scala.io.Source
//import swaydb.core.segment.format.a.entry.id.EntryId.EntryFormat
//import swaydb.core.segment.format.a.entry.id._
//
///**
//  * Generates if Ids for all key-value types.
//  *
//  * Put entries with No meta block have the smallest id number and require only 1 byte for the id.
//  * All other entries requires 2 bytes maximum for the id.
//  */
//object IdsGenerator extends App {
//
//  def write(entry: String,
//            startId: Int): Int = {
//    val fullKeyId = s"${entry}EntryId"
//    val targetIdClass = IO.createFileIfAbsent(Paths.get(s"${System.getProperty("user.dir")}/core/src/main/scala/swaydb/core/segment/format/a/entry/id/$fullKeyId.scala")).get
//    val writer = new PrintWriter(targetIdClass.toFile)
//    writer.write("")
//
//    val templateClass = classOf[TemplateEntryId].getSimpleName
//
//    val maxId =
//      Source
//        .fromFile(s"${System.getProperty("user.dir")}/core/src/main/scala/swaydb/core/segment/format/a/entry/id/$templateClass.scala")
//        .getLines
//        .foldLeft(startId) {
//          case (id, line) =>
//            val newLine =
//              if (!line.contains("remove this")) {
//                val newLine =
//                  line
//                    .replace(templateClass, fullKeyId)
//                    .replace(s"-1", s"$id")
//                    .replace(s"-2", s"$startId")
//                    .replace(s"-3", s"${id - 1}")
//                writer.append("\n" + newLine)
//                newLine
//              } else {
//                ""
//              }
//
//            if (line.contains("package")) {
//              writer.append("\n")
//              writer.append(
//                """
//                  |/** ******************************************
//                  |  * ************ GENERATED CLASS *************
//                  |  * ******************************************/""".stripMargin)
//            }
//
//            if (newLine.contains(s"$fullKeyId($id)"))
//              id + 1
//            else
//              id
//        }
//    writer.close()
//    maxId
//  }
//
//  /**
//    * Entries and their start ids
//    */
//  def idsMaps =
//    Seq(
//      //assign ids with a max of 2 bytes.
//      ("Put", Some(0)), //put are always key and are the most occurring use 1 bytes for put.
//      ("Group", None), //group are always key and are the least occurring use higher id number for 1 or 2 bytes get used.
//      //only use < 3 bytes for above i.e id < 16384.
//      //left ids for until 16384 for backward compatibility
//      //all other entries are not permanently persistent. Give them ids that require 3 bytes
//      ("Range", Some(16384)),
//      ("Remove", None),
//      ("Update", None),
//      ("Function", None),
//      ("PendingApply", None)
//    )
//
//  def maxId =
//    idsMaps.foldLeft(0) {
//      case (previousId, (entry, startId)) =>
//        val idToStartUsing = startId.getOrElse(previousId)
//        if (previousId > idToStartUsing) {
//          throw new IllegalStateException(s"$idToStartUsing is already used. Used up till $previousId.")
//        } //STOP! ID already used.
//        else {
//          println(s"Generating $entry entries. Start id: $idToStartUsing")
//          val usedId = write(entry, idToStartUsing)
//          println(s"Next id: $usedId")
//          usedId
//        }
//    }
//
//  println("\nMax ID: " + maxId)
//}
