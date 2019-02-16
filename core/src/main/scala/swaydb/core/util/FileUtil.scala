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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.util

import java.nio.file.{Files, Path}
import com.typesafe.scalalogging.LazyLogging
import scala.collection.JavaConverters._
import swaydb.data.io.IO
import PipeOps._
import swaydb.core.io.file.IOEffect

case class NotAnIntFile(path: Path) extends Throwable

case class UnknownExtension(path: Path) extends Throwable

private[core] object FileUtil extends LazyLogging {

  implicit class FileUtilImplicits(path: Path) {
    def fileId =
      FileUtil.fileId(path)

    def incrementFileId =
      FileUtil.incrementFileId(path)

    def incrementFolderId =
      FileUtil.incrementFolderId(path)

    def folderId =
      FileUtil.folderId(path)

    def files(extension: Extension) =
      FileUtil.files(path, extension)

    def folders =
      FileUtil.folders(path)

    def exists =
      IOEffect.exists(path)

  }

  implicit class FileIdImplicits(id: Long) {
    def toLogFileId =
      s"$id.${Extension.Log}"

    def toFolderId =
      s"$id"

    def toSegmentFileId =
      s"$id.${Extension.Seg}"
  }

  def incrementFileId(path: Path): IO[Path] =
    fileId(path) map {
      case (id, ext) =>
        path.getParent.resolve((id + 1) + "." + ext.toString)
    }

  def incrementFolderId(path: Path): Path =
    folderId(path) ==> {
      currentFolderId =>
        path.getParent.resolve((currentFolderId + 1).toString)
    }

  def folderId(path: Path): Long =
    path.getFileName.toString.toLong

  def fileId(path: Path): IO[(Long, Extension)] = {
    val fileName = path.getFileName.toString
    val extensionIndex = fileName.lastIndexOf(".")
    val extIndex = if (extensionIndex <= 0) fileName.length else extensionIndex

    IO(fileName.substring(0, extIndex).toLong) orElse IO.Failure(NotAnIntFile(path)) flatMap {
      fileId =>
        val ext = fileName.substring(extIndex + 1, fileName.length)
        if (ext == Extension.Log.toString)
          IO.Success(fileId, Extension.Log)
        else if (ext == Extension.Seg.toString)
          IO.Success(fileId, Extension.Seg)
        else {
          logger.error("Unknown extension for file {}", path)
          IO.Failure(UnknownExtension(path))
        }
    }
  }

  def isExtension(path: Path, ext: Extension): Boolean =
    fileId(path).map(_._2 == ext) getOrElse false

  def files(folder: Path,
            extension: Extension): List[Path] =
    IOEffect.stream(folder) {
      _.iterator()
        .asScala
        .filter(isExtension(_, extension))
        .toList
        .sortBy(path => fileId(path).get._1)
    }

  def folders(folder: Path): List[Path] =
    IOEffect.stream(folder) {
      _.iterator()
        .asScala
        .filter(folder => IO(folderId(folder)).isSuccess)
        .toList
        .sortBy(folderId)
    }

  def segmentFilesOnDisk(paths: Seq[Path]): Seq[Path] =
    paths
      .flatMap(_.files(Extension.Seg))
      .sortBy(_.getFileName.fileId.get._1)

}
