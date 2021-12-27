/*
 * Copyright (c) 27/12/21, 3:26 pm Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package swaydb.effect

import swaydb.utils.Extension

import java.nio.file.Path

object EffectImplicits {

  implicit class PathExtensionImplicits(path: Path) {
    @inline def fileId(): (Long, Extension) =
      Effect.numberFileId(path)

    @inline def incrementFileId(): Path =
      Effect.incrementFileId(path)

    @inline def incrementFolderId(): Path =
      Effect.incrementFolderId(path)

    @inline def folderId(): Long =
      Effect.folderId(path)

    @inline def files(extension: Extension): List[Path] =
      Effect.files(path, extension)

    @inline def folders(): List[Path] =
      Effect.folders(path)

    @inline def exists(): Boolean =
      Effect.exists(path)
  }

  implicit class FileIdImplicits(id: Long) {
    @inline final def toLogFileId: String =
      s"$id.${Extension.Log}"

    @inline final def toFolderId: String =
      s"$id"

    @inline final def toSegmentFileId: String =
      s"$id.${Extension.Seg}"
  }

}
