/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

package swaydb.compiler

import java.net.URLClassLoader
import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentHashMap

import scala.util.{Failure, Success, Try}

case class CustomClassLoader(outputPath: Path) extends URLClassLoader(Array(outputPath.toUri.toURL)) {
  private val cache = new ConcurrentHashMap[String, Class[_]]

  def newInstance[T](className: String): Try[Option[T]] =
    Try {
      loadClass(className).asInstanceOf[Class[T]].newInstance()
    } map {
      clazz =>
        Option(clazz)
    } recoverWith {
      case _: java.lang.ClassNotFoundException =>
        Success(None)
      case exception: Exception =>
        Failure(exception)
    }

  def removeClass[T](name: String): Boolean = {
    val classRemoved = cache.remove(name) != null
    val classDeleted = Files.deleteIfExists(outputPath.resolve(s"$name.class"))
    classRemoved || classDeleted
  }

  //do not call this method directly. Use newInstance instead.
  override def loadClass(name: String): Class[_] =
    cache.get(name) match {
      case null =>
        val clazz = super.loadClass(name)
        cache.put(name, clazz)
        clazz
      case clazz =>
        clazz
    }
}