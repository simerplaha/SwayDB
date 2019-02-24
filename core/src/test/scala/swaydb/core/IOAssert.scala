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

package swaydb.core

import org.scalatest.Matchers
import swaydb.data.io.IO

object IOAssert extends Matchers {

  implicit class GetIOImplicit[T](getThis: IO[T]) {
    def assertGet: T =
      getThis match {
        case IO.Failure(error) =>
          fail(error.exception)

        case IO.Success(value) =>
          value
      }
  }

  implicit class GetIOAsyncImplicit[T](getThis: IO.Async[T]) {
    def assertGet: T =
      getThis match {
        case IO.Failure(error) =>
          fail(error.exception)

        case IO.Success(value) =>
          value

        case later: IO.Later[T] =>
          later.safeGetBlocking.get
      }
  }

  implicit class GetOptionImplicit[T](getThis: Option[T]) {
    def assertGet: T =
      getThis.get
  }

  implicit class GetIOOptionImplicit[T](tryThis: IO[Option[T]]) {
    def assertGet: T =
      tryThis match {
        case IO.Failure(error) =>
          fail(error.exception)

        case IO.Success(value) =>
          value.assertGet
      }

    def assertGetOpt: Option[T] =
      tryThis match {
        case IO.Failure(error) =>
          fail(s"Failed Error: $error", error.exception)

        case IO.Success(value) =>
          value
      }
  }

  implicit class GetIOAsyncOptionImplicit[T](tryThis: IO.Async[Option[T]]) {
    def assertGet: T =
      tryThis match {
        case IO.Failure(error) =>
          fail(error.exception)
        case IO.Success(value) =>
          value.assertGet
        case later @ IO.Later(value, error) =>
          later.safeGetBlocking.get.assertGet
      }

    def assertGetOpt: Option[T] =
      tryThis match {
        case IO.Failure(error) =>
          fail(error.exception)

        case IO.Success(value) =>
          value

        case later: IO.Later[Option[T]] =>
          later.safeGetBlocking.get
      }
  }
}
