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

package swaydb

object BagGlass extends Bag.Sync[Glass] {
  @inline override def isSuccess[A](a: Glass[A]): Boolean = true

  @inline override def isFailure[A](a: Glass[A]): Boolean = false

  @inline override def exception[A](a: Glass[A]): Option[Throwable] = None

  @inline override def getOrElse[A, B >: A](a: Glass[A])(b: => B): B = a

  @inline override def getUnsafe[A](a: Glass[A]): A = a

  @inline override def orElse[A, B >: A](a: Glass[A])(b: Glass[B]): B = a

  @inline override def unit: Unit = ()

  @inline override def none[A]: Option[A] = Option.empty[A]

  @inline override def apply[A](a: => A): A = a

  @inline override def foreach[A](a: Glass[A])(f: A => Unit): Unit = f(a)

  @inline override def map[A, B](a: Glass[A])(f: A => B): B = f(a)

  @inline override def transform[A, B](a: Glass[A])(f: A => B): B = f(a)

  @inline override def flatMap[A, B](fa: Glass[A])(f: A => Glass[B]): B = f(fa)

  @inline override def success[A](value: A): A = value

  @inline override def failure[A](exception: Throwable): A = throw exception

  @inline override def fromIO[E: IO.ExceptionHandler, A](a: IO[E, A]): A = a.get

  @inline override def suspend[B](f: => Glass[B]): B = f

  @inline override def safe[B](f: => Glass[B]): B = f

  @inline override def flatten[A](fa: Glass[A]): A = fa

  @inline override def recover[A, B >: A](fa: Glass[A])(pf: PartialFunction[Throwable, B]): B = fa

  @inline override def recoverWith[A, B >: A](fa: Glass[A])(pf: PartialFunction[Throwable, Glass[B]]): B = fa
}
