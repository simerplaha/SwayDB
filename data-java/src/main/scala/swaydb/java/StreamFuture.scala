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

package swaydb.java

import java.util
import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.{BiFunction, Consumer, Predicate}

import swaydb.java.data.util.Java._

import scala.jdk.CollectionConverters._
import scala.compat.java8.FunctionConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext

class StreamFuture[A](val asScala: swaydb.Stream[A, scala.concurrent.Future])(implicit val ec: ExecutionContext) {

  def forEach(consumer: Consumer[A]): StreamFuture[Unit] =
    new StreamFuture[Unit](asScala.foreach(consumer.asScala))

  def map[B](function: JavaFunction[A, B]): StreamFuture[B] =
    Stream.fromScala(asScala.map(function.asScala))

  def flatMap[B](function: JavaFunction[A, StreamFuture[B]]): StreamFuture[B] =
    Stream.fromScala(asScala.flatMap(function.asScala(_).asScala))

  def drop(count: Int): StreamFuture[A] =
    Stream.fromScala(asScala.drop(count))

  def dropWhile(predicate: Predicate[A]): StreamFuture[A] =
    Stream.fromScala(asScala.dropWhile(predicate.asScala))

  def take(count: Int): StreamFuture[A] =
    Stream.fromScala(asScala.take(count))

  def takeWhile(predicate: Predicate[A]): StreamFuture[A] =
    Stream.fromScala(asScala.takeWhile(predicate.asScala))

  def filter(predicate: Predicate[A]): StreamFuture[A] =
    Stream.fromScala(asScala.filter(predicate.asScala))

  def filterNot(predicate: Predicate[A]): StreamFuture[A] =
    Stream.fromScala(asScala.filterNot(predicate.asScala))

  def lastOption: CompletionStage[Optional[A]] =
    asScala.lastOption.transform(_.asJava, ex => ex).toJava

  def headOption: CompletionStage[Optional[A]] =
    asScala.headOption.map(_.asJava).toJava

  def foldLeft[B](initial: B, function: BiFunction[B, A, B]): CompletionStage[B] =
    asScala.foldLeft(initial)(function.asScala).toJava

  def count(predicate: Predicate[A]): CompletionStage[Int] =
    asScala.count(predicate.test).toJava

  def size: CompletionStage[Int] =
    asScala.size.toJava

  def materialize: CompletionStage[util.List[A]] =
    asScala.materialize.map(_.asJava).toJava
}
