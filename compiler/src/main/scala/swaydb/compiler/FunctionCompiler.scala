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
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.typesafe.scalalogging.LazyLogging
import swaydb.compiler.data.CompiledFunction
import swaydb.compiler.util.PipeOps._

import scala.annotation.tailrec
import scala.reflect.io
import scala.reflect.io.VirtualFile
import scala.tools.nsc.{Global, Settings}
import scala.util.{Failure, Success, Try}

object FunctionCompiler extends LazyLogging {

  private val outputDir: Path =
    Paths.get(getClass.getClassLoader.getResource("").getPath)
      .getParent
      .resolve("functions")

  if (Files.notExists(outputDir)) Files.createDirectories(outputDir)

  private lazy val initialFunctionId: AtomicLong =
    Files
      .list(outputDir)
      .filter(_.getFileName.toString.startsWith("F"))
      .mapToLong(_.getFileName.toString.split("\\.").head.drop(1).toLong)
      .max()
      .==> {
        max =>
          if (max.isPresent)
            new AtomicLong(max.getAsLong)
          else
            new AtomicLong(0L)
      }

  @volatile private var classLoader =
    CustomClassLoader(outputPath = outputDir)

  private val reloadingClassLoader = new AtomicBoolean(false)

  private val scalaLibrary: Path =
    getClass
      .getClassLoader
      .asInstanceOf[URLClassLoader]
      .getURLs
      .find(_.getFile.contains("scala-library"))
      .map(url => Paths.get(url.toURI))
      .getOrElse {
        throw new Exception("scala-library not found in classpath")
      }

  private val settings: Settings = {
    val settings = new Settings(logger.error(_))
    settings.classpath.tryToSet(List(scalaLibrary.toString))
    settings.outdir.tryToSet(List(outputDir.toString))
    //use this directory for memory databases.
    //    val virtualDir = new io.VirtualDirectory("memory_functions", None)
    //    settings.outputDirs.setSingleOutput(virtualDir)
    settings
  }

  private def refreshClassloader(): Unit =
    classLoader = CustomClassLoader(outputPath = outputDir)

  private def makeFile(src: Array[Byte]): VirtualFile = {
    val singleFile = new io.VirtualFile("Function.scala")
    val output = singleFile.output
    output.write(src)
    output.close()
    singleFile
  }

  private def compile(code: String): Try[Unit] = {
    val reporter = new CompilationReporter(settings)
    val files = makeFile(code.getBytes())
    val global = Global(settings, reporter)
    val run = new global.Run()
    run.compileFiles(List(files))
    val errors = reporter.errors.result()
    if (errors.nonEmpty)
      Failure(new Exception(s"${errors.size} error(s) occurred: \n${errors.mkString("\n")}"))
    else
      scala.util.Success()
  }

  private def compileFunction(inputArguments: Seq[(String, String)],
                              returnType: String,
                              functionBody: String): Try[CompiledFunction] = {
    val extendsClass = s"Function${inputArguments.size}" + {
      if (inputArguments.isEmpty)
        s"[$returnType]"
      else
        s"[${inputArguments.map(_._2).mkString(", ")}, $returnType]"
    }

    val functionArguments = inputArguments.map {
      case (argName, argType) =>
        argName + ": " + argType
    }.mkString(", ")

    val className = "F" + initialFunctionId.incrementAndGet()
    val functionClass =
      s"""class $className extends $extendsClass {
         |	  override def apply($functionArguments): $returnType = {
         |       $functionBody
         |    }
         |}
       """.stripMargin

    compile(functionClass) map {
      _ =>
        CompiledFunction(
          inputs = inputArguments,
          returnType = returnType,
          className = className
        )
    }
  }

  private def getInputs(params: Seq[scala.meta.Term.Param]): Try[Seq[(String, String)]] =
    params.find(_.decltpe.isEmpty) match {
      case Some(value) =>
        Failure(new Exception(s"Missing type for function variable: ${value.name.toString()}"))
      case None =>
        Try {
          params map {
            param =>
              (param.name.toString(), param.decltpe.get.toString())
          }
        }
    }

  @tailrec
  final def compileFunction(function: String,
                            inputTypes: Option[Seq[String]],
                            outputType: String): Try[CompiledFunction] = {
    import scala.meta._
    if (function.isEmpty)
      Failure(new Exception("Empty function"))
    else if (outputType.isEmpty)
      Failure(new Exception("Output type is required"))
    else
      function.parse[Stat].toEither match {
        case Right(value) =>
          value match {
            case Term.Block(functionBlock) =>
              compileFunction(functionBlock.head.syntax, inputTypes, outputType)

            case q"_ $operation $body" =>
              val function = s"input => input ${operation.syntax} ${body.syntax}"
              compileFunction(function, inputTypes, outputType)

            case q"$body $operation _" =>
              val function = s"input => ${body.syntax} ${operation.syntax} input"
              compileFunction(function, inputTypes, outputType)

            case q"(..$inputs) => $body" =>
              inputTypes.map(inputTypes => Success(inputs.map(_.name.toString()).zip(inputTypes)))
                .getOrElse(getInputs(inputs))
                .flatMap {
                  inputTypes =>
                    compileFunction(
                      inputArguments = inputTypes,
                      returnType = outputType,
                      functionBody = body.syntax
                    )
                }

            case _ =>
              Failure(new Exception(s"Invalid function syntax: '$function'"))
          }
        case Left(value) =>
          Failure(value.details)
      }
  }

  def getFunction0[O](clazzName: String): Try[Option[() => O]] =
    newInstance[() => O](clazzName)

  def getFunction1[I, O](clazzName: String): Try[Option[I => O]] =
    newInstance[I => O](clazzName)

  def getFunction2[I1, I2, O](clazzName: String): Try[Option[(I1, I2) => O]] =
    newInstance[(I1, I2) => O](clazzName)

  def getFunction3[I1, I2, I3, O](clazzName: String): Try[Option[(I1, I2, I3) => O]] =
    newInstance[(I1, I2, I3) => O](clazzName)

  def newInstance[T](className: String): Try[Option[T]] =
    classLoader.newInstance[T](className)

  def removeClass[T](className: String): Unit =
    classLoader.removeClass[T](className) ==> {
      removed =>
        if (removed && reloadingClassLoader.compareAndSet(false, true))
          try
            refreshClassloader()
          finally
            reloadingClassLoader.set(false)
    }

}
