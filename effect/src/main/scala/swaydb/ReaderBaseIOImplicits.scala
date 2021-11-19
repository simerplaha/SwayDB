package swaydb

import swaydb.slice.ReaderBase

import scala.annotation.tailrec

object ReaderBaseIOImplicits {

  implicit class ReaderIOImplicits[B](reader: ReaderBase[B]) {

    @tailrec
    @inline final def foldLeftIO[E: IO.ExceptionHandler, R](result: R)(f: (R, ReaderBase[B]) => IO[E, R]): IO[E, R] =
      IO(reader.hasMore) match {
        case IO.Left(error) =>
          IO.Left(error)

        case IO.Right(yes) if yes =>
          f(result, reader) match {
            case IO.Right(newResult) =>
              foldLeftIO(newResult)(f)

            case IO.Left(error) =>
              IO.Left(error)
          }

        case _ =>
          IO.Right(result)
      }
  }
}
