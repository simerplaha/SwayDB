package swaydb.core.util

import com.typesafe.scalalogging.LazyLogging
import swaydb.data.Reserve
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}

/**
  * Reserves a range of keys for processing by a single thread.
  *
  * This is used to ensure that multiple threads do not concurrent perform compaction on overlapping keys within
  * the same Level.
  */
object ReserveRange extends LazyLogging {

  case class Range[T](from: Slice[Byte], to: Slice[Byte], reserve: Reserve[T])
  case class State[T](ranges: ListBuffer[Range[T]])

  def create[T](): State[T] =
    State(ListBuffer.empty)

  def get[T](from: Slice[Byte], to: Slice[Byte])(implicit state: State[T],
                                                 ordering: KeyOrder[Slice[Byte]]): Option[T] = {
    import ordering._
    state.synchronized {
      state
        .ranges
        .find(range => from.equiv(range.from) && to.equiv(range.to))
        .flatMap(_.reserve.info)
    }
  }


  def reserveOrGet[T](from: Slice[Byte], to: Slice[Byte], info: T)(implicit state: State[T],
                                                                   ordering: KeyOrder[Slice[Byte]]): Option[T] =
    state.synchronized {
      reserveOrGetRange(
        from = from,
        to = to,
        info = info
      ) match {
        case Left(range) =>
          range.reserve.info

        case Right(_) =>
          None
      }
    }

  def reserveOrListen[T](from: Slice[Byte], to: Slice[Byte], info: T)(implicit state: State[T],
                                                                      ordering: KeyOrder[Slice[Byte]]): Either[Future[Unit], Slice[Byte]] =
    state.synchronized {
      reserveOrGetRange(
        from = from,
        to = to,
        info = info
      ) match {
        case Left(range) =>
          val promise = Promise[Unit]()
          range.reserve.savePromise(promise)
          Left(promise.future)

        case Right(value) =>
          Right(value)
      }
    }

  def free[T](from: Slice[Byte])(implicit state: State[T],
                                 ordering: KeyOrder[Slice[Byte]]): Unit =
    state.synchronized {
      import ordering._
      state
        .ranges
        .find(from equiv _.from)
        .foreach {
          range =>
            state.ranges -= range
            Reserve.setFree(range.reserve)
        }
    }


  private def reserveOrGetRange[T](from: Slice[Byte], to: Slice[Byte], info: T)(implicit state: State[T],
                                                                                ordering: KeyOrder[Slice[Byte]]): Either[Range[T], Slice[Byte]] =
    state.synchronized {
      state
        .ranges
        .find(range => Slice.intersects((from, to), (range.from, range.to)))
        .map(Left(_))
        .getOrElse {
          state.ranges += ReserveRange.Range(from, to, Reserve(info))
          val waitingCount = state.ranges.size
          //Helps debug situations if too many threads and try to compact into the same Segment.
          if (waitingCount >= 100) logger.warn(s"Too many listeners: $waitingCount")
          Right(from)
        }
    }
}
