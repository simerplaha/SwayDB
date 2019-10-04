package swaydb

trait Serial[T[_]] {

  def execute[F](f: => F): T[F]

}
