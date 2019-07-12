package swaydb.data.config

sealed trait BlockIO
object BlockIO {
  case class ConcurrentIO(cacheOnAccess: Boolean) extends BlockIO
  case class SynchronisedIO(cacheOnAccess: Boolean) extends BlockIO
  case class ReservedIO(cacheOnAccess: Boolean) extends BlockIO
}
