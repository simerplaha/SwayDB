package swaydb.data.compaction

import scala.concurrent.ExecutionContext

sealed trait CompactionExecutionContext
object CompactionExecutionContext {
  case class Create(executionContext: ExecutionContext) extends CompactionExecutionContext
  case object Shared extends CompactionExecutionContext
}
