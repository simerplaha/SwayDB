package swaydb.core.file.sweeper

import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.ActorConfig
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper

object FileSweeperTestKit {

  def createFileSweeper(): FileSweeper.On =
    FileSweeper(1000, ActorConfig.Basic("Basic test 3", DefaultExecutionContext.sweeperEC))

  def createBufferCleaner(): ByteBufferSweeperActor =
    ByteBufferSweeper()(DefaultExecutionContext.sweeperEC)

}
