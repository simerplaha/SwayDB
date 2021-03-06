/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data.compaction

import swaydb.data.compaction.CompactionConfig.CompactionParallelism

import java.util.concurrent.ExecutorService
import scala.concurrent.ExecutionContext

object CompactionConfig {

  def create(resetCompactionPriorityAtInterval: Int,
             actorExecutionContext: ExecutorService,
             compactionExecutionContext: ExecutorService,
             levelZeroFlattenParallelism: Int,
             levelZeroMergeParallelism: Int,
             multiLevelTaskParallelism: Int,
             levelSegmentAssignmentParallelism: Int,
             groupedSegmentDefragParallelism: Int,
             defragmentedSegmentParallelism: Int,
             pushStrategy: PushStrategy): CompactionConfig =
    CompactionConfig(
      resetCompactionPriorityAtInterval = resetCompactionPriorityAtInterval,
      actorExecutionContext = ExecutionContext.fromExecutorService(actorExecutionContext),
      compactionExecutionContext = ExecutionContext.fromExecutorService(compactionExecutionContext),
      levelZeroFlattenParallelism = levelZeroFlattenParallelism,
      levelZeroMergeParallelism = levelZeroMergeParallelism,
      multiLevelTaskParallelism = multiLevelTaskParallelism,
      levelSegmentAssignmentParallelism = levelSegmentAssignmentParallelism,
      groupedSegmentDefragParallelism = groupedSegmentDefragParallelism,
      defragmentedSegmentParallelism = defragmentedSegmentParallelism,
      pushStrategy = pushStrategy
    )

  def apply(resetCompactionPriorityAtInterval: Int,
            actorExecutionContext: ExecutionContext,
            compactionExecutionContext: ExecutionContext,
            levelZeroFlattenParallelism: Int,
            levelZeroMergeParallelism: Int,
            multiLevelTaskParallelism: Int,
            levelSegmentAssignmentParallelism: Int,
            groupedSegmentDefragParallelism: Int,
            defragmentedSegmentParallelism: Int,
            pushStrategy: PushStrategy): CompactionConfig =
    if (resetCompactionPriorityAtInterval <= 0)
      throw new Exception(s"Invalid resetCompactionPriorityAtInterval $resetCompactionPriorityAtInterval. Should be greater than zero.")
    else if (levelZeroFlattenParallelism <= 0)
      throw new Exception(s"Invalid levelZeroFlattenParallelism $levelZeroFlattenParallelism. Should be greater than zero.")
    else if (levelZeroMergeParallelism <= 0)
      throw new Exception(s"Invalid levelZeroMergeParallelism $levelZeroMergeParallelism. Should be greater than zero.")
    else if (multiLevelTaskParallelism <= 0)
      throw new Exception(s"Invalid multiLevelTaskParallelism $multiLevelTaskParallelism. Should be greater than zero.")
    else if (levelSegmentAssignmentParallelism <= 0)
      throw new Exception(s"Invalid levelSegmentAssignmentParallelism $levelSegmentAssignmentParallelism. Should be greater than zero.")
    else if (groupedSegmentDefragParallelism <= 0)
      throw new Exception(s"Invalid groupedSegmentDefragParallelism $groupedSegmentDefragParallelism. Should be greater than zero.")
    else if (defragmentedSegmentParallelism <= 0)
      throw new Exception(s"Invalid defragmentedSegmentParallelism $defragmentedSegmentParallelism. Should be greater than zero.")
    else
      new CompactionConfig(
        resetCompactionPriorityAtInterval = resetCompactionPriorityAtInterval,
        actorExecutionContext = actorExecutionContext,
        compactionExecutionContext = compactionExecutionContext,
        levelZeroFlattenParallelism = levelZeroFlattenParallelism,
        levelZeroMergeParallelism = levelZeroMergeParallelism,
        multiLevelTaskParallelism = multiLevelTaskParallelism,
        levelSegmentAssignmentParallelism = levelSegmentAssignmentParallelism,
        groupedSegmentDefragParallelism = groupedSegmentDefragParallelism,
        defragmentedSegmentParallelism = defragmentedSegmentParallelism,
        pushStrategy = pushStrategy
      )

  object CompactionParallelism {
    def availableProcessors(): CompactionParallelism =
      new CompactionParallelism {
        val cores = Runtime.getRuntime.availableProcessors()

        override def levelZeroFlattenParallelism: Int =
          cores

        override def levelZeroMergeParallelism: Int =
          cores

        override def levelSegmentAssignmentParallelism: Int =
          cores

        override def groupedSegmentDefragParallelism: Int =
          cores

        override def defragmentedSegmentParallelism: Int =
          cores

        override def multiLevelTaskParallelism: Int =
          cores
      }
  }

  trait CompactionParallelism {
    /**
     * LevelZero can have multiple overlapping log files. This sets
     * the parallelism for each group of overlapping group of key-values.
     *
     * Eg: if there are logs with the following keys
     * Log1 -        10 - 20
     * Log2 -                  30 - 40
     * Log3 - 1         -           40
     *
     * The above when flattened/assigned will create two groups of overlapping key-values.
     * Group1 - 10 - 20
     *        - 1  - 20
     *
     * Group2 - 30 - 40
     *        - 21 - 40
     *
     * This configuration sets the parallelism of each group to execute merge.
     * The parallelism of merge per group is set via [[levelZeroMergeParallelism]].
     */
    def levelZeroFlattenParallelism: Int

    /**
     * Each group (as mentioned in [[levelZeroFlattenParallelism]]) can contain
     * multiple stacks of overlapping key-values. For example if there are 4 sets
     * of overlapping key-values
     *
     * key-values1 -  10 - 20
     * key-values2 -  1  - 20
     * key-values3 -   3-5
     * key-values4    2 - 10
     *
     * This will perform merge by in groups of 2
     *
     * The parallelism will be executed as follows.
     * key-values-1 & key-values-2 ----
     * -                               | ---- merged-key-Values-1-2
     * -                               |                           --------> final-merged-key-values-1-2-3-4
     * -                               | ---- merged-key-values-3-4
     * key-values-3 & key-values-4 ----
     *
     */
    def levelZeroMergeParallelism: Int

    /**
     * Compaction can assign merge tasks to multiple Levels.
     *
     * Eg: if a flattened log file from LevelZero ([[levelZeroFlattenParallelism]])
     * results in multiple groups that can be compacted into lower levels Level1, Level2 & Level3
     * then we can control this multi level concurrency via this configuration.
     */
    def multiLevelTaskParallelism: Int

    /**
     * Compaction can submit merge where there are multiple Segments
     * overlapping the new key-values. This sets the number of Segments
     * to merge concurrently.
     */
    def levelSegmentAssignmentParallelism: Int

    /**
     * Applies to Segments of format [[swaydb.data.config.SegmentFormat.Grouped]]
     * where each Segment store a group of key-values per Segment file.
     *
     * Each group can be defragmented & merged in parallel after the new key-values
     * are assigned to their respective groups.
     */
    def groupedSegmentDefragParallelism: Int

    /**
     * The above [[groupedSegmentDefragParallelism]] can result in multiple groups
     * of new key-values and remote Segment instance which can be grouped to create
     * new [[swaydb.data.config.SegmentFormat.Grouped]] Segments.
     */
    def defragmentedSegmentParallelism: Int

  }
}
/**
 * Configures Compaction strategy.
 *
 * @param executionContext                  [[ExecutionContext]] assigned to compaction.
 * @param resetCompactionPriorityAtInterval Example: if there are 7 Levels then setting this to 2 will
 *                                          run compaction on a maximum of two levels consecutively before
 *                                          re-ordering/re-prioritising/re-computing compaction priority.
 */

case class CompactionConfig private(resetCompactionPriorityAtInterval: Int,
                                    actorExecutionContext: ExecutionContext,
                                    compactionExecutionContext: ExecutionContext,
                                    levelZeroFlattenParallelism: Int,
                                    levelZeroMergeParallelism: Int,
                                    multiLevelTaskParallelism: Int,
                                    levelSegmentAssignmentParallelism: Int,
                                    groupedSegmentDefragParallelism: Int,
                                    defragmentedSegmentParallelism: Int,
                                    pushStrategy: PushStrategy) extends CompactionParallelism
