package swaydb.core.segment.block.sortedindex

import swaydb.data.slice.Slice

/**
 * IndexEntries that are used to create secondary indexes - binarySearchIndex & hashIndex
 */
private[segment] class SortedIndexBlockSecondaryIndexEntry(var indexOffset: Int, //mutable because if the bytes are normalised then this is adjust during close.
                                                           val mergedKey: Slice[Byte],
                                                           val comparableKey: Slice[Byte],
                                                           val keyType: Byte)
