/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.collection.unsafe.sort;

import java.util.Comparator;
import java.util.LinkedList;

<<<<<<< HEAD
import org.apache.avro.reflect.Nullable;

import org.apache.spark.TaskContext;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
=======
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
import org.apache.spark.util.collection.Sorter;

/**
 * Sorts records using an AlphaSort-style key-prefix sort. This sort stores pointers to records
 * alongside a user-defined prefix of the record's sorting key. When the underlying sort algorithm
 * compares records, it will first compare the stored key prefixes; if the prefixes are not equal,
 * then we do not need to traverse the record pointers to compare the actual records. Avoiding these
 * random memory accesses improves cache hit rates.
 */
public final class UnsafeInMemorySorter {

  private static final class SortComparator implements Comparator<RecordPointerAndKeyPrefix> {

    private final RecordComparator recordComparator;
    private final PrefixComparator prefixComparator;
    private final TaskMemoryManager memoryManager;

    SortComparator(
        RecordComparator recordComparator,
        PrefixComparator prefixComparator,
        TaskMemoryManager memoryManager) {
      this.recordComparator = recordComparator;
      this.prefixComparator = prefixComparator;
      this.memoryManager = memoryManager;
    }

    @Override
    public int compare(RecordPointerAndKeyPrefix r1, RecordPointerAndKeyPrefix r2) {
      final int prefixComparisonResult = prefixComparator.compare(r1.keyPrefix, r2.keyPrefix);
      int uaoSize = UnsafeAlignedOffset.getUaoSize();
      if (prefixComparisonResult == 0) {
        final Object baseObject1 = memoryManager.getPage(r1.recordPointer);
        // skip length
        final long baseOffset1 = memoryManager.getOffsetInPage(r1.recordPointer) + uaoSize;
        final Object baseObject2 = memoryManager.getPage(r2.recordPointer);
        // skip length
        final long baseOffset2 = memoryManager.getOffsetInPage(r2.recordPointer) + uaoSize;
        return recordComparator.compare(baseObject1, baseOffset1, baseObject2, baseOffset2);
      } else {
        return prefixComparisonResult;
      }
    }
  }

  private final MemoryConsumer consumer;
  private final TaskMemoryManager memoryManager;
<<<<<<< HEAD
  @Nullable
=======
  private final Sorter<RecordPointerAndKeyPrefix, LongArray> sorter;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  private final Comparator<RecordPointerAndKeyPrefix> sortComparator;

  /**
   * If non-null, specifies the radix sort parameters and that radix sort will be used.
   */
  @Nullable
  private final PrefixComparators.RadixSortSupport radixSortSupport;

  /**
   * Within this buffer, position {@code 2 * i} holds a pointer to the record at
   * index {@code i}, while position {@code 2 * i + 1} in the array holds an 8-byte key prefix.
   *
   * Only part of the array will be used to store the pointers, the rest part is preserved as
   * temporary buffer for sorting.
   */
  private LongArray array;

  /**
   * The position in the sort buffer where new records can be inserted.
   */
  private int pos = 0;

<<<<<<< HEAD
  /**
   * If sorting with radix sort, specifies the starting position in the sort buffer where records
   * with non-null prefixes are kept. Positions [0..nullBoundaryPos) will contain null-prefixed
   * records, and positions [nullBoundaryPos..pos) non-null prefixed records. This lets us avoid
   * radix sorting over null values.
   */
  private int nullBoundaryPos = 0;

  /*
   * How many records could be inserted, because part of the array should be left for sorting.
   */
  private int usableCapacity = 0;

  private long initialSize;

  private long totalSortTimeNanos = 0L;

=======
  private long initialSize;

>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  public UnsafeInMemorySorter(
    final MemoryConsumer consumer,
    final TaskMemoryManager memoryManager,
    final RecordComparator recordComparator,
    final PrefixComparator prefixComparator,
<<<<<<< HEAD
    int initialSize,
    boolean canUseRadixSort) {
    this(consumer, memoryManager, recordComparator, prefixComparator,
      consumer.allocateArray(initialSize * 2), canUseRadixSort);
  }

  public UnsafeInMemorySorter(
      final MemoryConsumer consumer,
      final TaskMemoryManager memoryManager,
      final RecordComparator recordComparator,
      final PrefixComparator prefixComparator,
      LongArray array,
      boolean canUseRadixSort) {
    this.consumer = consumer;
    this.memoryManager = memoryManager;
    this.initialSize = array.size();
    if (recordComparator != null) {
      this.sortComparator = new SortComparator(recordComparator, prefixComparator, memoryManager);
      if (canUseRadixSort && prefixComparator instanceof PrefixComparators.RadixSortSupport) {
        this.radixSortSupport = (PrefixComparators.RadixSortSupport)prefixComparator;
      } else {
        this.radixSortSupport = null;
      }
    } else {
      this.sortComparator = null;
      this.radixSortSupport = null;
    }
    this.array = array;
    this.usableCapacity = getUsableCapacity();
  }

  private int getUsableCapacity() {
    // Radix sort requires same amount of used memory as buffer, Tim sort requires
    // half of the used memory as buffer.
    return (int) (array.size() / (radixSortSupport != null ? 2 : 1.5));
=======
    int initialSize) {
    this(consumer, memoryManager, recordComparator, prefixComparator,
      consumer.allocateArray(initialSize * 2));
  }

  public UnsafeInMemorySorter(
    final MemoryConsumer consumer,
      final TaskMemoryManager memoryManager,
      final RecordComparator recordComparator,
      final PrefixComparator prefixComparator,
      LongArray array) {
    this.consumer = consumer;
    this.memoryManager = memoryManager;
    this.initialSize = array.size();
    this.sorter = new Sorter<>(UnsafeSortDataFormat.INSTANCE);
    this.sortComparator = new SortComparator(recordComparator, prefixComparator, memoryManager);
    this.array = array;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }

  /**
   * Free the memory used by pointer array.
   */
  public void free() {
    if (consumer != null) {
      consumer.freeArray(array);
      array = null;
    }
  }

  public void reset() {
    if (consumer != null) {
      consumer.freeArray(array);
<<<<<<< HEAD
      array = consumer.allocateArray(initialSize);
      usableCapacity = getUsableCapacity();
=======
      this.array = consumer.allocateArray(initialSize);
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    }
    pos = 0;
    nullBoundaryPos = 0;
  }

  /**
   * @return the number of records that have been inserted into this sorter.
   */
  public int numRecords() {
    return pos / 2;
  }

<<<<<<< HEAD
  /**
   * @return the total amount of time spent sorting data (in-memory only).
   */
  public long getSortTimeNanos() {
    return totalSortTimeNanos;
  }

  public long getMemoryUsage() {
    return array.size() * 8;
  }

  public boolean hasSpaceForAnotherRecord() {
    return pos + 1 < usableCapacity;
=======
  public long getMemoryUsage() {
    return array.size() * 8L;
  }

  public boolean hasSpaceForAnotherRecord() {
    return pos + 2 <= array.size();
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }

  public void expandPointerArray(LongArray newArray) {
    if (newArray.size() < array.size()) {
      throw new OutOfMemoryError("Not enough memory to grow pointer array");
    }
    Platform.copyMemory(
      array.getBaseObject(),
      array.getBaseOffset(),
      newArray.getBaseObject(),
      newArray.getBaseOffset(),
<<<<<<< HEAD
      pos * 8L);
    consumer.freeArray(array);
    array = newArray;
    usableCapacity = getUsableCapacity();
=======
      array.size() * 8L);
    consumer.freeArray(array);
    array = newArray;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }

  /**
   * Inserts a record to be sorted. Assumes that the record pointer points to a record length
   * stored as a 4-byte integer, followed by the record's bytes.
   *
   * @param recordPointer pointer to a record in a data page, encoded by {@link TaskMemoryManager}.
   * @param keyPrefix a user-defined key prefix
   */
  public void insertRecord(long recordPointer, long keyPrefix, boolean prefixIsNull) {
    if (!hasSpaceForAnotherRecord()) {
      throw new IllegalStateException("There is no space for new record");
<<<<<<< HEAD
    }
    if (prefixIsNull && radixSortSupport != null) {
      // Swap forward a non-null record to make room for this one at the beginning of the array.
      array.set(pos, array.get(nullBoundaryPos));
      pos++;
      array.set(pos, array.get(nullBoundaryPos + 1));
      pos++;
      // Place this record in the vacated position.
      array.set(nullBoundaryPos, recordPointer);
      nullBoundaryPos++;
      array.set(nullBoundaryPos, keyPrefix);
      nullBoundaryPos++;
    } else {
      array.set(pos, recordPointer);
      pos++;
      array.set(pos, keyPrefix);
      pos++;
    }
  }

  public final class SortedIterator extends UnsafeSorterIterator implements Cloneable {

    private final int numRecords;
    private int position;
    private int offset;
=======
    }
    array.set(pos, recordPointer);
    pos++;
    array.set(pos, keyPrefix);
    pos++;
  }

  public final class SortedIterator extends UnsafeSorterIterator {

    private final int numRecords;
    private int position;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    private Object baseObject;
    private long baseOffset;
    private long keyPrefix;
    private int recordLength;
    private long currentPageNumber;
    private final TaskContext taskContext = TaskContext.get();

<<<<<<< HEAD
    private SortedIterator(int numRecords, int offset) {
      this.numRecords = numRecords;
      this.position = 0;
      this.offset = offset;
    }

    public SortedIterator clone() {
      SortedIterator iter = new SortedIterator(numRecords, offset);
=======
    private SortedIterator(int numRecords) {
      this.numRecords = numRecords;
      this.position = 0;
    }

    public SortedIterator clone () {
      SortedIterator iter = new SortedIterator(numRecords);
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      iter.position = position;
      iter.baseObject = baseObject;
      iter.baseOffset = baseOffset;
      iter.keyPrefix = keyPrefix;
      iter.recordLength = recordLength;
      iter.currentPageNumber = currentPageNumber;
      return iter;
    }

    @Override
<<<<<<< HEAD
    public int getNumRecords() {
      return numRecords;
    }

    @Override
    public boolean hasNext() {
      return position / 2 < numRecords;
=======
    public boolean hasNext() {
      return position / 2 < numRecords;
    }

    public int numRecordsLeft() {
      return numRecords - position / 2;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    }

    @Override
    public void loadNext() {
      // Kill the task in case it has been marked as killed. This logic is from
      // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
      // to avoid performance overhead. This check is added here in `loadNext()` instead of in
      // `hasNext()` because it's technically possible for the caller to be relying on
      // `getNumRecords()` instead of `hasNext()` to know when to stop.
      if (taskContext != null) {
        taskContext.killTaskIfInterrupted();
      }
      // This pointer points to a 4-byte record length, followed by the record's bytes
<<<<<<< HEAD
      final long recordPointer = array.get(offset + position);
      currentPageNumber = TaskMemoryManager.decodePageNumber(recordPointer);
      int uaoSize = UnsafeAlignedOffset.getUaoSize();
      baseObject = memoryManager.getPage(recordPointer);
      // Skip over record length
      baseOffset = memoryManager.getOffsetInPage(recordPointer) + uaoSize;
      recordLength = UnsafeAlignedOffset.getSize(baseObject, baseOffset - uaoSize);
      keyPrefix = array.get(offset + position + 1);
=======
      final long recordPointer = array.get(position);
      baseObject = memoryManager.getPage(recordPointer);
      baseOffset = memoryManager.getOffsetInPage(recordPointer) + 4;  // Skip over record length
      recordLength = Platform.getInt(baseObject, baseOffset - 4);
      keyPrefix = array.get(position + 1);
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      position += 2;
    }

    @Override
    public Object getBaseObject() { return baseObject; }

    @Override
    public long getBaseOffset() { return baseOffset; }

    public long getCurrentPageNumber() {
      return currentPageNumber;
    }

    @Override
    public int getRecordLength() { return recordLength; }

    @Override
    public long getKeyPrefix() { return keyPrefix; }
  }

  /**
   * Return an iterator over record pointers in sorted order. For efficiency, all calls to
   * {@code next()} will return the same mutable object.
   */
<<<<<<< HEAD
  public UnsafeSorterIterator getSortedIterator() {
    int offset = 0;
    long start = System.nanoTime();
    if (sortComparator != null) {
      if (this.radixSortSupport != null) {
        offset = RadixSort.sortKeyPrefixArray(
          array, nullBoundaryPos, (pos - nullBoundaryPos) / 2L, 0, 7,
          radixSortSupport.sortDescending(), radixSortSupport.sortSigned());
      } else {
        MemoryBlock unused = new MemoryBlock(
          array.getBaseObject(),
          array.getBaseOffset() + pos * 8L,
          (array.size() - pos) * 8L);
        LongArray buffer = new LongArray(unused);
        Sorter<RecordPointerAndKeyPrefix, LongArray> sorter =
          new Sorter<>(new UnsafeSortDataFormat(buffer));
        sorter.sort(array, 0, pos / 2, sortComparator);
      }
    }
    totalSortTimeNanos += System.nanoTime() - start;
    if (nullBoundaryPos > 0) {
      assert radixSortSupport != null : "Nulls are only stored separately with radix sort";
      LinkedList<UnsafeSorterIterator> queue = new LinkedList<>();

      // The null order is either LAST or FIRST, regardless of sorting direction (ASC|DESC)
      if (radixSortSupport.nullsFirst()) {
        queue.add(new SortedIterator(nullBoundaryPos / 2, 0));
        queue.add(new SortedIterator((pos - nullBoundaryPos) / 2, offset));
      } else {
        queue.add(new SortedIterator((pos - nullBoundaryPos) / 2, offset));
        queue.add(new SortedIterator(nullBoundaryPos / 2, 0));
      }
      return new UnsafeExternalSorter.ChainedIterator(queue);
    } else {
      return new SortedIterator(pos / 2, offset);
    }
=======
  public SortedIterator getSortedIterator() {
    sorter.sort(array, 0, pos / 2, sortComparator);
    return new SortedIterator(pos / 2);
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}
