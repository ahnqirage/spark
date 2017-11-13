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

package org.apache.spark.sql.catalyst.expressions.codegen;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;

/**
 * A helper class to manage the row buffer when construct unsafe rows.
 */
public class BufferHolder {
  public byte[] buffer;
  public int cursor = Platform.BYTE_ARRAY_OFFSET;
  private final UnsafeRow row;
  private final int fixedSize;

  public BufferHolder(UnsafeRow row) {
    this(row, 64);
  }

<<<<<<< HEAD
  public BufferHolder(UnsafeRow row, int initialSize) {
    int bitsetWidthInBytes = UnsafeRow.calculateBitSetWidthInBytes(row.numFields());
    if (row.numFields() > (ARRAY_MAX - initialSize - bitsetWidthInBytes) / 8) {
      throw new UnsupportedOperationException(
        "Cannot create BufferHolder for input UnsafeRow because there are " +
          "too many fields (number of fields: " + row.numFields() + ")");
    }
    this.fixedSize = bitsetWidthInBytes + 8 * row.numFields();
    this.buffer = new byte[fixedSize + initialSize];
    this.row = row;
    this.row.pointTo(buffer, buffer.length);
  }

  public BufferHolder() {
    this(64);
  }

  public BufferHolder(int size) {
    buffer = new byte[size];
  }

  /**
   * Grows the buffer to at least neededSize. If row is non-null, points the row to the buffer.
   */
  public void grow(int neededSize, UnsafeRow row) {
    final int length = totalSize() + neededSize;
    if (buffer.length < length) {
      // This will not happen frequently, because the buffer is re-used.
      int newLength = length < ARRAY_MAX / 2 ? length * 2 : ARRAY_MAX;
      final byte[] tmp = new byte[newLength];
      Platform.copyMemory(
        buffer,
        Platform.BYTE_ARRAY_OFFSET,
        tmp,
        Platform.BYTE_ARRAY_OFFSET,
        totalSize());
      buffer = tmp;
      if (row != null) {
        row.pointTo(buffer, length * 2);
      }
    }
  }

  public void grow(int neededSize) {
    grow(neededSize, null);
  }

  public void reset() {
    cursor = Platform.BYTE_ARRAY_OFFSET + fixedSize;
  }
  public void resetTo(int offset) {
    assert(offset <= buffer.length);
    cursor = Platform.BYTE_ARRAY_OFFSET + offset;
  }
  public void resetTo(int offset) {
    assert(offset <= buffer.length);
    cursor = Platform.BYTE_ARRAY_OFFSET + offset;
  }

  public int totalSize() {
    return cursor - Platform.BYTE_ARRAY_OFFSET;
  }
}
