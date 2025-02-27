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
package org.apache.spark.sql.execution

import io.glutenproject.columnarbatch.ColumnarBatches
import io.glutenproject.memory.nmm.NativeMemoryManagers
import io.glutenproject.vectorized.{ColumnarBatchSerializeResult, ColumnarBatchSerializerJniWrapper}

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, BroadcastPartitioning, Partitioning}
import org.apache.spark.sql.execution.joins.{HashedRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.TaskResources

import scala.collection.JavaConverters._;

// Utility methods to convert Vanilla broadcast relations from/to Velox broadcast relations.
// FIXME: Truncate output with batch size.
object BroadcastUtils {
  def veloxToSparkUnsafe[F, T](
      context: SparkContext,
      mode: BroadcastMode,
      from: Broadcast[F],
      fn: Iterator[ColumnarBatch] => Iterator[InternalRow]): Broadcast[T] = {
    mode match {
      case HashedRelationBroadcastMode(_, _) => // no-op
      case _ => throw new IllegalStateException("Unexpected broadcast mode: " + mode)
    }
    // ColumnarBuildSideRelation to HashedRelation.
    val fromBroadcast = from.asInstanceOf[Broadcast[ColumnarBuildSideRelation]]
    val fromRelation = fromBroadcast.value.asReadOnlyCopy()
    var rowCount: Long = 0
    val toRelation = TaskResources.runUnsafe {
      val rowIterator = fn(fromRelation.deserialized.flatMap {
        cb =>
          rowCount += cb.numRows()
          Iterator(cb)
      })
      mode.transform(rowIterator, Some(rowCount))
    }
    // Rebroadcast Spark relation.
    context.broadcast(toRelation).asInstanceOf[Broadcast[T]]
  }

  def sparkToVeloxUnsafe[F, T](
      context: SparkContext,
      mode: BroadcastMode,
      schema: StructType,
      from: Broadcast[F],
      fn: Iterator[InternalRow] => Iterator[ColumnarBatch]): Broadcast[T] = {
    mode match {
      case HashedRelationBroadcastMode(_, _) => // no-op
      case _ => throw new IllegalStateException("Unexpected broadcast mode: " + mode)
    }
    // HashedRelation to ColumnarBuildSideRelation.
    val fromBroadcast = from.asInstanceOf[Broadcast[HashedRelation]]
    val fromRelation = fromBroadcast.value.asReadOnlyCopy()
    val keys = fromRelation.keys()
    val toRelation = TaskResources.runUnsafe {
      val batchItr: Iterator[ColumnarBatch] = fn(keys.flatMap(key => fromRelation.get(key)))
      val serialized: Array[Array[Byte]] = serializeStream(batchItr) match {
        case ColumnarBatchSerializeResult.EMPTY =>
          Array()
        case result: ColumnarBatchSerializeResult =>
          Array(result.getSerialized)
      }
      ColumnarBuildSideRelation(schema.toAttributes, serialized)
    }
    // Rebroadcast Velox relation.
    context.broadcast(toRelation).asInstanceOf[Broadcast[T]]
  }

  def getBroadcastMode(partitioning: Partitioning): BroadcastMode = {
    partitioning match {
      case BroadcastPartitioning(mode) =>
        mode
      case _ =>
        throw new IllegalArgumentException("Unexpected partitioning: " + partitioning.toString)
    }
  }

  def serializeStream(batches: Iterator[ColumnarBatch]): ColumnarBatchSerializeResult = {
    val filtered = batches
      .filter(_.numRows() != 0)
      .map(
        b => {
          ColumnarBatches.retain(b)
          b
        })
      .toArray
    if (filtered.isEmpty) {
      return ColumnarBatchSerializeResult.EMPTY
    }
    val batchRuntime = ColumnarBatches.getRuntime(filtered.toList.asJava)
    val handleArray = filtered.map(ColumnarBatches.getNativeHandle)
    val serializeResult =
      try {
        ColumnarBatchSerializerJniWrapper
          .forRuntime(batchRuntime)
          .serialize(
            handleArray,
            NativeMemoryManagers
              .contextInstance("BroadcastRelation")
              .getNativeInstanceHandle)
      } finally {
        filtered.foreach(ColumnarBatches.release)
      }
    serializeResult
  }
}
