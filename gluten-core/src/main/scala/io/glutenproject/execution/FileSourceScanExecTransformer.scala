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
package io.glutenproject.execution

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PlanExpression}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.FileSourceScanExecShim
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.collection.BitSet

class FileSourceScanExecTransformer(
    @transient override val relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false)
  extends FileSourceScanExecShim(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan)
  with DatasourceScanTransformer {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance
      .genFileSourceScanTransformerMetrics(sparkContext) ++ staticMetricsAlias

  /** SQL metrics generated only for scans using dynamic partition pruning. */
  private lazy val staticMetricsAlias =
    if (partitionFilters.exists(FileSourceScanExecTransformer.isDynamicPruningFilter)) {
      Map(
        "staticFilesNum" -> SQLMetrics.createMetric(sparkContext, "static number of files read"),
        "staticFilesSize" -> SQLMetrics.createSizeMetric(sparkContext, "static size of files read")
      )
    } else {
      Map.empty[String, SQLMetric]
    }

  override def filterExprs(): Seq[Expression] = dataFilters

  override def outputAttributes(): Seq[Attribute] = output

  override def getPartitions: Seq[InputPartition] =
    BackendsApiManager.getTransformerApiInstance.genInputPartitionSeq(
      relation,
      dynamicallySelectedPartitions,
      output,
      bucketedScan,
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      disableBucketedScan)

  override def getPartitionSchema: StructType = relation.partitionSchema

  override def getDataSchema: StructType = relation.dataSchema

  override def getInputFilePathsInternal: Seq[String] = {
    relation.location.inputFiles.toSeq
  }

  override def equals(other: Any): Boolean = other match {
    case that: FileSourceScanExecTransformer =>
      that.canEqual(this) && super.equals(that)
    case _ => false
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[FileSourceScanExecTransformer]

  override def hashCode(): Int = super.hashCode()

  override protected def doValidateInternal(): ValidationResult = {
    if (hasMetadataColumns) {
      return ValidationResult.notOk(s"Unsupported metadataColumns scan in native.")
    }

    if (hasFieldIds) {
      // Spark read schema expects field Ids , the case didn't support yet by native.
      return ValidationResult.notOk(
        s"Unsupported matching schema column names " +
          s"by field ids in native scan.")
    }
    super.doValidateInternal()
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    doExecuteColumnarInternal()
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genFileSourceScanTransformerMetricsUpdater(metrics)

  override val nodeNamePrefix: String = "NativeFile"

  override val nodeName: String = {
    s"Scan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

  override def getProperties: Map[String, String] = {
    this.fileFormat match {
      case ReadFileFormat.TextReadFormat =>
        var options: Map[String, String] = Map()
        relation.options.foreach {
          case ("delimiter", v) => options += ("field_delimiter" -> v)
          case ("quote", v) => options += ("quote" -> v)
          case ("header", v) =>
            val cnt = if (v == "true") 1 else 0
            options += ("header" -> cnt.toString)
          case ("escape", v) => options += ("escape" -> v)
          case ("nullvalue", v) => options += ("nullValue" -> v)
          case (_, _) =>
        }
        options
      case _ => Map.empty
    }
  }

  @transient override lazy val fileFormat: ReadFileFormat =
    relation.fileFormat.getClass.getSimpleName match {
      case "OrcFileFormat" => ReadFileFormat.OrcReadFormat
      case "ParquetFileFormat" => ReadFileFormat.ParquetReadFormat
      case "DwrfFileFormat" => ReadFileFormat.DwrfReadFormat
      case "DeltaMergeTreeFileFormat" => ReadFileFormat.MergeTreeReadFormat
      case "CSVFileFormat" => ReadFileFormat.TextReadFormat
      case _ => ReadFileFormat.UnknownFormat
    }

  override def doCanonicalize(): FileSourceScanExecTransformer = {
    val canonicalized = super.doCanonicalize()
    new FileSourceScanExecTransformer(
      canonicalized.relation,
      canonicalized.output,
      canonicalized.requiredSchema,
      canonicalized.partitionFilters,
      canonicalized.optionalBucketSet,
      canonicalized.optionalNumCoalescedBuckets,
      canonicalized.dataFilters,
      canonicalized.tableIdentifier,
      canonicalized.disableBucketedScan
    )
  }
}

object FileSourceScanExecTransformer {
  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined
}
