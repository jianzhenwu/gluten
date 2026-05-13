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
package org.apache.gluten.extension.columnar.rewrite

import org.apache.gluten.expression.ExpressionConverter

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{ExpandExec, SparkPlan}
import org.apache.spark.sql.types.{DataType, DecimalType}
import org.apache.spark.util.SparkVersionUtil

import scala.util.control.NonFatal

/**
 * Spark 3.2 and 3.3 may produce Expand projections whose expression output types do not exactly
 * match the corresponding Expand output attributes. Spark's row path tolerates this, but native
 * Expand conversion requires each projection column to have one deterministic output type.
 */
object AlignExpandOutputTypes extends RewriteSingleNode {
  override def isRewritable(plan: SparkPlan): Boolean = {
    ExpandOutputTypeAlignment.isSpark32Or33 && plan.isInstanceOf[ExpandExec]
  }

  override def rewrite(plan: SparkPlan): SparkPlan = plan match {
    case expand: ExpandExec if isRewritable(expand) =>
      val alignedProjections = ExpandOutputTypeAlignment.alignProjections(
        expand.projections,
        expand.output,
        expand.child.output)
      if (alignedProjections == expand.projections) {
        expand
      } else {
        val newExpand = expand.copy(projections = alignedProjections)
        newExpand.copyTagsFrom(expand)
        newExpand
      }
    case _ => plan
  }
}

private[gluten] object ExpandOutputTypeAlignment {
  val isSpark32Or33: Boolean =
    SparkVersionUtil.compareMajorMinorVersion((3, 2)) >= 0 &&
      SparkVersionUtil.compareMajorMinorVersion((3, 4)) < 0

  def alignProjections(
      projections: Seq[Seq[Expression]],
      output: Seq[Attribute],
      inputAttributes: Seq[Attribute]): Seq[Seq[Expression]] = {
    projections.map {
      projection =>
        projection.zipWithIndex.map {
          case (expression, colIdx) if colIdx < output.length =>
            alignExpression(expression, output(colIdx).dataType, inputAttributes)
          case (expression, _) =>
            expression
        }
    }
  }

  def projectionTypeMismatchMessageIfAny(
      projections: Seq[Seq[Expression]],
      output: Seq[Attribute],
      inputAttributes: Seq[Attribute]): Option[String] = {
    if (projections.nonEmpty && output.nonEmpty) {
      val mismatchColumns =
        output.indices.filter(columnTypesMismatch(projections, output, inputAttributes, _))
      if (mismatchColumns.nonEmpty) {
        val diagnostics = mismatchColumns
          .take(5)
          .map(columnDiagnostics(projections, output, inputAttributes, _))
          .mkString("; ")
        val omittedColumns = mismatchColumns.size - 5
        val suffix =
          if (omittedColumns > 0) s"; ... $omittedColumns more mismatch column(s)" else ""
        return Some(
          "ExpandExecTransformer detected projection/output type mismatch before " +
            "Substrait conversion. Failing validation to avoid native ExpandRel " +
            "with inconsistent Spark or transformer column types: " +
            s"$diagnostics$suffix")
      }
    }
    None
  }

  private def alignExpression(
      expression: Expression,
      outputType: DataType,
      inputAttributes: Seq[Attribute]): Expression = {
    expression match {
      case Literal(null, _) if needsTypeAlignment(expression, outputType, inputAttributes) =>
        Literal.create(null, outputType)
      case other if needsExplicitOutputCast(other, outputType) =>
        // Keep inserted casts scoped to the known Spark 3.2/3.3 decimal Expand mismatch.
        Cast(other, outputType)
      case _ =>
        expression
    }
  }

  private def needsTypeAlignment(
      expression: Expression,
      outputType: DataType,
      inputAttributes: Seq[Attribute]): Boolean = {
    expression.dataType != outputType ||
    transformerDataType(expression, inputAttributes).exists(_ != outputType) ||
    needsExplicitOutputCast(expression, outputType)
  }

  private def needsExplicitOutputCast(expression: Expression, outputType: DataType): Boolean = {
    outputType.isInstanceOf[DecimalType] &&
    isNotAttributeAndLiteral(expression) &&
    !isCastToType(expression, outputType)
  }

  private def isCastToType(expression: Expression, dataType: DataType): Boolean =
    expression match {
      case Cast(_, castType, _, _) if castType == dataType => true
      case _ => false
    }

  private def isNotAttributeAndLiteral(expression: Expression): Boolean = expression match {
    case _: Attribute | _: BoundReference | _: Literal => false
    case _ => true
  }

  private def columnTypesMismatch(
      projections: Seq[Seq[Expression]],
      output: Seq[Attribute],
      inputAttributes: Seq[Attribute],
      colIdx: Int): Boolean = {
    if (projections.exists(_.length <= colIdx)) {
      return true
    }
    val outputType = output(colIdx).dataType
    val expressions = projections.map(_(colIdx))
    val sparkTypes = expressions.map(_.dataType)
    val transformerTypes = expressions.flatMap(transformerDataType(_, inputAttributes))
    sparkTypes.distinct.size > 1 ||
    sparkTypes.exists(_ != outputType) ||
    transformerTypes.distinct.size > 1 ||
    transformerTypes.exists(_ != outputType)
  }

  private def columnDiagnostics(
      projections: Seq[Seq[Expression]],
      output: Seq[Attribute],
      inputAttributes: Seq[Attribute],
      colIdx: Int): String = {
    val outputAttr = output(colIdx)
    val projectionTypes = projections.zipWithIndex.map {
      case (row, rowIdx) =>
        row.lift(colIdx) match {
          case Some(expression) =>
            val nativeType = transformerDataType(expression, inputAttributes)
              .map(_.catalogString)
              .getOrElse("<unavailable>")
            s"row[$rowIdx]=${expression.sql}:spark=${expression.dataType.catalogString}," +
              s"transformer=$nativeType"
          case None =>
            s"row[$rowIdx]=<missing>"
        }
    }
    s"col[$colIdx]=${outputAttr.name}:output=${outputAttr.dataType.catalogString}," +
      s" projections=[${projectionTypes.mkString(", ")}]"
  }

  private def transformerDataType(
      expression: Expression,
      inputAttributes: Seq[Attribute]): Option[DataType] = {
    try {
      Some(ExpressionConverter.replaceWithExpressionTransformer(
        expression,
        inputAttributes).dataType)
    } catch {
      case NonFatal(_) => None
    }
  }
}
