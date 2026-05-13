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

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite

class AlignExpandOutputTypesSuite extends AnyFunSuite {
  test("gate alignment rule to Spark 3.2 and 3.3") {
    val Version = """(\d+)\.(\d+).*""".r
    val Version(major, minor) = SPARK_VERSION
    val expected = major.toInt == 3 && minor.toInt >= 2 && minor.toInt < 4

    assert(ExpandOutputTypeAlignment.isSpark32Or33 == expected)
  }

  test("align null literal type to expand output type") {
    val outputType = DecimalType(20, 6)
    val expression = Literal.create(null, NullType)
    val output = AttributeReference("value", outputType, nullable = true)()

    val aligned = ExpandOutputTypeAlignment.alignProjections(
      Seq(Seq(expression)),
      Seq(output),
      Seq.empty)

    assert(aligned.head.head == Literal.create(null, outputType))
  }

  test("cast only known decimal complex expression mismatch") {
    val inputType = DecimalType(20, 6)
    val outputType = DecimalType(30, 6)
    val amount = AttributeReference("amount", inputType, nullable = true)()
    val expression = Add(amount, Literal(Decimal(1), inputType))
    val output = AttributeReference("value", outputType, nullable = true)()

    val aligned = ExpandOutputTypeAlignment.alignProjections(
      Seq(Seq(expression)),
      Seq(output),
      Seq(amount))

    aligned.head.head match {
      case Cast(child, castType, _, _) =>
        assert(child == expression)
        assert(castType == outputType)
      case other =>
        fail(s"Expected decimal expression to be cast to $outputType, got $other")
    }
  }

  test("do not insert generic casts for non-decimal mismatch") {
    val id = AttributeReference("id", IntegerType, nullable = false)()
    val expression = Add(id, Literal(1))
    val output = AttributeReference("value", LongType, nullable = false)()

    val aligned = ExpandOutputTypeAlignment.alignProjections(
      Seq(Seq(expression)),
      Seq(output),
      Seq(id))

    assert(aligned.head.head eq expression)
  }
}
