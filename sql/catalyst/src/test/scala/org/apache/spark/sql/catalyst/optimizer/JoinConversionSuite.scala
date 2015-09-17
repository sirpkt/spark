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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._

class JoinConversionSuite extends PlanTest {

  object OptimizeWithoutFilterPushdown extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubQueries) ::
        Batch("Join Conversion", Once,
        OuterJoinToInnerJoin) :: Nil
  }

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubQueries) ::
        Batch("Join Conversion", Once,
          OuterJoinToInnerJoin) ::
        Batch("Filter Pushdown", Once,
          SamplePushDown,
          CombineFilters,
          PushPredicateThroughProject,
          BooleanSimplification,
          PushPredicateThroughJoin,
          PushPredicateThroughGenerate,
          ColumnPruning,
          ProjectCollapsing) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  val testRelation1 = LocalRelation('d.int)

  // This test already passes.
  test("eliminate subqueries") {
    val originalQuery =
      testRelation
        .subquery('y)
        .select('a)

    val optimized = OptimizeWithoutFilterPushdown.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select('a.attr)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: convert left outer join #1") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, LeftOuter)
        .where("x.b".attr === 1 && "y.b".attr === 2)
    }

    val optimizedWithoutFilterPushdown = OptimizeWithoutFilterPushdown.execute(originalQuery.analyze)
    val correctAnswerWithoutFilterPushdown =
      x.join(y, Inner).where("x.b".attr === 1 && "y.b".attr === 2).analyze

    comparePlans(optimizedWithoutFilterPushdown, correctAnswerWithoutFilterPushdown)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      x.where('b === 1).join(y.where('b === 2), Inner).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: convert right outer join #1") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, RightOuter)
        .where("x.b".attr === 1 && "y.b".attr === 2)
    }

    val optimizedWithoutFilterPushdown = OptimizeWithoutFilterPushdown.execute(originalQuery.analyze)
    val correctAnswerWithoutFilterPushdown =
      x.join(y, Inner).where("x.b".attr === 1 && "y.b".attr === 2).analyze

    comparePlans(optimizedWithoutFilterPushdown, correctAnswerWithoutFilterPushdown)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      x.where('b === 1).join(y.where('b === 2), Inner).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: convert left outer join #2") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, LeftOuter, Some("x.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2)
    }

    val optimizedWithoutFilterPushdown = OptimizeWithoutFilterPushdown.execute(originalQuery.analyze)
    val correctAnswerWithoutFilterPushdown =
      x.join(y, Inner, Some("x.b".attr === 1)).where("x.b".attr === 2 && "y.b".attr === 2).analyze

    comparePlans(optimizedWithoutFilterPushdown, correctAnswerWithoutFilterPushdown)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      x.where('b === 2).join(y.where('b === 2), Inner, Some("x.b".attr === 1)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: convert right outer join #2") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, RightOuter, Some("y.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2)
    }

    val optimizedWithoutFilterPushdown = OptimizeWithoutFilterPushdown.execute(originalQuery.analyze)
    val correctAnswerWithoutFilterPushdown =
      x.join(y, Inner, Some("y.b".attr === 1)).where("x.b".attr === 2 && "y.b".attr === 2).analyze

    comparePlans(optimizedWithoutFilterPushdown, correctAnswerWithoutFilterPushdown)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      x.where('b === 2).join(y.where('b === 2), Inner, Some("y.b".attr === 1)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: convert left outer join #3") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, LeftOuter, Some("y.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2)
    }

    val optimizedWithoutFilterPushdown = OptimizeWithoutFilterPushdown.execute(originalQuery.analyze)
    val correctAnswerWithoutFilterPushdown =
      x.join(y, Inner, Some("y.b".attr === 1)).where("x.b".attr === 2 && "y.b".attr === 2).analyze

    comparePlans(optimizedWithoutFilterPushdown, correctAnswerWithoutFilterPushdown)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      x.where('b === 2).join(y.where('b === 2), Inner, Some("y.b".attr === 1)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: convert right outer join #3") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, RightOuter, Some("x.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2)
    }

    val optimizedWithoutFilterPushdown = OptimizeWithoutFilterPushdown.execute(originalQuery.analyze)
    val correctAnswerWithoutFilterPushdown =
      x.join(y, Inner, Some("x.b".attr === 1)).where("x.b".attr === 2 && "y.b".attr === 2).analyze

    comparePlans(optimizedWithoutFilterPushdown, correctAnswerWithoutFilterPushdown)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      x.where('b === 2).join(y.where('b === 2), Inner, Some("x.b".attr === 1)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: convert left outer join #4") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, LeftOuter)
        .where("x.b".attr === 2 && "y.b".attr.isNotNull)
    }

    val correctAnswerWithoutFilterPushdown =
      x.join(y, Inner).where("x.b".attr === 2 && "y.b".attr.isNotNull).analyze
    val optimizedWithoutFilterPushdown = OptimizeWithoutFilterPushdown.execute(originalQuery.analyze)

    comparePlans(optimizedWithoutFilterPushdown, correctAnswerWithoutFilterPushdown)

    val correctAnswer =
      x.where('b.attr === 2).join(y.where('b.attr.isNotNull), Inner).analyze
    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("joins: convert right outer join #4") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, RightOuter)
        .where("y.b".attr === 2 && "x.b".attr.isNotNull)
    }

    val correctAnswerWithoutFilterPushdown =
      x.join(y, Inner).where("y.b".attr === 2 && "x.b".attr.isNotNull).analyze
    val optimizedWithoutFilterPushdown = OptimizeWithoutFilterPushdown.execute(originalQuery.analyze)

    comparePlans(optimizedWithoutFilterPushdown, correctAnswerWithoutFilterPushdown)

    val correctAnswer =
      x.where('b.attr.isNotNull).join(y.where('b.attr === 2), Inner).analyze
    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("joins: do not convert left outer join #1") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, LeftOuter)
        .where("x.b".attr === 2 && "y.b".attr.isNull)
    }

    val correctAnswerWithoutFilterPushdown =
      x.join(y, LeftOuter).where("x.b".attr === 2 && "y.b".attr.isNull).analyze
    val optimizedWithoutFilterPushdown = OptimizeWithoutFilterPushdown.execute(originalQuery.analyze)

    comparePlans(optimizedWithoutFilterPushdown, correctAnswerWithoutFilterPushdown)

    val correctAnswer =
      x.where('b.attr === 2).join(y, LeftOuter).where("y.b".attr.isNull).analyze
    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("joins: do not convert right outer join #1") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, RightOuter)
        .where("y.b".attr === 2 && "x.b".attr.isNull)
    }

    val correctAnswerWithoutFilterPushdown =
      x.join(y, RightOuter).where("y.b".attr === 2 && "x.b".attr.isNull).analyze
    val optimizedWithoutFilterPushdown = OptimizeWithoutFilterPushdown.execute(originalQuery.analyze)

    comparePlans(optimizedWithoutFilterPushdown, correctAnswerWithoutFilterPushdown)

    val correctAnswer =
      x.join(y.where('b.attr === 2), RightOuter).where("x.b".attr.isNull).analyze
    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, correctAnswer)
  }
}
