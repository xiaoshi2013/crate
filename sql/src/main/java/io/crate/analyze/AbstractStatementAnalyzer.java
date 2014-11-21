/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.analyze.statements.DeprecatedAnalyzedStatement;
import io.crate.metadata.TableIdent;
import io.crate.sql.tree.*;

public abstract class AbstractStatementAnalyzer<R, C extends DeprecatedAnalyzedStatement> extends DefaultTraversalVisitor<R, C> {

    public abstract DeprecatedAnalyzedStatement newAnalysis(Analyzer.ParameterContext parameterContext);

    @Override
    protected R visitTable(Table node, C context) {
        Preconditions.checkState(context.table() == null, "selecting from multiple tables is not supported");
        TableIdent tableIdent = TableIdent.of(node);
        context.table(tableIdent);
        return null;
    }

    @Override
    protected R visitNode(Node node, C context) {
        throw new UnsupportedOperationException("Unsupported statement.");
    }

    /*
     * not supported yet expressions
     *
     * remove those methods if expressions gets supported
     */
    @Override
    protected R visitExtract(Extract node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitBetweenPredicate(BetweenPredicate node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitCoalesceExpression(CoalesceExpression node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitWith(With node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitWithQuery(WithQuery node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitWhenClause(WhenClause node, C context) {
        return visitNode(node, context);
    }

    @Override
    public R visitWindow(Window node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitSimpleCaseExpression(SimpleCaseExpression node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitNullIfExpression(NullIfExpression node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitIfExpression(IfExpression node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitSearchedCaseExpression(SearchedCaseExpression node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitSubqueryExpression(SubqueryExpression node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitUnion(Union node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitIntersect(Intersect node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitExcept(Except node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitTableSubquery(TableSubquery node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitJoin(Join node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitSampledRelation(SampledRelation node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitCurrentTime(CurrentTime node, C context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitExists(ExistsPredicate node, C context) {
        return visitNode(node, context);
    }

    @Override
    public R visitInputReference(InputReference node, C context) {
        return visitNode(node, context);
    }

    @Override
    public R visitMatchPredicate(MatchPredicate node, C context) {
        return visitNode(node, context);
    }
}
