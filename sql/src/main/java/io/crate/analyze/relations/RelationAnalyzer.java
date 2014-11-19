/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze.relations;

import com.google.common.base.Optional;
import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.Analyzer;
import io.crate.analyze.WhereClause;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.*;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

public class RelationAnalyzer extends DefaultTraversalVisitor<AnalyzedRelation, RelationAnalysisContext> {

    private final AnalysisMetaData analysisMetaData;
    private final Analyzer.ParameterContext parameterContext;

    private ExpressionAnalyzer expressionAnalyzer;
    private ExpressionAnalysisContext expressionAnalysisContext;

    public RelationAnalyzer(AnalysisMetaData analysisMetaData,
                            Analyzer.ParameterContext parameterContext) {
        this.analysisMetaData = analysisMetaData;
        this.parameterContext = parameterContext;
    }

    @Override
    protected AnalyzedRelation visitQuerySpecification(QuerySpecification node, RelationAnalysisContext context) {
        for (Relation relation : node.getFrom()) {
            process(relation, context);
        }
        expressionAnalyzer = new ExpressionAnalyzer(analysisMetaData, parameterContext, context.sources());
        expressionAnalysisContext = new ExpressionAnalysisContext();

        WhereClause whereClause = analyzeWhere(node.getWhere());
        List<Symbol> selectList = analyzeSelect(node.getSelect());
        List<Symbol> groupBy = analyzeGroupBy(node.getGroupBy());
        List<Symbol> orderBy = analyzeOrderBy(node.getOrderBy());
        Symbol having = analyzeHaving(node.getHaving());
        Integer limit = expressionAnalyzer.integerFromExpression(node.getLimit());
        int offset = firstNonNull(expressionAnalyzer.integerFromExpression(node.getOffset()), 0);

        return null;
    }

    private List<Symbol> analyzeOrderBy(List<SortItem> orderBy) {
        return null;
    }

    private List<Symbol> analyzeGroupBy(List<Expression> groupBy) {
        List<Symbol> groupBySymbols = new ArrayList<>(groupBy.size());
        for (Expression expression : groupBy) {
            Symbol symbol = expressionAnalyzer.normalize(expressionAnalyzer.convert(expression, expressionAnalysisContext));
        }
        return groupBySymbols;
    }

    private List<Symbol> analyzeSelect(Select select) {
        return null;
    }

    private Symbol analyzeHaving(Optional<Expression> having) {
        if (having.isPresent()) {
            return expressionAnalyzer.normalize(expressionAnalyzer.convert(having.get(), expressionAnalysisContext));
        }
        return null;
    }

    private WhereClause analyzeWhere(Optional<Expression> where) {
        if (!where.isPresent()) {
            return WhereClause.MATCH_ALL;
        }
        return new WhereClause(expressionAnalyzer.normalize(expressionAnalyzer.convert(where.get(), expressionAnalysisContext)));
    }

    @Override
    protected AnalyzedRelation visitAliasedRelation(AliasedRelation node, RelationAnalysisContext context) {
        context.addSourceRelation(
                null,
                node.getAlias(),
                process(node.getRelation(), new RelationAnalysisContext()));
        return null;
    }

    @Override
    protected AnalyzedRelation visitTable(Table node, RelationAnalysisContext context) {
        TableInfo tableInfo = analysisMetaData.referenceInfos().getTableInfoUnsafe(TableIdent.of(node));
        TableRelation tableRelation = new TableRelation(tableInfo);
        context.addSourceRelation(tableInfo.schemaInfo().name(), tableInfo.ident().name(), tableRelation);
        return tableRelation;
    }
}
