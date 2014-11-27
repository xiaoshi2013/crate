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

package io.crate.analyze.relations.select;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import io.crate.analyze.OutputNameFormatter;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.validator.SelectSymbolValidator;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.*;

import java.util.Map;

public class SelectAnalyzer {

    private static final InnerVisitor INSTANCE = new InnerVisitor();

    public static SelectAnalysis newSelectAnalysis(Map<QualifiedName, AnalyzedRelation> sources,
                                                   ExpressionAnalyzer expressionAnalyzer,
                                                   ExpressionAnalysisContext expressionAnalysisContext,
                                                   boolean selectFromFieldCache) {
        return new SelectAnalysis(sources, expressionAnalyzer, expressionAnalysisContext, selectFromFieldCache);
    }

    public static Multimap<String, Symbol> getOutputs(Select node, SelectAnalysis selectAnalysis) {
        INSTANCE.process(node, selectAnalysis);
        SelectSymbolValidator.validate(selectAnalysis.aliasedOutputs.values(), selectAnalysis.selectFromFieldCache);
        return selectAnalysis.aliasedOutputs;
    }


    public static class SelectAnalysis {

        private Map<QualifiedName, AnalyzedRelation> sources;
        private ExpressionAnalyzer expressionAnalyzer;
        private ExpressionAnalysisContext expressionAnalysisContext;
        private boolean selectFromFieldCache;
        private Multimap<String, Symbol> aliasedOutputs = LinkedListMultimap.create();

        private SelectAnalysis(Map<QualifiedName, AnalyzedRelation> sources,
                               ExpressionAnalyzer expressionAnalyzer,
                               ExpressionAnalysisContext expressionAnalysisContext,
                               boolean selectFromFieldCache) {
            this.sources = sources;
            this.expressionAnalyzer = expressionAnalyzer;
            this.expressionAnalysisContext = expressionAnalysisContext;
            this.selectFromFieldCache = selectFromFieldCache;
        }

        public Symbol toSymbol(Expression expression) {
            return expressionAnalyzer.convert(expression, expressionAnalysisContext);
        }

        public void add(String outputName, Symbol symbol) {
            aliasedOutputs.put(outputName, symbol);
        }
    }

    private static class InnerVisitor extends DefaultTraversalVisitor<Void, SelectAnalysis> {

        @Override
        protected Void visitSingleColumn(SingleColumn node, SelectAnalysis context) {
            Symbol symbol = context.toSymbol(node.getExpression());
            if (node.getAlias().isPresent()) {
                context.add(node.getAlias().get(), symbol);
            } else {
                context.add(OutputNameFormatter.format(node.getExpression()), symbol);
            }
            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, SelectAnalysis context) {
            for (AnalyzedRelation relation : context.sources.values()) {
                for (Map.Entry<String, Symbol> outputEntry : relation.outputs().entrySet()) {
                    context.add(outputEntry.getKey(), outputEntry.getValue());
                }
            }
            return null;
        }
    }
}
