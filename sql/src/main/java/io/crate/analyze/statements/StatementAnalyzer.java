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

package io.crate.analyze.statements;

import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.Analyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationAnalysisContext;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Query;

public class StatementAnalyzer extends DefaultTraversalVisitor<Void, StatementAnalysis> {

    private AnalysisMetaData analysisMetaData;
    private final Analyzer.ParameterContext parameterContext;

    public StatementAnalyzer(AnalysisMetaData analysisMetaData,
                             Analyzer.ParameterContext parameterContext) {
        this.analysisMetaData = analysisMetaData;
        this.parameterContext = parameterContext;
    }

    @Override
    protected Void visitQuery(Query node, StatementAnalysis context) {
        RelationAnalyzer relationAnalyzer = new RelationAnalyzer(analysisMetaData, parameterContext);
        RelationAnalysisContext relationAnalysisContext = new RelationAnalysisContext();
        AnalyzedRelation relation = relationAnalyzer.process(node.getQueryBody(), relationAnalysisContext);
        context.relation(relation);
        return null;
    }
}
