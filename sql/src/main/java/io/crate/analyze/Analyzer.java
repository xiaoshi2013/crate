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

import io.crate.analyze.statements.StatementAnalysis;
import io.crate.analyze.statements.StatementAnalyzer;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.sql.tree.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Inject;

import java.util.Locale;

import static io.crate.planner.symbol.Literal.newLiteral;

public class Analyzer {

    private final AnalyzerDispatcher dispatcher;

    private final static Object[] EMPTY_ARGS = new Object[0];
    private final static Object[][] EMPTY_BULK_ARGS = new Object[0][];

    @Inject
    public Analyzer(AnalyzerDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public Analysis analyze(Statement statement) {
        return analyze(statement, EMPTY_ARGS, EMPTY_BULK_ARGS);
    }

    public Analysis analyze(Statement statement, Object[] parameters, Object[][] bulkParams) {
        ParameterContext parameterContext = new ParameterContext(parameters, bulkParams);
        AnalyzedStatement analyzedStatement = dispatcher.process(statement, parameterContext);
        return new Analysis(analyzedStatement);
    }

    public static class ParameterContext {
        final Object[] parameters;
        final Object[][] bulkParameters;
        DataType[] bulkTypes;

        private int currentIdx = 0;

        public ParameterContext(Object[] parameters, Object[][] bulkParameters) {
            this.parameters = parameters;
            if (bulkParameters.length > 0) {
                validateBulkParams(bulkParameters);
            }
            this.bulkParameters = bulkParameters;
        }

        private void validateBulkParams(Object[][] bulkParams) {
            for (Object[] bulkParam : bulkParams) {
                if (bulkTypes == null) {
                    initializeBulkTypes(bulkParam);
                    continue;
                } else if (bulkParam.length != bulkTypes.length) {
                    throw new IllegalArgumentException("mixed number of arguments inside bulk arguments");
                }

                for (int i = 0; i < bulkParam.length; i++) {
                    Object o = bulkParam[i];
                    DataType expectedType = bulkTypes[i];
                    DataType guessedType = guessTypeSafe(o);

                    if (expectedType == DataTypes.UNDEFINED) {
                        bulkTypes[i] = guessedType;
                    } else if (o != null && !bulkTypes[i].equals(guessedType)) {
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                "argument %d of bulk arguments contains mixed data types", i + 1));
                    }
                }
            }
        }

        private static DataType guessTypeSafe(Object value) throws IllegalArgumentException {
            DataType guessedType = DataTypes.guessType(value, true);
            if (guessedType == null) {
                throw new IllegalArgumentException(String.format(
                        "Got an argument \"%s\" that couldn't be recognized", value));
            }
            return guessedType;
        }

        private void initializeBulkTypes(Object[] bulkParam) {
            bulkTypes = new DataType[bulkParam.length];
            for (int i = 0; i < bulkParam.length; i++) {
                bulkTypes[i] = guessTypeSafe(bulkParam[i]);
            }
        }

        public boolean hasBulkParams() {
            return bulkParameters.length > 0;
        }

        public void setBulkIdx(int i) {
            this.currentIdx = i;
        }

        public Object[] parameters() {
            if (hasBulkParams()) {
                return bulkParameters[currentIdx];
            }
            return parameters;
        }

        public io.crate.planner.symbol.Literal getAsSymbol(int index) {
            try {
                if (hasBulkParams()) {
                    // already did a type guess so it is possible to create a literal directly
                    return newLiteral(bulkTypes[index], bulkParameters[currentIdx][index]);
                }
                DataType type = guessTypeSafe(parameters[index]);
                // use type.value because some types need conversion (String to BytesRef, List to Array)
                return newLiteral(type, type.value(parameters[index]));
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "Tried to resolve a parameter but the arguments provided with the " +
                                "SQLRequest don't contain a parameter at position %d", index), e);
            }
        }
    }

    public static class AnalyzerDispatcher extends AstVisitor<AnalyzedStatement, ParameterContext> {

        private Functions functions;
        private ReferenceInfos referenceInfos;
        private ReferenceResolver referenceResolver;
        private final InsertFromValuesAnalyzer insertFromValuesAnalyzer;
        private final InsertFromSubQueryAnalyzer insertFromSubQueryAnalyzer;
        private final UpdateStatementAnalyzer updateStatementAnalyzer;
        private final DeleteStatementAnalyzer deleteStatementAnalyzer;
        private final CopyStatementAnalyzer copyStatementAnalyzer;
        private final DropTableStatementAnalyzer dropTableStatementAnalyzer;
        private final CreateTableStatementAnalyzer createTableStatementAnalyzer;
        private final CreateBlobTableStatementAnalyzer createBlobTableStatementAnalyzer;
        private final CreateAnalyzerStatementAnalyzer createAnalyzerStatementAnalyzer;
        private final DropBlobTableStatementAnalyzer dropBlobTableStatementAnalyzer;
        private final RefreshTableAnalyzer refreshTableAnalyzer;
        private final AlterTableAnalyzer alterTableAnalyzer;
        private final AlterBlobTableAnalyzer alterBlobTableAnalyzer;
        private final SetStatementAnalyzer setStatementAnalyzer;
        private final AlterTableAddColumnAnalyzer alterTableAddColumnAnalyzer;

        @Inject
        public AnalyzerDispatcher(Functions functions,
                                  ReferenceInfos referenceInfos,
                                  ReferenceResolver referenceResolver,
                                  InsertFromValuesAnalyzer insertFromValuesAnalyzer,
                                  InsertFromSubQueryAnalyzer insertFromSubQueryAnalyzer,
                                  UpdateStatementAnalyzer updateStatementAnalyzer,
                                  DeleteStatementAnalyzer deleteStatementAnalyzer,
                                  CopyStatementAnalyzer copyStatementAnalyzer,
                                  DropTableStatementAnalyzer dropTableStatementAnalyzer,
                                  CreateTableStatementAnalyzer createTableStatementAnalyzer,
                                  CreateBlobTableStatementAnalyzer createBlobTableStatementAnalyzer,
                                  CreateAnalyzerStatementAnalyzer createAnalyzerStatementAnalyzer,
                                  DropBlobTableStatementAnalyzer dropBlobTableStatementAnalyzer,
                                  RefreshTableAnalyzer refreshTableAnalyzer,
                                  AlterTableAnalyzer alterTableAnalyzer,
                                  AlterBlobTableAnalyzer alterBlobTableAnalyzer,
                                  SetStatementAnalyzer setStatementAnalyzer,
                                  AlterTableAddColumnAnalyzer alterTableAddColumnAnalyzer) {
            this.functions = functions;
            this.referenceInfos = referenceInfos;
            this.referenceResolver = referenceResolver;
            this.insertFromValuesAnalyzer = insertFromValuesAnalyzer;
            this.insertFromSubQueryAnalyzer = insertFromSubQueryAnalyzer;
            this.updateStatementAnalyzer = updateStatementAnalyzer;
            this.deleteStatementAnalyzer = deleteStatementAnalyzer;
            this.copyStatementAnalyzer = copyStatementAnalyzer;
            this.dropTableStatementAnalyzer = dropTableStatementAnalyzer;
            this.createTableStatementAnalyzer = createTableStatementAnalyzer;
            this.createBlobTableStatementAnalyzer = createBlobTableStatementAnalyzer;
            this.createAnalyzerStatementAnalyzer = createAnalyzerStatementAnalyzer;
            this.dropBlobTableStatementAnalyzer = dropBlobTableStatementAnalyzer;
            this.refreshTableAnalyzer = refreshTableAnalyzer;
            this.alterTableAnalyzer = alterTableAnalyzer;
            this.alterBlobTableAnalyzer = alterBlobTableAnalyzer;
            this.setStatementAnalyzer = setStatementAnalyzer;
            this.alterTableAddColumnAnalyzer = alterTableAddColumnAnalyzer;
        }

        private static AnalyzedStatement analyze(AbstractStatementAnalyzer statementAnalyzer,
                                                 Statement statement,
                                                 ParameterContext parameterContext) {
            AnalyzedStatement analyzedStatement = statementAnalyzer.newAnalysis(parameterContext);
            //noinspection unchecked
            statementAnalyzer.process(statement, analyzedStatement);
            analyzedStatement.normalize();
            return analyzedStatement;
        }

        @Override
        protected AnalyzedStatement visitQuery(Query node, ParameterContext context) {
            StatementAnalyzer statementAnalyzer = new StatementAnalyzer(
                    new AnalysisMetaData(functions, referenceInfos, referenceResolver), context);

            StatementAnalysis statementAnalysis = statementAnalyzer.process(node, null);
            return (AnalyzedStatement) statementAnalysis.relation();
        }

        @Override
        public AnalyzedStatement visitDelete(Delete node, ParameterContext context) {
            return analyze(deleteStatementAnalyzer, node, context);
        }

        @Override
        public AnalyzedStatement visitInsertFromValues(InsertFromValues node, ParameterContext context) {
            return analyze(insertFromValuesAnalyzer, node, context);
        }

        @Override
        public AnalyzedStatement visitInsertFromSubquery(InsertFromSubquery node, ParameterContext context) {
            return analyze(insertFromSubQueryAnalyzer, node, context);
        }

        @Override
        public AnalyzedStatement visitUpdate(Update node, ParameterContext context) {
            return analyze(updateStatementAnalyzer, node, context);
        }

        @Override
        public AnalyzedStatement visitCopyFromStatement(CopyFromStatement node, ParameterContext context) {
            return analyze(copyStatementAnalyzer, node, context);
        }

        @Override
        public AnalyzedStatement visitCopyTo(CopyTo node, ParameterContext context) {
            return analyze(copyStatementAnalyzer, node, context);
        }

        @Override
        public AnalyzedStatement visitDropTable(DropTable node, ParameterContext context) {
            return analyze(dropTableStatementAnalyzer, node, context);
        }

        @Override
        public AnalyzedStatement visitCreateTable(CreateTable node, ParameterContext context) {
            return analyze(createTableStatementAnalyzer, node, context);
        }

        @Override
        public AnalyzedStatement visitCreateAnalyzer(CreateAnalyzer node, ParameterContext context) {
            return analyze(createAnalyzerStatementAnalyzer, node, context);
        }

        @Override
        public AnalyzedStatement visitCreateBlobTable(CreateBlobTable node, ParameterContext context) {
            return analyze(createBlobTableStatementAnalyzer, node, context);
        }

        @Override
        public AnalyzedStatement visitDropBlobTable(DropBlobTable node, ParameterContext context) {
            return analyze(dropBlobTableStatementAnalyzer, node, context);
        }

        @Override
        public AnalyzedStatement visitAlterBlobTable(AlterBlobTable node, ParameterContext context) {
            return analyze(alterBlobTableAnalyzer, node, context);
        }

        @Override
        public AnalyzedStatement visitRefreshStatement(RefreshStatement node, ParameterContext context) {
            return analyze(refreshTableAnalyzer, node, context);
        }

        @Override
        public AnalyzedStatement visitAlterTable(AlterTable node, ParameterContext context) {
            return analyze(alterTableAnalyzer, node, context);
        }

        @Override
        public AnalyzedStatement visitAlterTableAddColumnStatement(AlterTableAddColumn node, ParameterContext context) {
            return analyze(alterTableAddColumnAnalyzer, node, context);
        }

        @Override
        public AnalyzedStatement visitSetStatement(SetStatement node, ParameterContext context) {
            return analyze(setStatementAnalyzer, node, context);
        }

        @Override
        public AnalyzedStatement visitResetStatement(ResetStatement node, ParameterContext context) {
            return analyze(setStatementAnalyzer, node, context);
        }

        @Override
        protected AnalyzedStatement visitNode(Node node, ParameterContext context) {
            throw new UnsupportedOperationException(String.format("cannot analyze statement: '%s'", node));
        }
    }
}
