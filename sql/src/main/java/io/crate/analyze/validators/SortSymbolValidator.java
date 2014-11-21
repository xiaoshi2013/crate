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

package io.crate.analyze.validators;

import io.crate.metadata.FunctionInfo;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.*;
import io.crate.types.DataTypes;

import java.util.Locale;

/**
 * validate that sortSymbols don't contain partition by columns
 */
public class SortSymbolValidator {

    private static final InnerVisitor VISITOR = new InnerVisitor();

    public static void validate(Symbol symbol) {
        VISITOR.process(symbol, null);
    }

    static class SortContext {
        private final TableInfo tableInfo;
        private boolean inFunction;
        public SortContext(TableInfo tableInfo) {
            this.tableInfo = tableInfo;
            this.inFunction = false;
        }
    }

    private static class InnerVisitor extends SymbolVisitor<SortSymbolValidator.SortContext, Void> {

        @Override
        public Void visitFunction(Function symbol, SortContext context) {
            try {
                if (context.inFunction == false
                        && !DataTypes.PRIMITIVE_TYPES.contains(symbol.valueType())) {
                    throw new UnsupportedOperationException(
                            String.format(Locale.ENGLISH,
                                    "Cannot ORDER BY '%s': invalid return type '%s'.",
                                    SymbolFormatter.format(symbol),
                                    symbol.valueType())
                    );
                }

                if (symbol.info().type() == FunctionInfo.Type.PREDICATE) {
                    throw new UnsupportedOperationException(String.format(
                            "%s predicate cannot be used in an ORDER BY clause", symbol.info().ident().name()));
                }

                context.inFunction = true;
                for (Symbol arg : symbol.arguments()) {
                    process(arg, context);
                }
            } finally {
                context.inFunction = false;
            }
            return null;
        }

        @Override
        public Void visitReference(Reference symbol, SortContext context) {
            if (context.tableInfo.partitionedBy().contains(symbol.info().ident().columnIdent())) {
                throw new UnsupportedOperationException(
                        SymbolFormatter.format(
                                "cannot use partitioned column %s in ORDER BY clause",
                                symbol));
            }
            // if we are in a function, we do not need to check the data type.
            // the function will do that for us.
            if (!context.inFunction && !DataTypes.PRIMITIVE_TYPES.contains(symbol.info().type())) {
                throw new UnsupportedOperationException(
                        String.format(Locale.ENGLISH,
                                "Cannot ORDER BY '%s': invalid data type '%s'.",
                                SymbolFormatter.format(symbol),
                                symbol.valueType())
                );
            } else if (symbol.info().indexType() == ReferenceInfo.IndexType.ANALYZED) {
                throw new UnsupportedOperationException(
                        String.format("Cannot ORDER BY '%s': sorting on analyzed/fulltext columns is not possible",
                                SymbolFormatter.format(symbol)));
            } else if (symbol.info().indexType() == ReferenceInfo.IndexType.NO) {
                throw new UnsupportedOperationException(
                        String.format("Cannot ORDER BY '%s': sorting on non-indexed columns is not possible",
                                SymbolFormatter.format(symbol)));
            }
            return null;
        }

        @Override
        public Void visitDynamicReference(DynamicReference symbol, SortContext context) {
            throw new UnsupportedOperationException(
                    SymbolFormatter.format("Cannot order by \"%s\". The column doesn't exist.", symbol));
        }

        @Override
        public Void visitSymbol(Symbol symbol, SortContext context) {
            return null;
        }
    }
}
