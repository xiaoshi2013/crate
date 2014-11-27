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

import com.google.common.collect.ImmutableList;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.Input;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnalyzedStatementTest {

    private ReferenceInfos referenceInfos;
    private ReferenceResolver resolver;
    private Functions functions;
    private static final TableIdent TEST_TABLE_IDENT = new TableIdent(null, "test1");
    private static final FunctionInfo TEST_FUNCTION_INFO = new FunctionInfo(
            new FunctionIdent("abs", ImmutableList.<DataType>of(DataTypes.DOUBLE)), DataTypes.DOUBLE);
    private static final TableInfo userTableInfo = TestingTableInfo.builder(TEST_TABLE_IDENT, RowGranularity.DOC, new Routing())
            .add("id", DataTypes.LONG, null)
            .add("name", DataTypes.STRING, null)
            .add("d", DataTypes.DOUBLE, null)
            .add("dyn_empty", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
            .add("dyn", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
            .add("dyn", DataTypes.DOUBLE, ImmutableList.of("d"))
            .add("dyn", DataTypes.OBJECT, ImmutableList.of("inner_strict"), ColumnPolicy.STRICT)
            .add("dyn", DataTypes.DOUBLE, ImmutableList.of("inner_strict", "double"))
            .add("strict", DataTypes.OBJECT, null, ColumnPolicy.STRICT)
            .add("strict", DataTypes.DOUBLE, ImmutableList.of("inner_d"))
            .add("ignored", DataTypes.OBJECT, null, ColumnPolicy.IGNORED)
            .addPrimaryKey("id")
            .clusteredBy("id")
            .build();


    static class AbsFunction extends Scalar<Double, Number> {

        @Override
        public Double evaluate(Input<Number>... args) {
            if (args == null || args.length == 0) {
                return 0.0d;
            }
            return Math.abs((args[0].value()).doubleValue());
        }

        @Override
        public FunctionInfo info() {
            return TEST_FUNCTION_INFO;
        }

        @Override
        public Symbol normalizeSymbol(Function symbol) {
            if (symbol.arguments().size() == 1
                    && symbol.arguments().get(0).symbolType() == SymbolType.LITERAL
                    && ((Literal)symbol.arguments().get(0)).valueType().equals(DataTypes.DOUBLE)) {
                return Literal.newLiteral(evaluate(((Input)symbol.arguments().get(0))));
            }
            return symbol;
        }
    }

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void bindFunctions() {
            super.bindFunctions();
            functionBinder.addBinding(TEST_FUNCTION_INFO.ident()).toInstance(new AbsFunction());
        }

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(TEST_TABLE_IDENT.name())).thenReturn(userTableInfo);
            schemaBinder.addBinding(DocSchemaInfo.NAME).toInstance(schemaInfo);
        }
    }



    @Before
    public void prepare() {
        Injector injector = new ModulesBuilder()
                .add(new TestMetaDataModule())
                .createInjector();
        referenceInfos = injector.getInstance(ReferenceInfos.class);
        resolver = injector.getInstance(ReferenceResolver.class);
        functions = injector.getInstance(Functions.class);
    }

}
