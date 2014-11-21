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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
import io.crate.metadata.ColumnIdent;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Symbols;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SelectAnalyzedStatement implements AnalyzedStatement, AnalyzedRelation {

    private final Multimap<String, Symbol> selectList;
    private final Map<QualifiedName, AnalyzedRelation> sources;
    private final WhereClause whereClause;
    private final List<Symbol> groupBy;
    private final OrderBy orderBy;
    private final Symbol having;
    private final Integer limit;
    private final int offset;

    public SelectAnalyzedStatement(Multimap<String, Symbol> selectList,
                                   Map<QualifiedName, AnalyzedRelation> sources,
                                   WhereClause whereClause,
                                   List<Symbol> groupBy,
                                   OrderBy orderBy,
                                   Symbol having,
                                   Integer limit,
                                   int offset) {
        this.selectList = selectList;
        this.sources = sources;
        this.whereClause = whereClause;
        this.groupBy = groupBy;
        this.orderBy = orderBy;
        this.having = having;
        this.limit = limit;
        this.offset = offset;
    }

    public OrderBy orderBy() {
        return orderBy;
    }

    public List<Symbol> groupBy() {
        return groupBy;
    }

    public boolean isLimited() {
        return limit != null || offset > 0;
    }

    @Override
    public Collection<String> outputNames() {
        return selectList.keys();
    }

    @Override
    public Collection<DataType> outputTypes() {
        return Symbols.extractTypes(selectList.values());
    }

    @Override
    public boolean expectsAffectedRows() {
        return false;
    }

    @Override
    public void normalize() {
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitSelectStatement(this, context);
    }

    @Override
    public Reference getReference(ColumnIdent columnIdent) {
        return null;
    }

    @Override
    public Map<String, Symbol> outputs() {
        return Maps.transformValues(selectList.asMap(), new Function<Collection<Symbol>, Symbol>() {
            @Nullable
            @Override
            public Symbol apply(Collection<Symbol> input) {
                return Iterables.getOnlyElement(input);
            }
        });
    }

    @Override
    public <C, R> R accept(RelationVisitor<C, R> visitor, C context) {
        return visitor.visitSelectAnalyzedStatement(this, context);
    }
}
