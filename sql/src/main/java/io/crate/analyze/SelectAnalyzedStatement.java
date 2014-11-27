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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
import io.crate.metadata.ColumnIdent;
import io.crate.planner.symbol.*;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;

public class SelectAnalyzedStatement extends AnalyzedStatement implements AnalyzedRelation {

    private final List<Symbol> groupBy;
    private final OrderBy orderBy;
    private final Symbol having;
    private final Multimap<String, Symbol> selectList;
    private final Map<QualifiedName, AnalyzedRelation> sources;
    private final WhereClause whereClause;
    private final Integer limit;
    private final int offset;
    private boolean hasSysExpressions;
    private boolean hasAggregates;
    private List<Symbol> outputSymbols;

    public SelectAnalyzedStatement(Multimap<String, Symbol> selectList,
                                   Map<QualifiedName, AnalyzedRelation> sources,
                                   WhereClause whereClause,
                                   List<Symbol> groupBy,
                                   OrderBy orderBy,
                                   Symbol having,
                                   Integer limit,
                                   int offset,
                                   boolean hasSysExpressions,
                                   boolean hasAggregates) {
        super(null);
        this.selectList = selectList;
        this.sources = sources;
        this.whereClause = whereClause;
        this.groupBy = groupBy;
        this.orderBy = orderBy;
        this.having = having;
        this.limit = limit;
        this.offset = offset;
        this.hasSysExpressions = hasSysExpressions;
        this.hasAggregates = hasAggregates;
    }

    public Map<QualifiedName, AnalyzedRelation> sources() {
        return sources;
    }

    public WhereClause whereClause() {
        return whereClause;
    }

    public Integer limit() {
        return limit;
    }

    public int offset() {
        return offset;
    }

    public boolean isLimited() {
        return limit != null || offset > 0;
    }

    @Nullable
    public List<Symbol> groupBy() {
        return groupBy;
    }

    public boolean hasGroupBy() {
        return groupBy != null && groupBy.size() > 0;
    }

    @Nullable
    public Symbol havingClause() {
        return having;
    }

    @Override
    public boolean hasNoResult() {
        if (having != null && having.symbolType() == SymbolType.LITERAL) {
            Literal havingLiteral = (Literal) having;
            if (havingLiteral.value() == false) {
                return true;
            }
        }

        if (globalAggregate()) {
            return firstNonNull(limit(), 1) < 1 || offset() > 0;
        }
        return whereClause.noMatch() || limit != null && limit == 0;
    }

    private boolean globalAggregate() {
        return hasAggregates() && !hasGroupBy();
    }

    public boolean hasAggregates() {
        return hasAggregates;
    }

    @Override
    public void normalize() {
//        if (!sysExpressionsAllowed && hasSysExpressions) {
//            throw new UnsupportedOperationException("Selecting system columns from regular " +
//                    "tables is currently only supported by queries using group-by or " +
//                    "global aggregates.");
//        }
//
//        super.normalize();
//        normalizer.normalizeInplace(groupBy());
//        orderBy.normalize(normalizer);
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitSelectStatement(this, context);
    }

    public OrderBy orderBy() {
        return orderBy;
    }

    @Override
    public <C, R> R accept(RelationVisitor<C, R> visitor, C context) {
        return visitor.visitSelectAnalyzedStatement(this, context);
    }

    @Override
    public Reference getReference(ColumnIdent columnIdent) {
        throw new UnsupportedOperationException("getReference on SelectAnalyzedStatement is not implemented");
    }

    @Override
    public List<String> outputNames() {
        return Lists.newArrayList(selectList.keys());
    }

    @Override
    public List<DataType> outputTypes() {
        return Symbols.extractTypes(outputSymbols());
    }

    @Override
    public Map<String, Symbol> outputs() {
        return Maps.transformValues(selectList.asMap(), new Function<Collection<Symbol>, Symbol>() {
            @Nullable
            @Override
            public Symbol apply(@Nullable Collection<Symbol> input) {
                assert input != null;
                return Iterables.getOnlyElement(input);
            }
        });
    }

    public List<Symbol> outputSymbols() {
        if (outputSymbols == null) {
            outputSymbols = Lists.newArrayList(selectList.values());
        }
        return outputSymbols;
    }

    public boolean hasSysExpressions() {
        return hasSysExpressions;
    }
}
