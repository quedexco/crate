/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.relations;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.*;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.exceptions.AmbiguousOrderByException;
import io.crate.metadata.Path;
import io.crate.operation.operator.AndOperator;
import org.apache.commons.lang3.ArrayUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

class RelationNormalizer extends AnalyzedRelationVisitor<RelationNormalizer.Context, QueriedRelation> {

    private static final RelationNormalizer INSTANCE = new RelationNormalizer();

    public static QueriedRelation normalize(AnalyzedRelation relation, AnalysisMetaData analysisMetaData) {
        return INSTANCE.process(relation, new Context(analysisMetaData));
    }

    @Override
    public QueriedRelation visitQueriedSelectRelation(QueriedSelectRelation relation, Context context) {
        context.fields(relation.fields());
        context.querySpec = mergeQuerySpecs(context.querySpec, relation.querySpec());
        return process(relation.relation(), context);
    }

    @Override
    public QueriedRelation visitQueriedTable(QueriedTable table, Context context) {
        context.fields(table.fields());
        QuerySpec querySpec = mergeQuerySpecs(context.querySpec, table.querySpec());

        QueriedTable relation = new QueriedTable(table.tableRelation(), context.paths(), querySpec);
        relation.normalize(context.analysisMetaData);
        return relation;
    }

    @Override
    public QueriedRelation visitQueriedDocTable(QueriedDocTable table, Context context) {
        context.fields(table.fields());
        QuerySpec querySpec = mergeQuerySpecs(context.querySpec, table.querySpec());

        QueriedDocTable relation = new QueriedDocTable(table.tableRelation(), context.paths(), querySpec);
        relation.normalize(context.analysisMetaData);
        relation.analyzeWhereClause(context.analysisMetaData);
        return relation;
    }

    private static QuerySpec mergeQuerySpecs(@Nullable QuerySpec querySpec1, QuerySpec querySpec2) {
        if (querySpec1 == null) {
            return querySpec2;
        }

        QuerySpec querySpec = new QuerySpec();
        querySpec.outputs(querySpec1.outputs());
        querySpec.where(mergeWhere(querySpec1, querySpec2));
        querySpec.orderBy(mergeOrderBy(querySpec1, querySpec2));
        querySpec.offset(mergeOffset(querySpec1, querySpec2));
        querySpec.limit(mergeLimit(querySpec1, querySpec2));

        return querySpec;
    }

    private static WhereClause mergeWhere(QuerySpec querySpec1, QuerySpec querySpec2) {
        WhereClause where1 = querySpec1.where();
        WhereClause where2 = querySpec2.where();

        if (!where1.hasQuery() || where1 == WhereClause.MATCH_ALL) {
            return where2;
        } else if (!where2.hasQuery() || where2 == WhereClause.MATCH_ALL) {
            return where1;
        }

        return new WhereClause(AndOperator.join(ImmutableList.of(where1.query(), where2.query())));
    }

    @Nullable
    private static OrderBy mergeOrderBy(QuerySpec querySpec1, QuerySpec querySpec2) {
        if (!querySpec1.orderBy().isPresent()) {
            return querySpec2.orderBy().orNull();
        } else if (!querySpec2.orderBy().isPresent()) {
            return querySpec1.orderBy().orNull();
        }
        OrderBy orderBy1 = querySpec1.orderBy().get();
        OrderBy orderBy2 = querySpec2.orderBy().get();

        List<Symbol> orderBySymbols = orderBy2.orderBySymbols();
        List<Boolean> reverseFlags = new ArrayList<>(Arrays.asList(ArrayUtils.toObject(orderBy2.reverseFlags())));
        List<Boolean> nullsFirst = new ArrayList<>(Arrays.asList(orderBy2.nullsFirst()));

        for (int i = 0; i < orderBy1.orderBySymbols().size(); i++) {
            Symbol orderBySymbol = orderBy1.orderBySymbols().get(i);
            int idx = orderBySymbols.indexOf(orderBySymbol);
            if (idx == -1) {
                orderBySymbols.add(orderBySymbol);
                reverseFlags.add(orderBy1.reverseFlags()[i]);
                nullsFirst.add(orderBy1.nullsFirst()[i]);
            } else {
                if (reverseFlags.get(idx) != orderBy1.reverseFlags()[i]) {
                    throw new AmbiguousOrderByException(orderBySymbol);
                }
                if (nullsFirst.get(idx) != orderBy1.nullsFirst()[i]) {
                    throw new AmbiguousOrderByException(orderBySymbol);
                }
            }
        }

        return new OrderBy(orderBySymbols, ArrayUtils.toPrimitive(reverseFlags.toArray(new Boolean[0])), nullsFirst.toArray(new Boolean[0]));
    }

    @Nullable
    private static Integer mergeOffset(QuerySpec querySpec1, QuerySpec querySpec2) {
        return querySpec1.offset() + querySpec2.offset();
    }

    @Nullable
    private static Integer mergeLimit(QuerySpec querySpec1, QuerySpec querySpec2) {
        if (!querySpec1.limit().isPresent()) {
            return querySpec2.limit().orNull();
        } else if (!querySpec2.limit().isPresent()) {
            return querySpec1.limit().orNull();
        }

        Integer limit1 = querySpec1.limit().or(0);
        Integer limit2 = querySpec2.limit().or(0);

        return Math.min(limit1, limit2);
    }

    static class Context {
        private final AnalysisMetaData analysisMetaData;

        private QuerySpec querySpec;
        private List<Field> fields;

        public Context(AnalysisMetaData analysisMetaData) {
            this.analysisMetaData = analysisMetaData;
        }

        public void fields(List<Field> fields) {
            if (this.fields == null) {
                this.fields = fields;
            }
        }

        public Collection<? extends Path> paths() {
            return Collections2.transform(fields, new Function<Field, Path>() {
                @Override
                public Path apply(Field input) {
                    return input.path();
                }
            });
        }
    }
}
