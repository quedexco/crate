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

package io.crate.metadata.pg_catalog;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RowCollectExpression;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import org.apache.lucene.util.BytesRef;

import java.util.Arrays;
import java.util.Map;

public class PgCatalogTables {

    public Supplier<Iterable<?>> pgType() {
        return Suppliers.<Iterable<?>>ofInstance(Arrays.asList(
            new PGTypeInfo(1007, "_int4", ",", 23),
            new PGTypeInfo(23, "int4", ",", 0)
        ));
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory> pgTypeExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
            .put(PgTypeTable.Columns.OID, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new RowContextCollectorExpression<PGTypeInfo, Integer>() {
                        @Override
                        public Integer value() {
                            return row.oid;
                        }
                    };
                }
            })
            .put(PgTypeTable.Columns.TYPNAME, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new RowContextCollectorExpression<PGTypeInfo, BytesRef>() {
                        @Override
                        public BytesRef value() {
                            return row.typname;
                        }
                    };
                }
            })
            .put(PgTypeTable.Columns.TYPDELIM, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new RowContextCollectorExpression<PGTypeInfo, BytesRef>() {
                        @Override
                        public BytesRef value() {
                            return row.typdelim;
                        }
                    };
                }
            })
            .put(PgTypeTable.Columns.TYPELEM, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new RowContextCollectorExpression<PGTypeInfo, Integer>() {
                        @Override
                        public Integer value() {
                            return row.typelem;
                        }
                    };
                }
            })
            .build();
    }

    private static class PGTypeInfo {
        private final int oid;
        private final BytesRef typname;
        private final BytesRef typdelim;
        private final int typelem;

        PGTypeInfo(int oid, String typname, String typdelim, int typelem) {
            this.oid = oid;
            this.typname = new BytesRef(typname);
            this.typdelim = new BytesRef(typdelim);
            this.typelem = typelem;
        }
    }
}
