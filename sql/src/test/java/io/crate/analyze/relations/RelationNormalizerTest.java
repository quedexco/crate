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

import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.BaseAnalyzerTest;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.testing.MockedClusterServiceModule;
import org.elasticsearch.common.inject.Module;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.T3.META_DATA_MODULE;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.instanceOf;

public class RelationNormalizerTest extends BaseAnalyzerTest {

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
            new MockedClusterServiceModule(),
            META_DATA_MODULE,
            new ScalarFunctionModule(),
            new OperatorModule()
        ));
        return modules;
    }

    private QueriedRelation normalize(String stmt) {
        SelectAnalyzedStatement statement = analyze(stmt);
        QueriedRelation relation = statement.relation();
        assertThat(relation, instanceOf(QueriedDocTable.class));
        return relation;
    }

    @Test
    public void testOrderByPushDown() throws Exception {
        QueriedRelation relation = normalize(
            "select * from (select * from t1 limit 10 offset 5) as alias_sub order by x");
        assertThat(relation.querySpec(), isSQL("SELECT doc.t1.a, doc.t1.x, doc.t1.i ORDER BY doc.t1.x LIMIT 10 OFFSET 5"));
    }

    @Test
    public void testOrderByMerge() throws Exception {
        QueriedRelation relation = normalize(
            "select * from (select * from t1 order by a) as alias_sub order by x");
        assertThat(relation.querySpec(), isSQL("SELECT doc.t1.a, doc.t1.x, doc.t1.i ORDER BY doc.t1.a, doc.t1.x"));
    }

    @Test
    public void testLimitOffsetMerge() throws Exception {
        QueriedRelation relation = normalize(
            "select * from (select * from t1 limit 10 offset 5) as alias_sub limit 5 offset 2");
        assertThat(relation.querySpec(), isSQL("SELECT doc.t1.a, doc.t1.x, doc.t1.i LIMIT 5 OFFSET 7"));
    }

    @Test
    public void testNestedMerge() throws Exception {
        QueriedRelation relation = normalize(
            "select a from (select a from (select a, x from t1 order by i) as alias_sub1 order by x) as alias_sub2 order by a");
        assertThat(relation.querySpec(), isSQL("SELECT doc.t1.a ORDER BY doc.t1.i, doc.t1.x, doc.t1.a"));
    }

    @Test
    public void testOutputsMerge() throws Exception {
        QueriedRelation relation = normalize(
            "select s + 1 from (select i + x as s from t1) as alias_sub1");
        assertThat(relation.querySpec(), isSQL("SELECT add(add(doc.t1.i, doc.t1.x), 1)"));
    }

    @Test
    public void testWhereMerge() throws Exception {
        QueriedRelation relation = normalize(
            "select x + i from (select x, i from t1 where a = 'a') as alias_sub1 where i > 10");
        assertThat(relation.querySpec(), isSQL("SELECT add(doc.t1.x, doc.t1.i) WHERE ((doc.t1.i > 10) AND (doc.t1.a = 'a'))"));
    }
}
