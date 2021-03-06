package org.greenplum.pxf.plugins.ignite;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.io.DataType;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Validate SQL string generated by the {@link IgnitePartitionFragmenter#buildFragmenterSql} method
 * and the {@link WhereSQLBuilder#buildWhereSQL} method.
 */
public class SqlBuilderTest {
    @Test
    public void testIdFilter() throws Exception {
        prepareConstruction();
        when(context.hasFilter()).thenReturn(true);
        when(context.getFilterString()).thenReturn("a0c20s1d1o5");  // id=1

        WhereSQLBuilder builder = new WhereSQLBuilder(context);
        assertEquals("id=1", builder.buildWhereSQL());
    }

    @Test
    public void testDateAndAmtFilter() throws Exception {
        prepareConstruction();
        when(context.hasFilter()).thenReturn(true);
        // cdate>'2008-02-01' and cdate<'2008-12-01' and amt > 1200
        when(context.getFilterString()).thenReturn("a1c25s10d2008-02-01o2a1c25s10d2008-12-01o1l0a2c20s4d1200o2l0");

        WhereSQLBuilder builder = new WhereSQLBuilder(context);
        assertEquals("cdate>'2008-02-01' AND cdate<'2008-12-01' AND amt>1200"
                , builder.buildWhereSQL());
    }

    @Test
    public void testUnsupportedOperationFilter() throws Exception {
        prepareConstruction();
        when(context.hasFilter()).thenReturn(true);
        // grade like 'bad'
        when(context.getFilterString()).thenReturn("a3c25s3dbado7");

        WhereSQLBuilder builder = new WhereSQLBuilder(context);
        assertEquals(null, builder.buildWhereSQL());
    }

    @Test
    public void testUnsupportedLogicalFilter() throws Exception {
        prepareConstruction();
        when(context.hasFilter()).thenReturn(true);
        // cdate>'2008-02-01' or amt < 1200
        when(context.getFilterString()).thenReturn("a1c25s10d2008-02-01o2a2c20s4d1200o2l1");

        WhereSQLBuilder builder = new WhereSQLBuilder(context);
        assertEquals(null, builder.buildWhereSQL());
    }

    @Test
    public void testDatePartition() throws Exception {
        prepareConstruction();
        when(context.hasFilter()).thenReturn(false);
        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn("2008-01-01:2009-01-01");
        when(context.getOption("INTERVAL")).thenReturn("2:month");
        IgnitePartitionFragmenter fragment = new IgnitePartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(6, fragments.size());

        // Partition 1: cdate>=2008-01-01 and cdate<2008-03-01
        when(context.getFragmentMetadata()).thenReturn(fragments.get(0).getMetadata());
        StringBuilder sb = new StringBuilder(ORIGINAL_SQL);
        IgnitePartitionFragmenter.buildFragmenterSql(context, sb);
        assertEquals(ORIGINAL_SQL + " WHERE cdate>='2008-01-01' AND cdate<'2008-03-01'", sb.toString());
    }

    @Test
    public void testFilterAndPartition() throws Exception {
        prepareConstruction();
        when(context.hasFilter()).thenReturn(true);
        when(context.getFilterString()).thenReturn("a0c20s1d5o2"); //id>5
        when(context.getOption("PARTITION_BY")).thenReturn("grade:enum");
        when(context.getOption("RANGE")).thenReturn("excellent:good:general:bad");

        WhereSQLBuilder builder = new WhereSQLBuilder(context);
        String whereSql = builder.buildWhereSQL();
        assertEquals("id>5", whereSql);

        IgnitePartitionFragmenter fragment = new IgnitePartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        // Partition 1: id>5 and grade='excellent'
        when(context.getFragmentMetadata()).thenReturn(fragments.get(0).getMetadata());

        String filterSql = ORIGINAL_SQL + " WHERE " + whereSql;
        StringBuilder sb = new StringBuilder(filterSql);
        IgnitePartitionFragmenter.buildFragmenterSql(context, sb);
        assertEquals(filterSql + " AND grade='excellent'", sb.toString());
    }

    @Test
    public void testNoPartition() throws Exception {
        prepareConstruction();
        when(context.hasFilter()).thenReturn(false);
        IgnitePartitionFragmenter fragment = new IgnitePartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(1, fragments.size());

        when(context.getFragmentMetadata()).thenReturn(fragments.get(0).getMetadata());

        StringBuilder sb = new StringBuilder(ORIGINAL_SQL);
        IgnitePartitionFragmenter.buildFragmenterSql(context, sb);
        assertEquals(ORIGINAL_SQL, sb.toString());
    }

    private void prepareConstruction() throws Exception {
        context = mock(RequestContext.class);
        when(context.getDataSource()).thenReturn("sales");

        ArrayList<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columns.add(new ColumnDescriptor("cdate", DataType.DATE.getOID(), 1, "date", null));
        columns.add(new ColumnDescriptor("amt", DataType.FLOAT8.getOID(), 2, "float8", null));
        columns.add(new ColumnDescriptor("grade", DataType.TEXT.getOID(), 3, "text", null));
        when(context.getTupleDescription()).thenReturn(columns);
        when(context.getColumn(0)).thenReturn(columns.get(0));
        when(context.getColumn(1)).thenReturn(columns.get(1));
        when(context.getColumn(2)).thenReturn(columns.get(2));
        when(context.getColumn(3)).thenReturn(columns.get(3));
    }


    private static final String ORIGINAL_SQL = "SELECT * FROM sales";
    private RequestContext context;
}
