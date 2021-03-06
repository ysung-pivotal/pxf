package org.greenplum.pxf.plugins.hdfs.utilities;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor("org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities")
@PrepareForTest({HdfsUtilities.class, ReflectionUtils.class})
public class HdfsUtilitiesTest {

    private Configuration conf;
    private CompressionCodecFactory factory;

    @Before
    public void SetupCompressionFactory() {
        conf = PowerMockito.mock(Configuration.class);
        factory = mock(CompressionCodecFactory.class);
        Logger LOG = mock(Logger.class);
        Whitebox.setInternalState(HdfsUtilities.class, LOG);
    }

    @Test
    public void getCodecNoName() {

        Configuration conf = new Configuration();
        String name = "some.bad.codec";

        try {
            HdfsUtilities.getCodec(conf, name);
            fail("function should fail with bad codec name " + name);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Compression codec " + name + " was not found.");
        }
    }

    @Test
    public void getCodecNoConf() {

        Configuration conf = null;
        String name = "org.apache.hadoop.io.compress.GzipCodec";

        try {
            HdfsUtilities.getCodec(conf, name);
            fail("function should fail with when conf is null");
        } catch (NullPointerException e) {
            assertTrue(true);
        }
    }

    @Test
    public void getCodecGzip() {

        Configuration conf = new Configuration();
        String name = "org.apache.hadoop.io.compress.GzipCodec";

        PowerMockito.mockStatic(ReflectionUtils.class);
        GzipCodec gzipCodec = mock(GzipCodec.class);

        when(ReflectionUtils.newInstance(GzipCodec.class, conf)).thenReturn(gzipCodec);

        CompressionCodec codec = HdfsUtilities.getCodec(conf, name);
        assertNotNull(codec);
        assertEquals(codec, gzipCodec);
    }

    @Test
    public void isThreadSafe() {

        testIsThreadSafe(
                "readable compression, no compression - thread safe",
                "/some/path/without.compression",
                null, null,
                true);

        testIsThreadSafe(
                "readable compression, gzip compression - thread safe",
                "/some/compressed/path.gz",
                null, new GzipCodec(),
                true);

        testIsThreadSafe(
                "readable compression, bzip2 compression - not thread safe",
                "/some/path/with/bzip2.bz2",
                null, new BZip2Codec(),
                false);

        testIsThreadSafe(
                "writable compression, no compression codec - thread safe",
                "/some/path",
                null, null,
                true);

        testIsThreadSafe(
                "writable compression, some compression codec - thread safe",
                "/some/path",
                "I.am.a.nice.codec", new NotSoNiceCodec(),
                true);

        testIsThreadSafe(
                "writable compression, compression codec bzip2 - not thread safe",
                "/some/path",
                "org.apache.hadoop.io.compress.BZip2Codec", new BZip2Codec(),
                false);
    }

    private void testIsThreadSafe(String testDescription, String path, String codecStr,
                                  CompressionCodec codec, boolean expectedResult) {
        prepareDataForIsThreadSafe(path, codecStr, codec);

        boolean result = HdfsUtilities.isThreadSafe(conf, path, codecStr);
        assertEquals(testDescription, expectedResult, result);
    }

    private void prepareDataForIsThreadSafe(String dataDir, String codecStr, CompressionCodec codec) {
        try {
            PowerMockito.whenNew(Configuration.class).withNoArguments().thenReturn(conf);
        } catch (Exception e) {
            fail("new Configuration mocking failed");
        }

        if (codecStr == null) {
            when(factory.getCodec(new Path(dataDir))).thenReturn(codec);
        } else {
            PowerMockito.stub(PowerMockito.method(HdfsUtilities.class, "getCodecClass")).toReturn(codec.getClass());
        }
    }

    @Test
    public void isSplittableCodec() {

        testIsSplittableCodec("no codec - splittable",
                "some/innocent.file", null, true);
        testIsSplittableCodec("gzip codec - not splittable",
                "/gzip.gz", new GzipCodec(), false);
        testIsSplittableCodec("default codec - not splittable",
                "/default.deflate", new DefaultCodec(), false);
        testIsSplittableCodec("bzip2 codec - splittable",
                "bzip2.bz2", new BZip2Codec(), true);
    }

    private void testIsSplittableCodec(String description,
                                       String pathName, CompressionCodec codec, boolean expected) {
        Path path = new Path(pathName);
        when(factory.getCodec(path)).thenReturn(codec);

        boolean result = HdfsUtilities.isSplittableCodec(factory, path);
        assertEquals(description, result, expected);
    }

    @Test
    public void testToString() {
        List<OneField> oneFields = Arrays.asList(new OneField(1, "uno"), new OneField(2, "dos"), new OneField(3, "tres"));

        assertEquals("uno!dos!tres", HdfsUtilities.toString(oneFields, "!"));
        assertEquals("uno", HdfsUtilities.toString(Collections.singletonList(oneFields.get(0)), "!"));
        assertEquals("", HdfsUtilities.toString(Collections.emptyList(), "!"));
    }

    @Test
    public void testParseFileSplit() throws Exception {
        RequestContext requestContext = mock(RequestContext.class);
        when(requestContext.getDataSource()).thenReturn("/abc/path/to/data/source");
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(bas);
        os.writeLong(10);
        os.writeLong(100);
        os.writeObject(new String[] { "hostname" });
        os.close();

        when(requestContext.getFragmentMetadata()).thenReturn(bas.toByteArray());
        FileSplit fileSplit = HdfsUtilities.parseFileSplit(requestContext);
        assertEquals(fileSplit.getStart(), 10);
        assertEquals(fileSplit.getLength(), 100);
        assertEquals(fileSplit.getPath().toString(), "/abc/path/to/data/source");
    }
}
