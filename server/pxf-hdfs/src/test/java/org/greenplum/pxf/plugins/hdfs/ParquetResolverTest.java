package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.pig.convert.DecimalUtils;
import org.apache.parquet.schema.*;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class ParquetResolverTest {

    private ParquetResolver resolver;
    private RequestContext context;
    private MessageType schema;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        resolver = new ParquetResolver();
        context = new RequestContext();
        schema = new MessageType("test");
        context.setMetadata(schema);
    }

    @Test
    public void testInitialize() {
        resolver.initialize(context);
    }

    @Test
    public void testGetFields_FailsOnMissingSchema() {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("No schema detected in request context");

        context.setMetadata(null);
        resolver.initialize(context);
        resolver.getFields(new OneRow());
    }

    @Test
    public void testSetFields_FailsOnMissingSchema() throws IOException {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("No schema detected in request context");

        context.setMetadata(null);
        resolver.initialize(context);
        resolver.setFields(new ArrayList<>());
    }

    @Test
    public void testSetFields_Primitive() throws IOException {
        schema = getParquetSchemaForPrimitiveTypes(Type.Repetition.OPTIONAL, false);
        // schema has changed, set metadata again
        context.setMetadata(schema);
        resolver.initialize(context);
        List<OneField> fields = new ArrayList<>();
        fields.add(new OneField(DataType.TEXT.getOID(), "row1"));
        fields.add(new OneField(DataType.TEXT.getOID(), "s_6"));
        fields.add(new OneField(DataType.INTEGER.getOID(), 1));
        fields.add(new OneField(DataType.FLOAT8.getOID(), 6.0d));
        fields.add(new OneField(DataType.NUMERIC.getOID(), "1.234560000000000000"));
        fields.add(new OneField(DataType.TIMESTAMP.getOID(), "2013-07-13 21:00:05"));
        fields.add(new OneField(DataType.REAL.getOID(), 7.7f));
        fields.add(new OneField(DataType.BIGINT.getOID(), 23456789l));
        fields.add(new OneField(DataType.BOOLEAN.getOID(), false));
        fields.add(new OneField(DataType.SMALLINT.getOID(), (short) 1));
        fields.add(new OneField(DataType.SMALLINT.getOID(), (short) 10));
        fields.add(new OneField(DataType.TEXT.getOID(), "abcd"));
        fields.add(new OneField(DataType.TEXT.getOID(), "abc"));
        fields.add(new OneField(DataType.BYTEA.getOID(), new byte[]{(byte) 49}));
        OneRow row = resolver.setFields(fields);
        assertNotNull(row);
        Object data = row.getData();
        assertNotNull(data);
        assertTrue(data instanceof Group);
        Group group = (Group) data;

        // assert column values
        assertEquals("row1", group.getString(0,0));
        assertEquals("s_6", group.getString(1,0));
        assertEquals(1, group.getInteger(2,0));
        assertEquals(6.0d, group.getDouble(3,0), 0d);
        assertEquals(BigDecimal.valueOf(1234560000000000000L, 18),
                DecimalUtils.binaryToDecimal(group.getBinary(4,0), 19, 18));

        NanoTime nanoTime = NanoTime.fromBinary(group.getInt96(5,0));
        assertEquals(2456487, nanoTime.getJulianDay()); // 13 Jul 2013 in Julian days
        assertEquals((21*60*60+5L) * 1000 * 1000 * 1000, nanoTime.getTimeOfDayNanos()); // 21:00:05 time
        assertEquals(7.7f, group.getFloat(6,0), 0f);
        assertEquals(23456789L, group.getLong(7,0));
        assertEquals(false, group.getBoolean(8,0));
        assertEquals(1, group.getInteger(9,0));
        assertEquals(10, group.getInteger(10,0));
        assertEquals("abcd", group.getString(11,0));
        assertEquals("abc", group.getString(12,0));
        assertArrayEquals(new byte[]{(byte) 49}, group.getBinary(13,0).getBytes());

        // assert value repetition count
        for (int i=0; i<14; i++) {
            assertEquals(1, group.getFieldRepetitionCount(i));
        }
    }

    @Test
    public void testSetFields_Primitive_Nulls() throws IOException {
        schema = getParquetSchemaForPrimitiveTypes(Type.Repetition.OPTIONAL,false);
        // schema has changed, set metadata again
        context.setMetadata(schema);
        resolver.initialize(context);
        List<OneField> fields = new ArrayList<>();
        fields.add(new OneField(DataType.TEXT.getOID(), null));
        fields.add(new OneField(DataType.TEXT.getOID(), null));
        fields.add(new OneField(DataType.INTEGER.getOID(), null));
        fields.add(new OneField(DataType.FLOAT8.getOID(), null));
        fields.add(new OneField(DataType.NUMERIC.getOID(), null));
        fields.add(new OneField(DataType.TIMESTAMP.getOID(), null));
        fields.add(new OneField(DataType.REAL.getOID(), null));
        fields.add(new OneField(DataType.BIGINT.getOID(), null));
        fields.add(new OneField(DataType.BOOLEAN.getOID(), null));
        fields.add(new OneField(DataType.SMALLINT.getOID(), null));
        fields.add(new OneField(DataType.SMALLINT.getOID(), null));
        fields.add(new OneField(DataType.TEXT.getOID(), null));
        fields.add(new OneField(DataType.TEXT.getOID(), null));
        fields.add(new OneField(DataType.BYTEA.getOID(), null));
        OneRow row = resolver.setFields(fields);
        assertNotNull(row);
        Object data = row.getData();
        assertNotNull(data);
        assertTrue(data instanceof Group);
        Group group = (Group) data;
        // assert value repetition count
        for (int i=0; i<14; i++) {
            assertEquals(0, group.getFieldRepetitionCount(i));
        }
    }

    @Test
    public void testGetFields_Primitive_EmptySchema() throws IOException {
        resolver.initialize(context);

        List<Group> groups = readParquetFile("primitive_types.parquet", 25);
        OneRow row1 = new OneRow(groups.get(0)); // get row 1
        List<OneField> fields = resolver.getFields(row1);
        assertTrue(fields.isEmpty());
    }

    @Test
    public void testGetFields_Primitive() throws IOException {
        schema = getParquetSchemaForPrimitiveTypes(Type.Repetition.OPTIONAL,true);
        // schema has changed, set metadata again
        context.setMetadata(schema);
        resolver.initialize(context);

        List<Group> groups = readParquetFile("primitive_types.parquet", 25);
        assertEquals(25, groups.size());

        List<OneField> fields = assertRow(groups, 0, 14);
        //s1 : "row1" : TEXT
        assertField(fields, 0, "row1", DataType.TEXT);
        assertField(fields, 1, "s_6", DataType.TEXT);
        assertField(fields, 2, 1, DataType.INTEGER);
        assertField(fields, 3, 6.0d, DataType.FLOAT8);
        assertField(fields, 4, BigDecimal.valueOf(1234560000000000000L, 18), DataType.NUMERIC);
        assertField(fields, 5, java.sql.Timestamp.from(ZonedDateTime.parse("2013-07-13T21:00:05-07:00").toInstant()), DataType.TIMESTAMP);
        assertField(fields, 6, 7.7f, DataType.REAL);
        assertField(fields, 7, 23456789l, DataType.BIGINT);
        assertField(fields, 8, false, DataType.BOOLEAN);
        assertField(fields, 9, (short) 1, DataType.SMALLINT);
        assertField(fields, 10, (short) 10, DataType.SMALLINT);
        assertField(fields, 11, "abcd", DataType.TEXT);
        assertField(fields, 12, "abc", DataType.TEXT);
        assertField(fields, 13, new byte[]{(byte) 49}, DataType.BYTEA); // 49 is the ascii code for '1'

        // test nulls
        fields = assertRow(groups, 11, 14);
        assertField(fields, 1, null, DataType.TEXT);
        fields = assertRow(groups, 12, 14);
        assertField(fields, 2, null, DataType.INTEGER);
        fields = assertRow(groups, 13, 14);
        assertField(fields, 3, null, DataType.FLOAT8);
        fields = assertRow(groups, 14, 14);
        assertField(fields, 4, null, DataType.NUMERIC);
        fields = assertRow(groups, 15, 14);
        assertField(fields, 5, null, DataType.TIMESTAMP);
        fields = assertRow(groups, 16, 14);
        assertField(fields, 6, null, DataType.REAL);
        fields = assertRow(groups, 17, 14);
        assertField(fields, 7, null, DataType.BIGINT);
        fields = assertRow(groups, 18, 14);
        assertField(fields, 8, null, DataType.BOOLEAN);
        fields = assertRow(groups, 19, 14);
        assertField(fields, 9, null, DataType.SMALLINT);
        fields = assertRow(groups, 20, 14);
        assertField(fields, 10, null, DataType.SMALLINT);
        fields = assertRow(groups, 22, 14);
        assertField(fields, 11, null, DataType.TEXT);
        fields = assertRow(groups, 23, 14);
        assertField(fields, 12, null, DataType.TEXT);
        fields = assertRow(groups, 24, 14);
        assertField(fields, 13, null, DataType.BYTEA);
    }

    @Test
    public void testGetFields_Primitive_RepeatedString() throws IOException {
        List<Type> columns = new ArrayList<>();
        columns.add(new PrimitiveType(Type.Repetition.REPEATED, PrimitiveTypeName.BINARY, "myString", OriginalType.UTF8));
        schema = new MessageType("TestProtobuf.StringArray", columns);
        context.setMetadata(schema);
        resolver.initialize(context);

        List<Group> groups = readParquetFile("proto-repeated-string.parquet", 3);
        List<OneField> fields;

        // row 0
        fields = assertRow(groups, 0, 1);
        assertEquals(DataType.TEXT.getOID(), fields.get(0).type);
        assertEquals("[\"hello\",\"world\"]", fields.get(0).val);

        // row 1
        fields = assertRow(groups, 1, 1);
        assertEquals(DataType.TEXT.getOID(), fields.get(0).type);
        assertEquals("[\"good\",\"bye\"]", fields.get(0).val);

        // row 2
        fields = assertRow(groups, 2, 1);
        assertEquals(DataType.TEXT.getOID(), fields.get(0).type);
        assertEquals("[\"one\",\"two\",\"three\"]", fields.get(0).val);

    }

    @Test
    public void testGetFields_Primitive_Repeated_Synthetic() throws IOException {
        // this test does not read the actual Parquet file, but rather construct Group object synthetically
        schema = getParquetSchemaForPrimitiveTypes(Type.Repetition.REPEATED,true);
        // schema has changed, set metadata again
        context.setMetadata(schema);
        resolver.initialize(context);

        Group group = new SimpleGroup(schema);

        /*
                    "t1    TEXT",
            "t2    TEXT",
            "num1  INTEGER",
            "dub1  DOUBLE PRECISION",
            "dec1  NUMERIC",
            "tm    TIMESTAMP",
            "r     REAL",
            "bg    BIGINT",
            "b     BOOLEAN",
            "tn    SMALLINT",
            "sml   SMALLINT",
            "vc1   VARCHAR(5)",
            "c1    CHAR(3)",
            "bin   BYTEA"
    };

SELECT t1, t2, num1, dub1, dec1, tm, r, bg, b, tn, sml, vc1, c1, bin FROM parquet_view ORDER BY t1;
          t1          |  t2  | num1 | dub1 |             dec1             |         tm          |  r   |    bg    | b | tn | sml  |  vc1  | c1  | bin
----------------------+------+------+------+------------------------------+---------------------+------+----------+---+----+------+-------+-----+-----
 row1                 | s_6  |    1 |    6 |         1.234560000000000000 | 2013-07-13 21:00:05 |  7.7 | 23456789 | f |  1 |   10 | abcd  | abc | 1
         */
        group.add(0, "row1-1");
        group.add(0, "row1-2");

        // leave column 1 (t2) unset as part fo the test

        group.add(2, 1);
        group.add(2, 2);
        group.add(2, 3);

        group.add(3, 6.0d);
        group.add(3, -16.34d);

        BigDecimal value = new BigDecimal((String) "12345678.9012345987654321");
        byte fillByte = (byte) (value.signum() < 0 ? 0xFF : 0x00);
        byte[] unscaled = value.unscaledValue().toByteArray();
        byte[] bytes = new byte[16];
        int offset = bytes.length - unscaled.length;
        for (int i = 0; i < bytes.length; i += 1) {
                bytes[i] = (i < offset) ? fillByte : unscaled[i - offset];
        }
        group.add(4, Binary.fromReusedByteArray(bytes));

        group.add(5, ParquetTypeConverter.getBinary(1549317584246L));
        group.add(5, ParquetTypeConverter.getBinary(-123456789L));

        List<Group> groups = new ArrayList<>();
        groups.add(group);
        List<OneField> fields = assertRow(groups, 0, 14);
        //s1 : "row1" : TEXT
        assertField(fields, 0, "[\"row1-1\",\"row1-2\"]", DataType.TEXT);
        assertField(fields, 1, "[]", DataType.TEXT);
        assertField(fields, 2, "[1,2,3]", DataType.TEXT);
        assertField(fields, 3, "[6.0,-16.34]", DataType.TEXT);
        assertField(fields, 4, "[123456.789012345987654321]", DataType.TEXT); // scale fixed to 18 in schema
        //assertField(fields, 5, "[1549317584246,-123456789]", DataType.TEXT);
//        assertField(fields, 6, 7.7f, DataType.REAL);
//        assertField(fields, 7, 23456789l, DataType.BIGINT);
//        assertField(fields, 8, false, DataType.BOOLEAN);
//        assertField(fields, 9, (short) 1, DataType.SMALLINT);
//        assertField(fields, 10, (short) 10, DataType.SMALLINT);
//        assertField(fields, 11, "abcd", DataType.TEXT);
//        assertField(fields, 12, "abc", DataType.TEXT);
//        assertField(fields, 13, new byte[]{(byte) 49}, DataType.BYTEA); // 49 is the ascii code for '1'

    }

    @Test
    public void testGetFields_Primitive_RepeatedInt() throws IOException {
        List<Type> columns = new ArrayList<>();
        columns.add(new PrimitiveType(Type.Repetition.REPEATED, PrimitiveTypeName.INT32, "repeatedInt"));
        schema = new MessageType("TestProtobuf.RepeatedIntMessage", columns);
        context.setMetadata(schema);
        resolver.initialize(context);

        List<Group> groups = readParquetFile("old-repeated-int.parquet", 1);
        List<OneField> fields = assertRow(groups, 0, 1);
        assertEquals(DataType.TEXT.getOID(), fields.get(0).type);
        assertEquals("[1,2,3]", fields.get(0).val);

    }

    private List<OneField> assertRow(List<Group> groups, int desiredRow, int numFields) {
        OneRow row = new OneRow(groups.get(desiredRow)); // get row
        List<OneField> fields = resolver.getFields(row);
        assertEquals(numFields, fields.size());
        return fields;
    }

    private void assertField(List<OneField> fields, int index, Object value, DataType type) {
        assertEquals(type.getOID(), fields.get(index).type);
        if (type == DataType.BYTEA) {
            assertArrayEquals((byte[]) value, (byte[]) fields.get(index).val);
        } else {
            assertEquals(value, fields.get(index).val);
        }

    }

    private MessageType getParquetSchemaForPrimitiveTypes(Type.Repetition repetition, boolean readCase) {
        List<Type> fields = new ArrayList<>();

        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.BINARY, "s1", OriginalType.UTF8));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.BINARY, "s2", OriginalType.UTF8));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.INT32, "n1", null));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.DOUBLE, "d1", null));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 16, "dc1", OriginalType.DECIMAL, new DecimalMetadata(38, 18), null));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.INT96, "tm", null));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.FLOAT, "f", null));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.INT64, "bg", null));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.BOOLEAN, "b", null));

        // GPDB only has int16 and not int8 type, so for write tiny numbers int8 are still treated as shorts in16
        OriginalType tinyType = readCase ? OriginalType.INT_8 : OriginalType.INT_16;
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.INT32, "tn", tinyType));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.INT32, "sml", OriginalType.INT_16));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.BINARY, "vc1", OriginalType.UTF8));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.BINARY, "c1", OriginalType.UTF8));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.BINARY, "bin", null));

        return new MessageType("hive_schema", fields);
    }

    private List<Group> readParquetFile(String file, long expectedSize) throws IOException {
        List<Group> result = new ArrayList<>();
        String parquetFile = getClass().getClassLoader().getResource("parquet/" + file).getPath();
        Path path = new Path(parquetFile);

        ParquetFileReader fileReader = new ParquetFileReader(new Configuration(), path, ParquetMetadataConverter.NO_FILTER);
        PageReadStore rowGroup;
        while ((rowGroup = fileReader.readNextRowGroup()) != null) {
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            RecordReader<Group> recordReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
            long rowCount = rowGroup.getRowCount();
            for (long i = 0; i < rowCount; i++) {
                result.add(recordReader.read());
            }
        }
        fileReader.close();
        assertEquals(expectedSize, result.size());
        return result;
    }

}
