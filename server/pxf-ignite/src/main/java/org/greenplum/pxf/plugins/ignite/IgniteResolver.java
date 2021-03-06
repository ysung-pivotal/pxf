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

import com.google.gson.JsonArray;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

/**
 * PXF-Ignite resolver class
 */
public class IgniteResolver extends IgniteBasePlugin implements Resolver {

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);
        columns = requestContext.getTupleDescription();
    }

    /**
     * Transform a {@link JsonArray} stored in {@link OneRow} into a list of {@link OneField}
     *
     * @throws ParseException if the response could not be correctly parsed
     * @throws UnsupportedOperationException if the type of some field is not supported
     */
    @Override
    public List<OneField> getFields(OneRow row) throws ParseException, UnsupportedOperationException {
        JsonArray result = JsonArray.class.cast(row.getData());
        LinkedList<OneField> fields = new LinkedList<OneField>();

        if (result.size() != columns.size()) {
            throw new ParseException("getFields(): Failed (a tuple received from Ignite contains more or less fields than requested). Raw tuple: '" + result.toString() + "'", 0);
        }

        for (int i = 0; i < columns.size(); i++) {
            Object value = null;
            OneField oneField = new OneField(columns.get(i).columnTypeCode(), null);

            // Handle null values
            if (result.get(i).isJsonNull()) {
                fields.add(oneField);
                continue;
            }
            switch (DataType.get(oneField.type)) {
                case INTEGER:
                    value = result.get(i).getAsInt();
                    break;
                case FLOAT8:
                    value = result.get(i).getAsDouble();
                    break;
                case REAL:
                    value = result.get(i).getAsFloat();
                    break;
                case BIGINT:
                    value = result.get(i).getAsLong();
                    break;
                case SMALLINT:
                    value = result.get(i).getAsShort();
                    break;
                case BOOLEAN:
                    value = result.get(i).getAsBoolean();
                    break;
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                case NUMERIC:
                    value = result.get(i).getAsString();
                    break;
                case BYTEA:
                    value = Base64.decodeBase64(result.get(i).getAsString());
                    break;
                case TIMESTAMP:
                    boolean isConversionSuccessful = false;
                    for (SimpleDateFormat sdf : getTimestampSDFs.get()) {
                        try {
                            value = sdf.parse(result.get(i).getAsString());
                            isConversionSuccessful = true;
                            break;
                        }
                        catch (ParseException e) {
                            // pass
                        }
                    }
                    if (!isConversionSuccessful) {
                        throw new ParseException(result.get(i).getAsString(), 0);
                    }
                    break;
                case DATE:
                    value = getDateSDF.parse(result.get(i).getAsString());
                    break;
                default:
                    throw new UnsupportedOperationException("Field type not supported: " + DataType.get(oneField.type).toString()
                            + ", Column: " + columns.get(i).columnName());
            }

            oneField.val = value;
            fields.add(oneField);
        }

        return fields;
    }

    /**
     * Transforms a list of {@link OneField} from PXF into a {@link OneRow} with a string inside, containing a tuple from SQL INSERT query
     *
     * @param record List of fields
     * @return row one row
     *
     * @throws UnsupportedOperationException if the type of some field is not supported
     */
    @Override
    public OneRow setFields(List<OneField> record) throws UnsupportedOperationException {
        StringBuilder sb = new StringBuilder();
        String fieldDivisor = "";

        sb.append("(");
        for (OneField oneField : record) {
            sb.append(fieldDivisor);
            fieldDivisor = ", ";
            switch (DataType.get(oneField.type)) {
                case BOOLEAN:
                case INTEGER:
                case FLOAT8:
                case REAL:
                case BIGINT:
                case NUMERIC:
                case SMALLINT:
                    sb.append(String.valueOf(oneField.val));
                    break;
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                    sb.append("'" + String.valueOf(oneField.val) + "'");
                    break;
                case BYTEA:
                    sb.append("'" + Hex.encodeHexString((byte[])(oneField.val)) + "'");
                    break;
                case TIMESTAMP:
                    sb.append(setTimestampSDF.get().format(oneField.val));
                    break;
                case DATE:
                    sb.append(setDateSDF.format(oneField.val));
                    break;
                default:
                    throw new UnsupportedOperationException("Field type not supported: " + DataType.get(oneField.type).toString());
            }
        }
        sb.append(")");
        return new OneRow(sb.toString());
    }

    private static final Log LOG = LogFactory.getLog(IgniteResolver.class);

    private static final SimpleDateFormat getDateSDF = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat setDateSDF = new SimpleDateFormat("'yyyy-MM-dd'");

    // SimpleDateFormats to parse Ignite TIMESTAMP format
    private static ThreadLocal<SimpleDateFormat[]> getTimestampSDFs = new ThreadLocal<SimpleDateFormat[]>() {
        @Override protected SimpleDateFormat[] initialValue() {
            SimpleDateFormat[] retRes = {
                new SimpleDateFormat("MMM d, yyyy hh:mm:ss.SSSSSS a"),
                new SimpleDateFormat("MMM d, yyyy hh:mm:ss.SSSSS a"),
                new SimpleDateFormat("MMM d, yyyy hh:mm:ss.SSSS a"),
                new SimpleDateFormat("MMM d, yyyy hh:mm:ss.SSS a"),
                new SimpleDateFormat("MMM d, yyyy hh:mm:ss.SS a"),
                new SimpleDateFormat("MMM d, yyyy hh:mm:ss.S a"),
                new SimpleDateFormat("MMM d, yyyy hh:mm:ss a")
            };
            return retRes;
        }
    };

    // SimpleDateFormat to properly encode TIMESTAMP for INSERT queries
    private static ThreadLocal<SimpleDateFormat> setTimestampSDF = new ThreadLocal<SimpleDateFormat>() {
        @Override protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("'yyyy-MM-dd hh:mm:ss.SSSSSS'");
        }
    };

    // GPDB column descriptors
    private List<ColumnDescriptor> columns = null;
}
