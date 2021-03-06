package org.greenplum.pxf.plugins.jdbc;

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

import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JDBC tables plugin (base class)
 *
 * Implemented subclasses: {@link JdbcAccessor}, {@link JdbcResolver}.
 */
public class JdbcBasePlugin extends BasePlugin {

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);

        jdbcDriver = context.getOption("JDBC_DRIVER");
        if (jdbcDriver == null) {
            throw new IllegalArgumentException("JDBC_DRIVER is a required parameter");
        }

        dbUrl = context.getOption("DB_URL");
        if (dbUrl == null) {
            throw new IllegalArgumentException("DB_URL is a required parameter");
        }

        tableName = context.getDataSource();
        if (tableName == null) {
            throw new IllegalArgumentException("Data source must be provided");
        }
        /*
        At the moment, when writing into some table, the table name is
        concatenated with a special string that is necessary to write into HDFS.
        However, a raw table name is necessary in case of JDBC.
        The correct table name is extracted here.
        */
        Matcher matcher = tableNamePattern.matcher(tableName);
        if (matcher.matches()) {
            context.setDataSource(matcher.group(1));
            tableName = context.getDataSource();
        }

        columns = context.getTupleDescription();
        if (columns == null) {
            throw new IllegalArgumentException("Tuple description must be provided");
        }

        // This parameter is not required. The default value is null
        user = context.getOption("USER");
        if (user != null) {
            pass = context.getOption("PASS");
        }

        // This parameter is not required. The default value is 0
        String batchSizeRaw = context.getOption("BATCH_SIZE");
        if (batchSizeRaw != null) {
            try {
                batchSize = Integer.parseInt(batchSizeRaw);
                if (batchSize < 1) {
                    throw new NumberFormatException();
                } else if (batchSize == 0) {
                    batchSize = 1;
                }
                batchSizeIsSetByUser = true;
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("BATCH_SIZE is incorrect: must be a non-negative integer");
            }
        }

        // This parameter is not required. The default value is 1
        String poolSizeRaw = context.getOption("POOL_SIZE");
        if (poolSizeRaw != null) {
            try {
                poolSize = Integer.parseInt(poolSizeRaw);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("POOL_SIZE is incorrect: must be an integer");
            }
        }
    }

    /**
     * Open a new JDBC connection
     *
     * @throws ClassNotFoundException if the JDBC driver was not found
     * @throws SQLException if a database access error occurs
     * @throws SQLTimeoutException if a connection problem occurs
     *
     * @return connection
     */
    public Connection getConnection() throws ClassNotFoundException, SQLException, SQLTimeoutException {
        Connection connection;
        if (user != null) {
            LOG.debug("Open JDBC connection: driver={}, url={}, user={}, pass={}, table={}",
                    jdbcDriver, dbUrl, user, pass, tableName);
        } else {
            LOG.debug("Open JDBC connection: driver={}, url={}, table={}",
                    jdbcDriver, dbUrl, tableName);
        }
        Class.forName(jdbcDriver);
        if (user != null) {
            connection = DriverManager.getConnection(dbUrl, user, pass);
        }
        else {
            connection = DriverManager.getConnection(dbUrl);
        }
        return connection;
    }

    /**
     * Close a JDBC connection
     *
     * @param connection connection to close
     */
    public static void closeConnection(Connection connection) {
        try {
            if ((connection != null) && (!connection.isClosed())) {
                if ((connection.getMetaData().supportsTransactions()) && (!connection.getAutoCommit())) {
                    connection.commit();
                }
                connection.close();
            }
        }
        catch (SQLException e) {
            LOG.error("JDBC connection close error", e);
        }
    }

    /**
     * Prepare a JDBC PreparedStatement
     *
     * @param connection connection to use for creating the statement
     * @param query query to execute
     *
     * @return PreparedStatement
     *
     * @throws ClassNotFoundException if the JDBC driver was not found
     * @throws SQLException if a database access error occurs
     * @throws SQLTimeoutException if a connection problem occurs
     */
    public PreparedStatement getPreparedStatement(Connection connection, String query) throws SQLException, SQLTimeoutException, ClassNotFoundException {
        if ((connection == null) || (query == null)) {
            throw new IllegalArgumentException("The provided query or connection is null");
        }
        if (connection.getMetaData().supportsTransactions()) {
            connection.setAutoCommit(false);
        }
        return connection.prepareStatement(query);
    }

    /**
     * Close a JDBC Statement (and the connection it is based on)
     *
     * @param statement statement to close
     */
    public static void closeStatement(Statement statement) {
        if (statement == null) {
            return;
        }
        Connection connection = null;
        try {
            if (!statement.isClosed()) {
                connection = statement.getConnection();
                statement.close();
            }
        }
        catch (Exception e) {}
        closeConnection(connection);
    }

    // JDBC parameters
    protected String jdbcDriver = null;
    protected String dbUrl = null;
    protected String user = null;
    protected String pass = null;

    protected String tableName = null;

    // '100' is a recommended value: https://docs.oracle.com/cd/E11882_01/java.112/e16548/oraperf.htm#JJDBC28754
    public static final int DEFAULT_BATCH_SIZE = 100;
    // After argument parsing, this value is guaranteed to be >= 1
    protected int batchSize = DEFAULT_BATCH_SIZE;
    protected boolean batchSizeIsSetByUser = false;

    protected int poolSize = 1;

    // Columns description
    protected List<ColumnDescriptor> columns = null;


    private static final Logger LOG = LoggerFactory.getLogger(JdbcBasePlugin.class);

    // At the moment, when writing into some table, the table name is concatenated with a special string that is necessary to write into HDFS. However, a raw table name is necessary in case of JDBC. This Pattern allows to extract the correct table name from the given RequestContext.dataSource
    private static final Pattern tableNamePattern = Pattern.compile("/(.*)/[0-9]*-[0-9]*_[0-9]*");
}
