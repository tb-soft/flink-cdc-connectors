/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oceanbase.source;

import com.oceanbase.oms.logmessage.DataMessage;

import java.sql.Types;

/** Utils to convert jdbc type and value of a field. */
public class OceanBaseJdbcConverter {

    private static boolean isBoolean(int jdbcType, String typeName) {
        return jdbcType == Types.BOOLEAN || (jdbcType == Types.BIT && "TINYINT".equals(typeName));
    }

    public static int getType(int jdbcType, String typeName) {
        // treat boolean as tinyint type
        if (isBoolean(jdbcType, typeName)) {
            jdbcType = Types.TINYINT;
        }
        // treat year as int type
        if ("YEAR".equals(typeName)) {
            jdbcType = Types.INTEGER;
        }

        // upcasting
        if ("INT UNSIGNED".equals(typeName)) {
            jdbcType = Types.BIGINT;
        }
        if ("BIGINT UNSIGNED".equals(typeName)) {
            jdbcType = Types.DECIMAL;
        }

        // widening conversion according to com.mysql.jdbc.ResultSetImpl#getObject
        switch (jdbcType) {
            case Types.TINYINT:
            case Types.SMALLINT:
                return Types.INTEGER;
            case Types.REAL:
                return Types.FLOAT;
            default:
                return jdbcType;
        }
    }

    public static int getType(DataMessage.Record.Field.Type fieldType) {
        switch (fieldType) {
            case NULL:
                return Types.NULL;
            case INT8:
            case INT16:
            case INT24:
            case INT32:
            case YEAR:
                return Types.INTEGER;
            case INT64:
                return Types.BIGINT;
            case FLOAT:
            case DOUBLE:
                return Types.DOUBLE;
            case DECIMAL:
                return Types.DECIMAL;
            case ENUM:
            case SET:
            case STRING:
            case JSON:
                return Types.CHAR;
            case TIMESTAMP:
            case DATETIME:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_NANO:
                return Types.TIMESTAMP;
            case DATE:
                return Types.DATE;
            case TIME:
                return Types.TIME;
            case BIT:
                return Types.BIT;
            case BLOB:
            case BINARY:
                return Types.BINARY;
            case INTERVAL_YEAR_TO_MONTH:
            case INTERVAL_DAY_TO_SECOND:
            case GEOMETRY:
            case RAW:
                // it's weird to get wrong type from TEXT column, temporarily treat it as a string
            case UNKOWN:
            default:
                return Types.VARCHAR;
        }
    }
}
