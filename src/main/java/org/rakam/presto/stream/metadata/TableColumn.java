/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.rakam.presto.stream.metadata;

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

public class TableColumn
{
    private final SchemaTableName table;
    private final String columnName;
    private final int ordinalPosition;
    private final Type dataType;
    @Nullable private final Signature signature;
    private boolean isAggregationField;

    public TableColumn(SchemaTableName table, String columnName, Signature signature, boolean isAggregationField, int ordinalPosition, Type dataType)
    {
        this.table = requireNonNull(table, "table is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        checkArgument(ordinalPosition >= 0, "ordinal position is negative");
        this.ordinalPosition = ordinalPosition;
        this.signature = signature;
        this.isAggregationField = isAggregationField;
        this.dataType = requireNonNull(dataType, "dataType is null");
    }

    public SchemaTableName getTable()
    {
        return table;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public Signature getSignature()
    {
        return signature;
    }

    public Type getDataType()
    {
        return dataType;
    }

    @Override
    public int hashCode()
    {
        return hash(table, columnName, ordinalPosition, dataType);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        TableColumn o = (TableColumn) obj;
        return Objects.equals(table, o.table) &&
                Objects.equals(columnName, o.columnName) &&
                Objects.equals(ordinalPosition, o.ordinalPosition) &&
                Objects.equals(dataType, o.dataType);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("columnName", columnName)
                .add("ordinalPosition", ordinalPosition)
                .add("dataType", dataType)
                .toString();
    }

    public ColumnMetadata toColumnMetadata()
    {
        return new ColumnMetadata(columnName, dataType, false); //ordinalPosition
    }

    public boolean getIsAggregationField()
    {
        return isAggregationField;
    }

    public static class Mapper
            implements ResultSetMapper<TableColumn>
    {
        private static final ObjectMapper mapper = new ObjectMapper();
        private final TypeManager typeManager;

        @Inject
        public Mapper(TypeManager typeManager)
        {
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        public TableColumn map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            SchemaTableName table = new SchemaTableName(
                    r.getString("schema_name"),
                    r.getString("table_name"));

            String typeName = r.getString("data_type");
            Type type = typeManager.getType(parseTypeSignature(typeName));
            checkArgument(type != null, "Unknown type %s", typeName);

            byte[] serializedSignature = r.getBytes("signature");

            Signature signature = null;
            if (serializedSignature != null) {
                try {
                    signature = mapper.readValue(serializedSignature, Signature.class);
                }
                catch (IOException e) {
                    //
                }
            }

            return new TableColumn(
                    table,
                    r.getString("column_name"),
                    signature,
                    r.getBoolean("is_aggregation_field"),
                    r.getInt("ordinal_position"),
                    type);
        }
    }
}
