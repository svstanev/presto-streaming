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

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

public final class Table
{
    private final boolean isGrouped;
    private final long tableId;

    public Table(long tableId, boolean isGrouped)
    {
        this.tableId = tableId;
        this.isGrouped = isGrouped;
    }

    public long getTableId()
    {
        return tableId;
    }

    public boolean isGrouped()
    {
        return isGrouped;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableId);
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
        Table o = (Table) obj;
        return tableId == o.tableId;
    }

    public static class TableMapper
            implements ResultSetMapper<Table>
    {
        @Override
        public Table map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new Table(r.getLong("table_id"), r.getBoolean("is_grouped"));
        }
    }
}
