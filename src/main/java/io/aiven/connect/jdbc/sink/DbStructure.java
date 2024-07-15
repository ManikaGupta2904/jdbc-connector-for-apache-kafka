package io.aiven.connect.jdbc.sink;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.sink.metadata.FieldsMetadata;
import io.aiven.connect.jdbc.sink.metadata.SinkRecordField;
import io.aiven.connect.jdbc.util.TableDefinition;
import io.aiven.connect.jdbc.util.TableDefinitions;
import io.aiven.connect.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static org.apache.kafka.common.requests.DeleteAclsResponse.log;

public class DbStructure {
    private static final Logger log = LoggerFactory.getLogger(DbStructure.class);

    private final DatabaseDialect dbDialect;
    private final TableDefinitions tableDefns;

    public DbStructure(final DatabaseDialect dbDialect) {
        this.dbDialect = dbDialect;
        this.tableDefns = new TableDefinitions(dbDialect);
    }

    public TableDefinition tableDefinitionFor(final TableId tableId, final Connection connection) throws SQLException {
        final var tblDefinition = tableDefns.get(connection, tableId);
        return Objects.nonNull(tblDefinition) ? tblDefinition : tableDefns.refresh(connection, tableId);
    }

    public boolean createOrAmendIfNecessary(
            final JdbcSinkConfig config,
            final Connection connection,
            final TableId tableId,
            final FieldsMetadata fieldsMetadata
    ) throws SQLException {
        if (tableDefns.get(connection, tableId) == null) {
            try {
                create(config, connection, tableId, fieldsMetadata);
            } catch (final SQLException sqle) {
                log.warn("Create failed, will attempt amend if table already exists", sqle);
                tableDefns.refresh(connection, tableId);
            }
        }
        return amendIfNecessary(config, connection, tableId, fieldsMetadata, config.maxRetries);
    }

    void create(final JdbcSinkConfig config, final Connection connection, final TableId tableId, final FieldsMetadata fieldsMetadata) throws SQLException {
        if (!config.autoCreate) {
            try {
                throw new ConnectException(String.format("Table %s is missing and auto-creation is disabled", tableId));
            } catch (ConnectException e) {
                throw new RuntimeException(e);
            }
        }
        final String sql = dbDialect.buildCreateTableStatement(tableId, fieldsMetadata.allFields.values());
        log.info("Creating table with sql: {}", sql);
        dbDialect.applyDdlStatements(connection, Collections.singletonList(sql));
    }

    boolean amendIfNecessary(
            final JdbcSinkConfig config,
            final Connection connection,
            final TableId tableId,
            final FieldsMetadata fieldsMetadata,
            final int maxRetries
    ) throws SQLException {
        final TableDefinition tableDefn = tableDefns.get(connection, tableId);
        FieldComparator fieldComparator = new FieldComparator(fieldsMetadata, tableDefn.columnNames());

        Set<SinkRecordField> missingFields = fieldComparator.findMissingFields();

        if (missingFields.isEmpty()) {
            return false;
        }

        for (final SinkRecordField missingField : missingFields) {
            if (!missingField.isOptional() && missingField.defaultValue() == null) {
                try {
                    throw new ConnectException("Cannot ALTER to add missing field " + missingField + ", as it is not optional and does not have a default value");
                } catch (ConnectException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (!config.autoEvolve) {
            try {
                throw new ConnectException(String.format("Table %s is missing fields (%s) and auto-evolution is disabled", tableId, missingFields));
            } catch (ConnectException e) {
                throw new RuntimeException(e);
            }
        }

        final List<String> amendTableQueries = dbDialect.buildAlterTable(tableId, missingFields);
        log.info("Amending table to add missing fields:{} maxRetries:{} with SQL: {}", missingFields, maxRetries, amendTableQueries);

        try {
            dbDialect.applyDdlStatements(connection, amendTableQueries);
        } catch (final SQLException sqle) {
            if (maxRetries <= 0) {
                try {
                    throw new ConnectException(String.format("Failed to amend table '%s' to add missing fields: %s", tableId, missingFields));
                } catch (ConnectException e) {
                    throw new RuntimeException(e);
                }
            }
            log.warn("Amend failed, re-attempting", sqle);
            tableDefns.refresh(connection, tableId);
            return amendIfNecessary(config, connection, tableId, fieldsMetadata, maxRetries - 1);
        }

        tableDefns.refresh(connection, tableId);
        return true;
    }
}

class FieldComparator {
    private final FieldsMetadata fieldsMetadata;
    private final Set<String> dbColumnNames;

    public FieldComparator(FieldsMetadata fieldsMetadata, Set<String> dbColumnNames) {
        this.fieldsMetadata = fieldsMetadata;
        this.dbColumnNames = dbColumnNames;
    }

    public Set<SinkRecordField> findMissingFields() {
        Set<SinkRecordField> missingFields = new HashSet<>();
        for (final SinkRecordField field : fieldsMetadata.allFields.values()) {
            if (!dbColumnNames.contains(field.name())) {
                log.debug("Found missing field: {}", field);
                missingFields.add(field);
            }
        }
        return checkForCaseSensitivity(missingFields);
    }

    private Set<SinkRecordField> checkForCaseSensitivity(Set<SinkRecordField> missingFields) {
        if (missingFields.isEmpty()) {
            return missingFields;
        }

        Set<String> columnNamesLowerCase = new HashSet<>();
        for (final String columnName : dbColumnNames) {
            columnNamesLowerCase.add(columnName.toLowerCase());
        }

        Set<SinkRecordField> missingFieldsIgnoreCase = new HashSet<>();
        for (final SinkRecordField missing : missingFields) {
            if (!columnNamesLowerCase.contains(missing.name().toLowerCase())) {
                missingFieldsIgnoreCase.add(missing);
            }
        }

        if (!missingFieldsIgnoreCase.isEmpty()) {
            log.info("Unable to find fields {} among column names {}", missingFieldsIgnoreCase, dbColumnNames);
        }

        return missingFieldsIgnoreCase;
    }
}
