package io.aiven.connect.jdbc.source;

import javax.sql.rowset.serial.SerialBlob;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JdbcSourceTaskConversionTest extends JdbcSourceTaskTestBase {

    public boolean extendedMapping = true;

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        final Map<String, String> taskConfig = singleTableConfig(extendedMapping);
        taskConfig.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, "1");
        task.start(taskConfig);
    }

    @AfterEach
    public void tearDown() throws Exception {
        task.stop();
        super.tearDown();
    }

    @Test
    public void testBoolean() throws Exception {
        testTypeConversion("BOOLEAN", false, false, Schema.BOOLEAN_SCHEMA, false);
    }

    @Test
    public void testNullableBoolean() throws Exception {
        testTypeConversion("BOOLEAN", true, false, Schema.OPTIONAL_BOOLEAN_SCHEMA, false);
        testTypeConversion("BOOLEAN", true, null, Schema.OPTIONAL_BOOLEAN_SCHEMA, null);
    }

    @Test
    public void testSmallInt() throws Exception {
        testTypeConversion("SMALLINT", false, 1, Schema.INT16_SCHEMA, (short) 1);
    }

    // ... other test methods ...

    private void testTypeConversion(final String sqlType, final boolean nullable,
                                    final Object sqlValue, final Schema convertedSchema,
                                    final Object convertedValue) throws Exception {
        createTable(sqlType, nullable);
        insertValue(sqlValue);
        List<SourceRecord> records = pollRecords();
        validateRecords(records, convertedSchema, convertedValue);
        dropTable();
    }

    private void createTable(final String sqlType, final boolean nullable) throws Exception {
        String sqlColumnSpec = sqlType + (nullable ? "" : " NOT NULL");
        db.createTable(SINGLE_TABLE_NAME, "id", sqlColumnSpec);
    }

    private void insertValue(final Object sqlValue) throws Exception {
        db.insert(SINGLE_TABLE_NAME, "id", sqlValue);
    }

    private List<SourceRecord> pollRecords() throws Exception {
        List<SourceRecord> records = null;
        for (int retries = 0; retries < 10 && records == null; retries++) {
            records = task.poll();
        }
        return records;
    }

    private void dropTable() throws Exception {
        db.dropTable(SINGLE_TABLE_NAME);
    }

    private void validateRecords(final List<SourceRecord> records, final Schema expectedFieldSchema,
                                 final Object expectedValue) {
        assertThat(records).hasSize(1);
        final Object objValue = records.get(0).value();
        assertThat(objValue).isInstanceOf(Struct.class);
        final Struct value = (Struct) objValue;

        final Schema schema = value.schema();
        assertThat(schema.type()).isEqualTo(Type.STRUCT);
        final List<Field> fields = schema.fields();
        assertThat(fields).hasSize(1);

        final Schema fieldSchema = fields.get(0).schema();
        assertThat(fieldSchema).isEqualTo(expectedFieldSchema);
        if (expectedValue instanceof byte[]) {
            assertThat(value.get(fields.get(0))).isInstanceOf(byte[].class);
            assertThat(ByteBuffer.wrap((byte[]) value.get(fields.get(0))))
                    .isEqualTo(ByteBuffer.wrap((byte[]) expectedValue));
        } else {
            assertThat(value.get(fields.get(0))).isEqualTo(expectedValue);
        }
    }
}
