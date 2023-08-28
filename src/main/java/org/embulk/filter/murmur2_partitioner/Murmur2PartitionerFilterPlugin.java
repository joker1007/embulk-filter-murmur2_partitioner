package org.embulk.filter.murmur2_partitioner;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.embulk.config.*;
import org.embulk.spi.*;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.Types;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class Murmur2PartitionerFilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("key_column")
        public String getKeyColumn();

        @Config("partition_column")
        @ConfigDefault("\"partition\"")
        public String getPartitionColumn();

        @Config("partition_count")
        @ConfigDefault("null")
        public Optional<Integer> getPartitionCount();
        public void setPartitionCount(Optional<Integer> count);

        @Config("topic")
        @ConfigDefault("null")
        public Optional<String> getTopic();

        @Config("brokers")
        @ConfigDefault("[]")
        public List<String> getBrokers();
    }

    private final ConfigMapperFactory configMapperFactory = ConfigMapperFactory.withDefault();

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            Control control)
    {
        ConfigMapper configMapper = configMapperFactory.createConfigMapper();
        PluginTask task = configMapper.map(config, PluginTask.class);

        if (!task.getPartitionCount().isPresent() && !task.getTopic().isPresent()) {
            throw new ConfigException("Either `partition_count` or `topic` parameter are required.");
        }

        if (task.getTopic().isPresent()) {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, task.getBrokers());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
            try(KafkaConsumer<Bytes, Bytes> consumer = new KafkaConsumer<>(props)) {
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(task.getTopic().get());
                task.setPartitionCount(Optional.of(partitionInfos.size()));
            }
        }

        List<Column> columns = new ArrayList<>();
        boolean hasPartitionColumn = false;
        for (Column column : inputSchema.getColumns()) {
            if (column.getName().equals(task.getKeyColumn())) {
                if (!(column.getType() instanceof StringType || column.getType() instanceof LongType)) {
                    throw new ConfigException(String.format("Unsupport %s as key_column (name = %s)", column.getType().getName(), column.getName()));
                }
            }

            if (column.getName().equals(task.getPartitionColumn())) {
                hasPartitionColumn = true;
            }

            columns.add(column);
        }
        if (!hasPartitionColumn) {
            columns.add(new Column(inputSchema.getColumnCount(), task.getPartitionColumn(), Types.LONG));
        }

        Schema outputSchema = new Schema(columns);

        control.run(task.toTaskSource(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema,
            Schema outputSchema, PageOutput output)
    {
        TaskMapper taskMapper = configMapperFactory.createTaskMapper();
        PluginTask task = taskMapper.map(taskSource, PluginTask.class);

        Column partitionColumn = outputSchema.getColumns()
                .stream()
                .filter(column -> column.getName().equals(task.getPartitionColumn()))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("partition column is not found"));

        return new PageOutput() {
            private final PageReader pageReader = new PageReader(inputSchema);
            private final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private final ColumnVisitorImpl columnVisitor = new ColumnVisitorImpl(task.getKeyColumn(), task.getPartitionCount().get(), partitionColumn, pageReader, pageBuilder);

            @Override
            public void add(Page page) {
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    inputSchema.visitColumns(columnVisitor);
                    pageBuilder.addRecord();
                }
            }

            @Override
            public void finish() {
                pageBuilder.finish();
            }

            @Override
            public void close() {
                pageBuilder.close();
            }
        };
    }

    static class ColumnVisitorImpl implements ColumnVisitor
    {
        private final String keyColumn;
        private final int partitionCount;
        private final Column partitionColumn;
        private final PageReader pageReader;
        private final PageBuilder pageBuilder;

        ColumnVisitorImpl(String keyColumn, int partitionCount, Column partitionColumn, PageReader pageReader, PageBuilder pageBuilder) {
            this.keyColumn = keyColumn;
            this.partitionCount = partitionCount;
            this.partitionColumn = partitionColumn;
            this.pageReader = pageReader;
            this.pageBuilder = pageBuilder;
        }

        @Override
        public void booleanColumn(Column column) {
            if (pageReader.isNull(column)) {
                pageBuilder.setNull(column);
                return;
            }
            pageBuilder.setBoolean(column, pageReader.getBoolean(column));
        }

        @Override
        public void longColumn(Column column) {
            if (pageReader.isNull(column)) {
                pageBuilder.setNull(column);
                return;
            }

            if (column.getName().equals(keyColumn)) {
                int partition = Murmur2Partitioner.partition(pageReader.getLong(column), partitionCount);
                pageBuilder.setLong(partitionColumn, partition);
            }

            pageBuilder.setLong(column, pageReader.getLong(column));
        }

        @Override
        public void doubleColumn(Column column) {
            if (pageReader.isNull(column)) {
                pageBuilder.setNull(column);
                return;
            }
            pageBuilder.setDouble(column, pageReader.getDouble(column));
        }

        @Override
        public void stringColumn(Column column) {
            if (pageReader.isNull(column)) {
                pageBuilder.setNull(column);
                return;
            }

            if (column.getName().equals(keyColumn)) {
                int partition = Murmur2Partitioner.partition(pageReader.getString(column), partitionCount);
                pageBuilder.setLong(partitionColumn, partition);
            }

            pageBuilder.setString(column, pageReader.getString(column));
        }

        @Override
        public void timestampColumn(Column column) {
            if (pageReader.isNull(column)) {
                pageBuilder.setNull(column);
                return;
            }
            pageBuilder.setTimestamp(column, pageReader.getTimestamp(column));
        }

        @Override
        public void jsonColumn(Column column) {
            if (pageReader.isNull(column)) {
                pageBuilder.setNull(column);
                return;
            }
            pageBuilder.setJson(column, pageReader.getJson(column));
        }
    }
}
