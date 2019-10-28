package org.embulk.filter.murmur2_partitioner;

import com.google.common.collect.ImmutableList;
import org.embulk.config.*;
import org.embulk.spi.*;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.Types;

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
        public int getPartitionCount();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        ImmutableList.Builder<Column> listBuilder = ImmutableList.builder();
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

            listBuilder.add(column);
        }
        if (!hasPartitionColumn) {
            listBuilder.add(new Column(inputSchema.getColumnCount(), task.getPartitionColumn(), Types.LONG));
        }

        Schema outputSchema = new Schema(listBuilder.build());

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema,
            Schema outputSchema, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        Column partitionColumn = outputSchema.getColumns()
                .stream()
                .filter(column -> column.getName().equals(task.getPartitionColumn()))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("partition column is not found"));

        return new PageOutput() {
            private PageReader pageReader = new PageReader(inputSchema);
            private PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private ColumnVisitorImpl columnVisitor = new ColumnVisitorImpl(task.getKeyColumn(), task.getPartitionCount(), partitionColumn, pageReader, pageBuilder);

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
        private String keyColumn;
        private int partitionCount;
        private Column partitionColumn;
        private PageReader pageReader;
        private PageBuilder pageBuilder;

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
