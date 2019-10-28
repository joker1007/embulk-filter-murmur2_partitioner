package org.embulk.filter.murmur2_partitioner;

import org.embulk.EmbulkEmbed;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.spi.*;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.spi.util.Pages;
import org.embulk.test.EmbulkTests;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestMurmur2PartitionerFilterPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ConfigSource configSource()
    {
        return EmbulkEmbed.newSystemConfigLoader().fromYamlString(EmbulkTests.readResource("org/embulk/filter/murmur2_partitioner/config.yml"));
    }

    private Schema inputSchema()
    {
        return schema("key", Types.STRING);
    }

    private List<Object[]> records;
    private List<Column> outputColumns;

    @Test
    public void testFilter() throws IOException
    {
        Schema inputSchema = inputSchema();
        MockPageOutput output = new MockPageOutput();
        FilterPlugin plugin = new Murmur2PartitionerFilterPlugin();
        List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema, "hoge", "fuga", "foo", "bar");

        plugin.transaction(configSource(), inputSchema(), (taskSource, outputSchema) -> {
            outputColumns = outputSchema.getColumns();

            try (PageOutput out = plugin.open(taskSource, inputSchema(), outputSchema, output)) {
                for (Page page : pages) {
                    out.add(page);
                }
                out.finish();
            }

            records = Pages.toObjects(outputSchema, output.pages);
        });

        assertEquals(2, outputColumns.size());
        assertEquals("key", outputColumns.get(0).getName());
        assertEquals("partition", outputColumns.get(1).getName());
        assertEquals(Types.LONG, outputColumns.get(1).getType());

        assertEquals(4, records.size());

        assertEquals("hoge", records.get(0)[0]);
        assertEquals(32L, records.get(0)[1]);
        assertEquals("fuga", records.get(1)[0]);
        assertEquals(77L, records.get(1)[1]);
        assertEquals("foo", records.get(2)[0]);
        assertEquals(56L, records.get(2)[1]);
        assertEquals("bar", records.get(3)[0]);
        assertEquals(105L, records.get(3)[1]);
    }

    public static Schema schema(Object... nameAndTypes)
    {
        Schema.Builder builder = Schema.builder();
        for (int i = 0; i < nameAndTypes.length; i += 2) {
            String name = (String) nameAndTypes[i];
            Type type = (Type) nameAndTypes[i + 1];
            builder.add(name, type);
        }
        return builder.build();
    }
}
