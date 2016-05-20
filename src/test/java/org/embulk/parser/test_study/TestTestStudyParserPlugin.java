// 自分のパッケージ名に変更
package org.embulk.parser.test_study;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.TaskSource;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.FileInput;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import com.google.common.collect.ImmutableList;
import org.embulk.spi.type.Type;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.util.Pages;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.File;
import java.util.List;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TestTestStudyParserPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();


    private ConfigSource config;
    private MockPageOutput output;

    // 自分のプラグインの名前に変える。
    private TestStudyParserPlugin plugin;


    @Before
    public void createResource()
    {
        //
        // parser:
        //   type: test_study
        //
        config = config().set("type", "test_study");

        // 自分のプラグインの名前に変える。
        plugin = new TestStudyParserPlugin();
        recreatePageOutput();
    }



    @Test
    public void skipRecords()
            throws Exception
    {

        //
        SchemaConfig schema = schema(
                column("c1", STRING),
                column("c2", STRING),
                column("c3", LONG)

        );

        ConfigSource config = this.config.deepCopy().set("columns", schema);


        transaction(config, fileInput(
                "aaa\tbbb123"
        ));


        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(1, records.size());

    }





    private void recreatePageOutput()
    {
        output = new MockPageOutput();
    }

    private ConfigSource config()
    {
        return runtime.getExec().newConfigSource();
    }

    private File getResourceFile(String resourceName)
            throws IOException
    {
        return new File(this.getClass().getResource(resourceName).getFile());
    }

    private ConfigSource getConfigFromYamlFile(File yamlFile)
            throws IOException
    {
        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        return loader.fromYamlFile(yamlFile);
    }

    private void transaction(ConfigSource config, final FileInput input)
    {
        plugin.transaction(config, new ParserPlugin.Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema schema)
            {
                plugin.run(taskSource, schema, input, output);
            }
        });
    }

    private FileInput fileInput(String... lines)
            throws Exception
    {
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            sb.append(line).append("\n");
        }

        ByteArrayInputStream in = new ByteArrayInputStream(sb.toString().getBytes());
        return new InputStreamFileInput(runtime.getBufferAllocator(), provider(in));
    }

    private InputStreamFileInput.IteratorProvider provider(InputStream... inputStreams)
            throws IOException
    {
        return new InputStreamFileInput.IteratorProvider(
                ImmutableList.copyOf(inputStreams));
    }

    private SchemaConfig schema(ColumnConfig... columns)
    {
        return new SchemaConfig(Lists.newArrayList(columns));
    }

    private ColumnConfig column(String name, Type type)
    {
        return column(name, type, config());
    }

    private ColumnConfig column(String name, Type type, ConfigSource option)
    {
        return new ColumnConfig(name, type, option);
    }


}
