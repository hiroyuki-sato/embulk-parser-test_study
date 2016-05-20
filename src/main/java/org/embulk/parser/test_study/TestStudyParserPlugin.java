package org.embulk.parser.test_study;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.util.LineDecoder;

import java.util.ArrayList;

import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;


public class TestStudyParserPlugin
        implements ParserPlugin
{
    public interface PluginTask
            extends Task,LineDecoder.DecoderTask, TimestampParser.Task
    {
        // configuration option 1 (required integer)
        // @Config("option1")
        // public int getOption1();



    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        ArrayList<ColumnConfig> columns = new ArrayList<ColumnConfig>();

        columns.add(new ColumnConfig("col1",   STRING, config));
        columns.add(new ColumnConfig("col2",   STRING, config));
        columns.add(new ColumnConfig("col3",   LONG, config));
        
        Schema schema = new SchemaConfig(columns).toSchema();

        control.run(task.dump(), schema);
    }

    @Override
    public void run(TaskSource taskSource, Schema schema,
            FileInput input, PageOutput output) {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        LineDecoder lineDecoder = new LineDecoder(input, task);
        PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output);

        String line = null;
        String[] elements;

        while (input.nextFile()) {
            while (true) {
                line = lineDecoder.poll();
                if (line == null) {
                    break;
                }
                elements = line.split("\t", -1);
                System.out.println(elements.length);

                if( elements.length <  3 ) {
                    continue;
                }

                pageBuilder.setString(0, elements[0]);
                pageBuilder.setString(1, elements[1]);
                pageBuilder.setLong(2, Integer.parseInt(elements[2]));
                pageBuilder.addRecord();

            }
        }
        pageBuilder.finish();
    }
}
