import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.avro.Column;
import org.apache.cassandra.avro.ColumnOrSuperColumn;
import org.apache.cassandra.avro.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class DumpSuperMap extends Configured implements Tool {
    public static class ColumnFamilyMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, Text> {
        
        private Integer longNames;
        
        public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException {
            String fields = "";
            if(longNames == 1) {
                for (IColumn superColumn : columns.values()) {
                    Long superColName = superColumn.name().getLong();
                    for(IColumn column : superColumn.getSubColumns()) {
                        fields = "";
                        fields += superColName;
                        fields += "\t";
                        fields += column.name().getLong();
                        fields += "\t";
                        fields += CassandraUtils.byteBufferToString(column.value());
                        context.write(new Text(CassandraUtils.byteBufferToString(key)), new Text(fields));
                    }
                }
            } else {
                for (IColumn superColumn : columns.values()) {
                    String superColName = CassandraUtils.byteBufferToString(superColumn.name());
                    for(IColumn column : superColumn.getSubColumns()) {
                        fields = "";
                        fields += superColName;
                        fields += "\t";
                        fields += CassandraUtils.byteBufferToString(column.name());
                        fields += "\t";
                        fields += CassandraUtils.byteBufferToString(column.value());
                        context.write(new Text(CassandraUtils.byteBufferToString(key)), new Text(fields));
                    }
                }
            }
        }

        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.longNames    = Integer.parseInt(conf.get("cassandra.longnames"));
        }

    }
    
    public int run(String[] args) throws Exception {
        Job job                    = new Job(getConf());
        job.setJarByClass(DumpSuperMap.class);
        job.setJobName("DumpSuperMap");
        job.setNumReduceTasks(0);
        job.setMapperClass(ColumnFamilyMapper.class);        
        job.setInputFormatClass(ColumnFamilyInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Configuration conf = job.getConfiguration();
        ConfigHelper.setRangeBatchSize(conf, Integer.parseInt(conf.get("cassandra.batch_size")));
        ConfigHelper.setRpcPort(conf, conf.get("cassandra.thrift_port"));
        ConfigHelper.setInitialAddress(conf, conf.get("cassandra.initial_host"));
        ConfigHelper.setInputColumnFamily(conf, conf.get("cassandra.keyspace"), conf.get("cassandra.column_family"));

        SliceRange range = new SliceRange(ByteBuffer.wrap(ArrayUtils.EMPTY_BYTE_ARRAY), ByteBuffer.wrap(ArrayUtils.EMPTY_BYTE_ARRAY), false, Integer.MAX_VALUE);
        SlicePredicate predicate = new SlicePredicate().setColumn_names(null).setSlice_range(range);
        
        ConfigHelper.setInputSlicePredicate(conf, predicate);

        // Handle output path
        List<String> other_args = new ArrayList<String>();
        for (int i=0; i < args.length; ++i) {
            other_args.add(args[i]);
        }
        FileOutputFormat.setOutputPath(job, new Path(other_args.get(0)));

        // Submit job to server and wait for completion
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new DumpSuperMap(), args);
        System.exit(0);
    }
}
