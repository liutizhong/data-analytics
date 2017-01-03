package com.yg.combinesmallfiles.combinesmallfilesbyhadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import com.yg.util.PairOfStringLong;
import java.io.IOException;

/**
 * Created by liuti on 2017/1/3.
 */
public class CustomCFIF extends CombineFileInputFormat<PairOfStringLong, Text> {
    final static long MAX_SPLIT_SIZE_64MB = 67108864; // 64 MB = 64*1024*1024

    public CustomCFIF() {
        super();
        setMaxSplitSize(MAX_SPLIT_SIZE_64MB);
    }

    public RecordReader<PairOfStringLong, Text> createRecordReader(InputSplit split,
                                                                   TaskAttemptContext context)
            throws IOException {
        return new CombineFileRecordReader<PairOfStringLong, Text>((CombineFileSplit)split,
                context,
                CustomRecordReader.class);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}