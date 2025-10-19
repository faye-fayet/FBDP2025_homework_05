package com.hadoop.stock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class StockWordCount {

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopWords = new HashSet<>();
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                URI[] cacheFiles = context.getCacheFiles();
                if (cacheFiles != null && cacheFiles.length > 0) {
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    Path path = new Path(cacheFiles[0].toString());
                    BufferedReader reader = new BufferedReader(
                        new InputStreamReader(fs.open(path)));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        stopWords.add(line.trim().toLowerCase());
                    }
                    reader.close();
                }
            } catch (IOException e) {
                System.err.println("Exception reading stop words file: " + e);
            }
        }
        
        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            if (key.get() == 0) {
                return;
            }
            
            String line = value.toString();
            int lastCommaIndex = line.lastIndexOf(',');
            if (lastCommaIndex == -1) {
                return;
            }
            
            String text = line.substring(0, lastCommaIndex).trim();
            String sentimentStr = line.substring(lastCommaIndex + 1).trim();
            
            if (text.startsWith("\"") && text.endsWith("\"")) {
                text = text.substring(1, text.length() - 1);
            }
            
            int sentiment;
            try {
                sentiment = Integer.parseInt(sentimentStr);
            } catch (NumberFormatException e) {
                return;
            }
            
            if (sentiment != 1 && sentiment != -1) {
                return;
            }
            
            text = text.toLowerCase();
            text = text.replaceAll("[^a-z\\s]", " ");
            text = text.replaceAll("\\s+", " ").trim();
            
            String[] words = text.split("\\s+");
            
            for (String w : words) {
                w = w.trim();
                if (!w.isEmpty() && !stopWords.contains(w) && !w.matches("\\d+")) {
                    String compositeKey = sentiment + "_" + w;
                    word.set(compositeKey);
                    context.write(word, one);
                }
            }
        }
    }
    
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private Map<String, Integer> positiveWordCount = new HashMap<>();
        private Map<String, Integer> negativeWordCount = new HashMap<>();
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            String keyStr = key.toString();
            String[] parts = keyStr.split("_", 2);
            
            if (parts.length != 2) {
                return;
            }
            
            int sentiment = Integer.parseInt(parts[0]);
            String word = parts[1];
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            if (sentiment == 1) {
                positiveWordCount.put(word, sum);
            } else if (sentiment == -1) {
                negativeWordCount.put(word, sum);
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("========== POSITIVE NEWS TOP 100 WORDS =========="), new IntWritable(0));
            List<Map.Entry<String, Integer>> positiveList = new ArrayList<>(positiveWordCount.entrySet());
            positiveList.sort((a, b) -> b.getValue().compareTo(a.getValue()));
            
            int count = 0;
            for (Map.Entry<String, Integer> entry : positiveList) {
                if (count >= 100) break;
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
                count++;
            }
            
            context.write(new Text(""), new IntWritable(0));
            context.write(new Text("========== NEGATIVE NEWS TOP 100 WORDS =========="), new IntWritable(0));
            
            List<Map.Entry<String, Integer>> negativeList = new ArrayList<>(negativeWordCount.entrySet());
            negativeList.sort((a, b) -> b.getValue().compareTo(a.getValue()));
            
            count = 0;
            for (Map.Entry<String, Integer> entry : negativeList) {
                if (count >= 100) break;
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
                count++;
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        if (args.length < 3) {
            System.err.println("Usage: StockWordCount <input_csv> <stopwords_file> <output_dir>");
            System.err.println("Example: StockWordCount /stock/input/stock_data.csv /stock/input/stop-word-list.txt /stock/output");
            System.exit(2);
        }
        
        System.out.println("Input CSV: " + args[0]);
        System.out.println("Stopwords file: " + args[1]);
        System.out.println("Output directory: " + args[2]);
        
        Job job = Job.getInstance(conf, "Stock Word Count");
        job.setJarByClass(StockWordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // 输入文件
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        // 停用词文件添加到分布式缓存
        job.addCacheFile(new URI(args[1]));
        
        // 输出目录
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}