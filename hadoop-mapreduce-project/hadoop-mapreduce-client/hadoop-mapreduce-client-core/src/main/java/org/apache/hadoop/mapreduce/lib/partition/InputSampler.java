// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明：为TotalOrderPartitioner生成分区划分文件的采样工具类
 * 核心功能：对输入数据进行采样，计算出全排序所需的分区划分点，使得每个Reduce分区数据大致均衡
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class InputSampler<K,V> extends Configured implements Tool  {

  private static final Logger LOG = LoggerFactory.getLogger(InputSampler.class);

  /**
   * 打印命令行使用帮助信息
   * @return 错误码-1
   */
  static int printUsage() {
    System.out.println("sampler -r <reduces>\n" +
      "      [-inFormat <input format class>]\n" +
      "      [-keyClass <map input & output key class>]\n" +
      "      [-splitRandom <double pcnt> <numSamples> <maxsplits> | " +
      "             // Sample from random splits at random (general)\n" +
      "       -splitSample <numSamples> <maxsplits> | " +
      "             // Sample from first records in splits (random data)\n"+
      "       -splitInterval <double pcnt> <maxsplits>]" +
      "             // Sample from splits at intervals (sorted data)");
    System.out.println("Default sampler: -splitRandom 0.1 10000 10");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  /**
   * 构造InputSampler实例，使用给定配置初始化
   * @param conf 配置对象
   */
  public InputSampler(Configuration conf) {
    setConf(conf);
  }

  /**
   * 采样器接口，定义从输入数据中获取样本的统一规范
   * @param <K> 键类型
   * @param <V> 值类型
   */
  public interface Sampler<K,V> {
    /**
     * 从给定作业的输入数据中采集样本，返回采样得到的键数组
     * @param inf 输入格式对象
     * @param job 作业对象
     * @return 采样得到的键数组
     * @throws IOException IO异常
     * @throws InterruptedException 中断异常
     */
    K[] getSample(InputFormat<K,V> inf, Job job) 
    throws IOException, InterruptedException;
  }

  /**
   * SplitSampler：从输入分片采样的实现类，采集每个采样分片前N条记录
   * 适用场景：数据随机分布的场景，采样成本低
   */
  public static class SplitSampler<K,V> implements Sampler<K,V> {

    protected final int numSamples;
    protected final int maxSplitsSampled;

    /**
     * 构造SplitSampler，采样所有分片，获取指定总数的样本
     * @param numSamples 总共需要采集的样本数量
     */
    public SplitSampler(int numSamples) {
      this(numSamples, Integer.MAX_VALUE);
    }

    /**
     * 构造SplitSampler，指定总样本数和最大采样分片数
     * @param numSamples 总共需要采集的样本数量
     * @param maxSplitsSampled 最多采样的分片数量
     */
    public SplitSampler(int numSamples, int maxSplitsSampled) {
      this.numSamples = numSamples;
      this.maxSplitsSampled = maxSplitsSampled;
    }

    /**
     * 从每个采样分片中采集前 总样本数/分片数 条记录作为样本
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, Job job) 
        throws IOException, InterruptedException {
      List<InputSplit> splits = inf.getSplits(job);
      ArrayList<K> samples = new ArrayList<K>(numSamples);
      int splitsToSample = Math.min(maxSplitsSampled, splits.size());
      int samplesPerSplit = numSamples / splitsToSample;
      long records = 0;
      for (int i = 0; i < splitsToSample; ++i) {
        // 创建采样任务上下文
        TaskAttemptContext samplingContext = new TaskAttemptContextImpl(
            job.getConfiguration(), new TaskAttemptID());
        // 创建记录读取器并初始化
        RecordReader<K,V> reader = inf.createRecordReader(
            splits.get(i), samplingContext);
        reader.initialize(splits.get(i), samplingContext);
        // 读取记录直到满足每个分片采样数量
        while (reader.nextKeyValue()) {
          samples.add(ReflectionUtils.copy(job.getConfiguration(),
                                           reader.getCurrentKey(), null));
          ++records;
          if ((i+1) * samplesPerSplit <= records) {
            break;
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
  }

  /**
   * RandomSampler：随机采样实现类，通用采样方案
   * 按照概率随机选择键，最终保留指定数量的样本
   */
  public static class RandomSampler<K,V> implements Sampler<K,V> {
    protected double freq;
    protected final int numSamples;
    protected final int maxSplitsSampled;

    /**
     * 构造RandomSampler，采样所有分片
     * @param freq 键被选中的概率
     * @param numSamples 需要保留的总样本数量
     */
    public RandomSampler(double freq, int numSamples) {
      this(freq, numSamples, Integer.MAX_VALUE);
    }

    /**
     * 构造RandomSampler，指定选中概率、样本数和最大采样分片数
     * @param freq 键被选中的概率
     * @param numSamples 需要保留的总样本数量
     * @param maxSplitsSampled 最多采样的分片数量
     */
    public RandomSampler(double freq, int numSamples, int maxSplitsSampled) {
      this.freq = freq;
      this.numSamples = numSamples;
      this.maxSplitsSampled = maxSplitsSampled;
    }

    /**
     * 打乱分片顺序，按概率随机选中键，达到样本数量后用蓄水池算法替换已有样本
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, Job job) 
        throws IOException, InterruptedException {
      List<InputSplit> splits = inf.getSplits(job);
      ArrayList<K> samples = new ArrayList<K>(numSamples);
      int splitsToSample = Math.min(maxSplitsSampled, splits.size());

      // 初始化随机数生成器
      Random r = new Random();
      long seed = r.nextLong();
      r.setSeed(seed);
      LOG.debug("seed: " + seed);
      // 打乱分片顺序，保证随机采样
      for (int i = 0; i < splits.size(); ++i) {
        InputSplit tmp = splits.get(i);
        int j = r.nextInt(splits.size());
        splits.set(i, splits.get(j));
        splits.set(j, tmp);
      }
      // 遍历分片采样，若未达到样本数量则继续采样更多分片
      for (int i = 0; i < splitsToSample ||
                     (i < splits.size() && samples.size() < numSamples); ++i) {
        TaskAttemptContext samplingContext = new TaskAttemptContextImpl(
            job.getConfiguration(), new TaskAttemptID());
        RecordReader<K,V> reader = inf.createRecordReader(
            splits.get(i), samplingContext);
        reader.initialize(splits.get(i), samplingContext);
        while (reader.nextKeyValue()) {
          // 按概率决定是否选中当前键
          if (r.nextDouble() <= freq) {
            if (samples.size() < numSamples) {
              // 样本未满直接添加
              samples.add(ReflectionUtils.copy(job.getConfiguration(),
                                               reader.getCurrentKey(), null));
            } else {
              // 样本已满，蓄水池算法随机替换一个已有样本
              int ind = r.nextInt(numSamples);
              samples.set(ind, ReflectionUtils.copy(job.getConfiguration(),
                               reader.getCurrentKey(), null));
              // 调整概率，保证每个样本被选中概率一致
              freq *= (numSamples - 1) / (double) numSamples;
            }
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
  }

  /**
   * IntervalSampler：间隔采样实现类，按固定间隔采样记录
   * 适用场景：已经有序的数据，能够均匀覆盖整个键范围
   */
  public static class IntervalSampler<K,V> implements Sampler<K,V> {
    protected final double freq;
    protected final int maxSplitsSampled;

    /**
     * 构造IntervalSampler，采样所有分片
     * @param freq 采样频率，即保留样本占总记录数的比例
     */
    public IntervalSampler(double freq) {
      this(freq, Integer.MAX_VALUE);
    }

    /**
     * 构造IntervalSampler，指定采样频率和最大采样分片数
     * @param freq 采样频率，即保留样本占总记录数的比例
     * @param maxSplitsSampled 最多采样的分片数量
     */
    public IntervalSampler(double freq, int maxSplitsSampled) {
      this.freq = freq;
      this.maxSplitsSampled = maxSplitsSampled;
    }

    /**
     * 对每个分片，当已保留样本占当前总读取记录数比例低于采样频率时采样当前记录
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, Job job) 
        throws IOException, InterruptedException {
      List<InputSplit> splits = inf.getSplits(job);
      ArrayList<K> samples = new ArrayList<K>();
      int splitsToSample = Math.min(maxSplitsSampled, splits.size());
      long records = 0;
      long kept = 0;
      for (int i = 0; i < splitsToSample; ++i) {
        TaskAttemptContext samplingContext = new TaskAttemptContextImpl(
            job.getConfiguration(), new TaskAttemptID());
        RecordReader<K,V> reader = inf.createRecordReader(
            splits.get(i), samplingContext);
        reader.initialize(splits.get(i), samplingContext);
        while (reader.nextKeyValue()) {
          ++records;
          // 已保留比例低于目标频率时采样当前记录
          if ((double) kept / records < freq) {
            samples.add(ReflectionUtils.copy(job.getConfiguration(),
                                 reader.getCurrentKey(), null));
            ++kept;
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
  }

  /**
   * 根据采样结果写入分区文件，供TotalOrderPartitioner使用
   * 对采样键排序后均匀划分分区，得到每个分区的分界点并写入分区文件
   * @param job 作业对象
   * @param sampler 采样器实例
   * @throws IOException IO异常
   * @throws ClassNotFoundException 类找不到异常
   * @throws InterruptedException 中断异常
   */
  @SuppressWarnings("unchecked") // getInputFormat, getOutputKeyComparator
  public static <K,V> void writePartitionFile(Job job, Sampler<K,V> sampler) 
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = job.getConfiguration();
    final InputFormat inf = 
        ReflectionUtils.newInstance(job.getInputFormatClass(), conf);
    // 分区数量等于Reduce任务数
    int numPartitions = job.getNumReduceTasks();
    // 获取采样键数组
    K[] samples = (K[])sampler.getSample(inf, job);
    LOG.info("Using " + samples.length + " samples");
    // 获取排序比较器，对采样键排序
    RawComparator<K> comparator =
      (RawComparator<K>) job.getSortComparator();
    Arrays.sort(samples, comparator);
    // 打开输出文件系统创建分区文件
    Path dst = new Path(TotalOrderPartitioner.getPartitionFile(conf));
    FileSystem fs = dst.getFileSystem(conf);
    // 删除已存在的旧分区文件
    fs.delete(dst, false);
    // 使用SequenceFile写入分区分界点
    SequenceFile.Writer writer = SequenceFile.createWriter(fs,
      conf, dst, job.getMapOutputKeyClass(), NullWritable.class);
    NullWritable nullValue = NullWritable.get();
    // 每个分区平均分配样本数量
    float stepSize = samples.length / (float) numPartitions;
    int last = -1;
    // 为每个Reduce分区选择分界点（从第二个分区开始）
    for(int i = 1; i < numPartitions; ++i) {
      int k = Math.round(stepSize * i);
      // 跳过相同键，保证分区分界点递增
      while (last >= k && comparator.compare(samples[last], samples[k]) == 0) {
        ++k;
      }
      // 写入分区分界键
      writer.append(samples[k], nullValue);
      last = k;
    }
    writer.close();
  }

  /**
   * Tool接口实现，处理命令行参数，执行采样生成分区文件
   * @param args 命令行参数
   * @return 执行结果，0表示成功，非0表示失败
   * @throws Exception 执行异常
   */
  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf());
    ArrayList<String> otherArgs = new ArrayList<String>();
    Sampler<K,V> sampler = null;
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-r".equals(args[i])) {
          // 设置Reduce任务数量
          job.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else if ("-inFormat".equals(args[i])) {
          // 设置输入格式类
          job.setInputFormatClass(
              Class.forName(args[++i]).asSubclass(InputFormat.class));
        } else if ("-keyClass".equals(args[i])) {
          // 设置Map输出键类型
          job.setMapOutputKeyClass(
              Class.forName(args[++i]).asSubclass(WritableComparable.class));
        } else if ("-splitSample".equals(args[i])) {
          // 使用SplitSampler
          int numSamples = Integer.parseInt(args[++i]);
          int maxSplits = Integer.parseInt(args[++i]);
          if (0 >= maxSplits) maxSplits = Integer.MAX_VALUE;
          sampler = new SplitSampler<K,V>(numSamples, maxSplits);
        } else if ("-splitRandom".equals(args[i])) {
          // 使用RandomSampler
          double pcnt = Double.parseDouble(args[++i]);
          int numSamples = Integer.parseInt(args[++i]);
          int maxSplits = Integer.parseInt(args[++i]);
          if (0 >= maxSplits) maxSplits = Integer.MAX_VALUE;
          sampler = new RandomSampler<K,V>(pcnt, numSamples, maxSplits);
        } else if ("-splitInterval".equals(args[i])) {
          // 使用IntervalSampler
          double pcnt = Double.parseDouble(args[++i]);
          int maxSplits = Integer.parseInt(args[++i]);
          if (0 >= maxSplits) maxSplits = Integer.MAX_VALUE;
          sampler = new IntervalSampler<K,V>(pcnt, maxSplits);
        } else {
          // 其他参数保留
          otherArgs.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {