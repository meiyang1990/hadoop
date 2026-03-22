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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：输入数据采样器，为TotalOrderPartitioner分区提供采样数据，兼容旧版Mapred API
 * 对输入数据进行采样获取键的分布，用于生成分区划分点，实现全排序作业的数据分区均衡
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class InputSampler<K,V> extends 
  org.apache.hadoop.mapreduce.lib.partition.InputSampler<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(InputSampler.class);

  /**
   * 构造函数，基于旧版MapRed JobConf创建输入采样器
   * @param conf 旧版MapRed作业配置
   */
  public InputSampler(JobConf conf) {
    super(conf);
  }

  /**
   * 生成分区文件并写入作业配置，供TotalOrderPartitioner使用
   * @param job 旧版MapRed作业配置
   * @param sampler 采样器实现，用于获取输入键样本
   * @throws IOException 读取输入或写入分区文件时IO异常
   * @throws ClassNotFoundException 类找不到异常
   * @throws InterruptedException 线程中断异常
   */
  public static <K,V> void writePartitionFile(JobConf job, Sampler<K,V> sampler)
      throws IOException, ClassNotFoundException, InterruptedException {
    writePartitionFile(Job.getInstance(job), sampler);
  }
  /**
   * 采样器接口，兼容旧版MapRed InputFormat，定义获取输入键样本的规范
   * @param <K> 键类型
   * @param <V> 值类型
   */
  public interface Sampler<K,V> extends
    org.apache.hadoop.mapreduce.lib.partition.InputSampler.Sampler<K, V> {
    /**
     * 从输入数据中采集键样本，返回样本数组
     * @param inf 输入格式对象，用于获取输入分片和记录读取
     * @param job 作业配置对象
     * @return 采集到的键样本数组
     * @throws IOException 读取输入时IO异常
     */
    K[] getSample(InputFormat<K,V> inf, JobConf job) throws IOException;
  }

  /**
   * 分片采样器，从每个分片开头采集固定数量记录
   * 适合随机分布数据，采样成本低
   * @param <K> 键类型
   * @param <V> 值类型
   */
  public static class SplitSampler<K,V> extends
      org.apache.hadoop.mapreduce.lib.partition.InputSampler.SplitSampler<K, V>
          implements Sampler<K,V> {

    /**
     * 创建分片采样器，采集所有分片，每个分片采集平均分配数量样本
     * @param numSamples 总共需要采集的样本数量
     */
    public SplitSampler(int numSamples) {
      this(numSamples, Integer.MAX_VALUE);
    }

    /**
     * 创建分片采样器，指定总样本数和最大采样分片数
     * @param numSamples 总共需要采集的样本数量
     * @param maxSplitsSampled 最多采样的分片数量
     */
    public SplitSampler(int numSamples, int maxSplitsSampled) {
      super(numSamples, maxSplitsSampled);
    }

    /**
     * 从选中的分片中采集样本，每个分片取开头指定数量记录
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, JobConf job) throws IOException {
      // 获取所有输入分片
      InputSplit[] splits = inf.getSplits(job, job.getNumMapTasks());
      // 初始化样本列表
      ArrayList<K> samples = new ArrayList<K>(numSamples);
      // 计算实际需要采样的分片数，不超过最大值和总分片数
      int splitsToSample = Math.min(maxSplitsSampled, splits.length);
      // 计算分片采样步长，均匀间隔选择分片
      int splitStep = splits.length / splitsToSample;
      // 计算每个分片需要采集的样本数
      int samplesPerSplit = numSamples / splitsToSample;
      long records = 0;
      // 遍历每个需要采样的分片
      for (int i = 0; i < splitsToSample; ++i) {
        // 获取分片记录读取器
        RecordReader<K,V> reader = inf.getRecordReader(splits[i * splitStep],
            job, Reporter.NULL);
        K key = reader.createKey();
        V value = reader.createValue();
        // 读取分片记录，直到采集够当前分片样本数
        while (reader.next(key, value)) {
          samples.add(key);
          key = reader.createKey();
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
   * 随机采样器，按概率随机选择键，通用采样实现
   * 适合各种输入分布，通过随机选择保证样本代表性
   * @param <K> 键类型
   * @param <V> 值类型
   */
  public static class RandomSampler<K,V> extends
      org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler<K, V>
          implements Sampler<K,V> {

    /**
     * 创建随机采样器，采样所有分片，指定采样频率和总样本数
     * @param freq 键被选中的概率
     * @param numSamples 目标总样本数
     */
    public RandomSampler(double freq, int numSamples) {
      this(freq, numSamples, Integer.MAX_VALUE);
    }

    /**
     * 创建随机采样器，指定采样频率、总样本数和最大采样分片数
     * @param freq 键被选中的概率
     * @param numSamples 目标总样本数
     * @param maxSplitsSampled 最多采样的分片数量
     */
    public RandomSampler(double freq, int numSamples, int maxSplitsSampled) {
      super(freq, numSamples, maxSplitsSampled);
    }

    /**
     * 随机打乱分片顺序，按概率随机选择键，达到样本数后随机替换已有样本
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, JobConf job) throws IOException {
      // 获取所有输入分片
      InputSplit[] splits = inf.getSplits(job, job.getNumMapTasks());
      // 初始化样本列表
      ArrayList<K> samples = new ArrayList<K>(numSamples);
      // 计算实际需要采样的分片数
      int splitsToSample = Math.min(maxSplitsSampled, splits.length);

      // 初始化随机数生成器，记录种子用于调试
      Random r = new Random();
      long seed = r.nextLong();
      r.setSeed(seed);
      LOG.debug("seed: " + seed);
      // 打乱分片顺序，保证随机采样
      for (int i = 0; i < splits.length; ++i) {
        InputSplit tmp = splits[i];
        int j = r.nextInt(splits.length);
        splits[i] = splits[j];
        splits[j] = tmp;
      }
      // 遍历分片采样，直到采够目标样本数或读完选中分片
      for (int i = 0; i < splitsToSample ||
                     (i < splits.length && samples.size() < numSamples); ++i) {
        // 获取当前分片记录读取器
        RecordReader<K,V> reader = inf.getRecordReader(splits[i], job,
            Reporter.NULL);
        K key = reader.createKey();
        V value = reader.createValue();
        // 遍历分片内所有记录，按概率选中样本
        while (reader.next(key, value)) {
          if (r.nextDouble() <= freq) {
            // 样本还没满，直接添加
            if (samples.size() < numSamples) {
              samples.add(key);
            } else {
              // 样本已满，随机替换已有样本，保证每个样本被替换概率均匀
              int ind = r.nextInt(numSamples);
              if (ind != numSamples) {
                samples.set(ind, key);
              }
              // 调整选中概率，抵消替换带来的分布偏移
              freq *= (numSamples - 1) / (double) numSamples;
            }
            key = reader.createKey();
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
  }

  /**
   * 间隔采样器，按固定间隔采样记录，适合已经排序的输入数据
   * @param <K> 键类型
   * @param <V> 值类型
   */
  public static class IntervalSampler<K,V> extends
      org.apache.hadoop.mapreduce.lib.partition.InputSampler.IntervalSampler<K, V>
          implements Sampler<K,V> {

    /**
     * 创建间隔采样器，采样所有分片，指定采样频率
     * @param freq 期望采样比例（保留样本数/总记录数）
     */
    public IntervalSampler(double freq) {
      this(freq, Integer.MAX_VALUE);
    }

    /**
     * 创建间隔采样器，指定采样频率和最大采样分片数
     * @param freq 期望采样比例（保留样本数/总记录数）
     * @param maxSplitsSampled 最多采样的分片数量
     */
    public IntervalSampler(double freq, int maxSplitsSampled) {
      super(freq, maxSplitsSampled);
    }

    /**
     * 按间隔采样，保持实际采样比例接近目标频率，适合已排序数据
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, JobConf job) throws IOException {
      // 获取所有输入分片
      InputSplit[] splits = inf.getSplits(job, job.getNumMapTasks());
      // 初始化样本列表
      ArrayList<K> samples = new ArrayList<K>();
      // 计算实际需要采样的分片数
      int splitsToSample = Math.min(maxSplitsSampled, splits.length);
      // 计算分片采样步长，均匀间隔选择分片
      int splitStep = splits.length / splitsToSample;
      long records = 0;
      long kept = 0;
      // 遍历每个需要采样的分片
      for (int i = 0; i < splitsToSample; ++i) {
        // 获取分片记录读取器
        RecordReader<K,V> reader = inf.getRecordReader(splits[i * splitStep],
            job, Reporter.NULL);
        K key = reader.createKey();
        V value = reader.createValue();
        // 遍历分片内所有记录，按间隔保留样本
        while (reader.next(key, value)) {
          ++records;
          // 当已保留比例小于目标频率时，保留当前记录
          if ((double) kept / records < freq) {
            ++kept;
            samples.add(key);
            key = reader.createKey();
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
  }

}