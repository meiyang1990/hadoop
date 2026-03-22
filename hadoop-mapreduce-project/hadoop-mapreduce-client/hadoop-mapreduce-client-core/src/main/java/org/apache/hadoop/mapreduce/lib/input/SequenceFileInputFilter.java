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
 
package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件概述：SequenceFile 采样输入格式，允许 MapReduce 作业从 SequenceFile 中按自定义规则抽取样本数据
 * 核心功能：通过可配置的过滤器筛选输入记录，实现对大体积 SequenceFile 的抽样处理
 * A class that allows a map/red job to work on a sample of sequence files.
 * The sample is decided by the filter class set by the job.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileInputFilter<K, V>
    extends SequenceFileInputFormat<K, V> {
  public static final Logger LOG =
      LoggerFactory.getLogger(FileInputFormat.class);
  
  /** 配置项：过滤器实现类的全类名 */
  final public static String FILTER_CLASS = 
    "mapreduce.input.sequencefileinputfilter.class";
  /** 配置项：采样频率，用于百分比和MD5过滤器 */
  final public static String FILTER_FREQUENCY = 
    "mapreduce.input.sequencefileinputfilter.frequency";
  /** 配置项：正则表达式过滤器的匹配规则 */
  final public static String FILTER_REGEX = 
    "mapreduce.input.sequencefileinputfilter.regex";
    
  public SequenceFileInputFilter() {
  }
    
  /** 
   * 为指定输入分片创建带过滤功能的记录读取器
   * @param split 文件分片
   * @param context 任务尝试上下文
   * @return 带过滤功能的RecordReader实例
   */
  public RecordReader<K, V> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    context.setStatus(split.toString());
    return new FilterRecordReader<K, V>(context.getConfiguration());
  }


  /**
   * 为作业设置自定义过滤器实现类
   * @param job 当前作业对象
   * @param filterClass 过滤器实现类
   */
  public static void setFilterClass(Job job, Class<?> filterClass) {
    job.getConfiguration().set(FILTER_CLASS, filterClass.getName());
  }

         
  /**
   * 记录过滤器接口，所有自定义过滤器必须实现该接口
   */
  public interface Filter extends Configurable {
    /**
     * 判断当前记录是否被接受（保留）
     * @param key 当前记录的键
     * @return true 保留该记录；false 过滤掉该记录
     */
    public abstract boolean accept(Object key);
  }
    
  /**
   * 过滤器抽象基类，实现了Configurable接口的基础方法，供具体过滤器继承
   */
  public static abstract class FilterBase implements Filter {
    Configuration conf;
        
    public Configuration getConf() {
      return conf;
    }
  }
    
  /**
   * 基于正则表达式的过滤器，仅保留键匹配正则规则的记录
   */
  public static class RegexFilter extends FilterBase {
    private Pattern p;
    /**
     * 校验并设置正则表达式到配置中
     * @param conf 配置对象
     * @param regex 过滤用正则表达式
     * @throws PatternSyntaxException 正则语法错误时抛出
     */
    public static void setPattern(Configuration conf, String regex)
        throws PatternSyntaxException {
      try {
        Pattern.compile(regex);
      } catch (PatternSyntaxException e) {
        throw new IllegalArgumentException("Invalid pattern: "+regex);
      }
      conf.set(FILTER_REGEX, regex);
    }
        
    public RegexFilter() { }
        
    /**
     * 从配置中读取正则表达式并编译，完成过滤器初始化
     */
    public void setConf(Configuration conf) {
      String regex = conf.get(FILTER_REGEX);
      if (regex == null)
        throw new RuntimeException(FILTER_REGEX + "not set");
      this.p = Pattern.compile(regex);
      this.conf = conf;
    }


    /**
     * 通过正则匹配判断是否接受当前记录
     * @see Filter#accept(Object)
     */
    public boolean accept(Object key) {
      return p.matcher(key.toString()).matches();
    }
  }

  /**
   * 百分比采样过滤器，按固定频率保留记录：每N条记录保留第一条，N由频率参数指定
   * 例如频率为10时，每10条记录保留1条，整体采样率约为10%
   */
  public static class PercentFilter extends FilterBase {
    private int frequency;
    private int count;

    /**
     * 设置采样频率到配置中
     * @param conf 配置对象
     * @param frequency 采样频率，每frequency条保留1条
     */
    public static void setFrequency(Configuration conf, int frequency) {
      if (frequency <= 0)
        throw new IllegalArgumentException(
          "Negative " + FILTER_FREQUENCY + ": " + frequency);
      conf.setInt(FILTER_FREQUENCY, frequency);
    }
        
    public PercentFilter() { }
        
    /**
     * 从配置中读取采样频率，完成过滤器初始化
     * @param conf 配置对象
     */
    public void setConf(Configuration conf) {
      this.frequency = conf.getInt(FILTER_FREQUENCY, 10);
      if (this.frequency <= 0) {
        throw new RuntimeException(
          "Negative "+FILTER_FREQUENCY + ": " + this.frequency);
      }
      this.conf = conf;
    }

    /**
     * 按固定频率判断是否接受当前记录
     * @see Filter#accept(Object)
     */
    public boolean accept(Object key) {
      boolean accepted = false;
      if (count == 0)
        accepted = true;
      if (++count == frequency) {
        count = 0;
      }
      return accepted;
    }
  }

  /**
   * 基于MD5哈希的随机采样过滤器，对键计算MD5哈希，保留哈希值能被频率整除的记录
   * 相比百分比过滤器，该过滤器可以实现随机抽样，避免顺序采样带来的分布偏差
   */
  public static class MD5Filter extends FilterBase {
    private int frequency;
    private static final MessageDigest DIGESTER;
    public static final int MD5_LEN = 16;
    private byte [] digest = new byte[MD5_LEN];
        
    static {
      try {
        DIGESTER = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    }


    /**
     * 设置采样频率到配置中
     * @param conf 配置对象
     * @param frequency 采样频率，保留哈希值能被frequency整除的记录
     */
    public static void setFrequency(Configuration conf, int frequency) {
      if (frequency <= 0)
        throw new IllegalArgumentException(
          "Negative " + FILTER_FREQUENCY + ": " + frequency);
      conf.setInt(FILTER_FREQUENCY, frequency);
    }
        
    public MD5Filter() { }
        
    /**
     * 从配置中读取采样频率，完成过滤器初始化
     * @param conf 配置对象
     */
    public void setConf(Configuration conf) {
      this.frequency = conf.getInt(FILTER_FREQUENCY, 10);
      if (this.frequency <= 0) {
        throw new RuntimeException(
          "Negative " + FILTER_FREQUENCY + ": " + this.frequency);
      }
      this.conf = conf;
    }

    /**
     * 对键计算MD5哈希，按哈希结果判断是否接受当前记录
     * @see Filter#accept(Object)
     */
    public boolean accept(Object key) {
      try {
        long hashcode;
        // 根据键类型选择不同的哈希计算方式
        if (key instanceof Text) {
          hashcode = MD5Hashcode((Text)key);
        } else if (key instanceof BytesWritable) {
          hashcode = MD5Hashcode((BytesWritable)key);
        } else {
          // 其他类型转为字符串后计算哈希
          ByteBuffer bb;
          bb = Text.encode(key.toString());
          hashcode = MD5Hashcode(bb.array(), 0, bb.limit());
        }
        // 哈希值能被频率整除则保留
        if (hashcode / frequency * frequency == hashcode)
          return true;
      } catch(Exception e) {
        LOG.warn(e.toString());
        throw new RuntimeException(e);
      }
      return false;
    }
        
    private long MD5Hashcode(Text key) throws DigestException {
      return MD5Hashcode(key.getBytes(), 0, key.getLength());
    }
        
    private long MD5Hashcode(BytesWritable key) throws DigestException {
      return MD5Hashcode(key.getBytes(), 0, key.getLength());
    }
    
    /**
     * 对字节数组计算MD5哈希，并提取前8字节转为long型哈希值
     * @param bytes 原始字节数组
     * @param start 起始偏移
     * @param length 数据长度
     * @return 转换后的long型哈希值
     * @throws DigestException MD5摘要计算异常时抛出
     */
    synchronized private long MD5Hashcode(byte[] bytes, 
        int start, int length) throws DigestException {
      DIGESTER.update(bytes, 0, length);
      DIGESTER.digest(digest, 0, MD5_LEN);
      long hashcode=0;
      // 将前8字节哈希组装为long型
      for (int i = 0; i < 8; i++)
        hashcode |= ((digest[i] & 0xffL) << (8 * (7 - i)));
      return hashcode;
    }
  }
    
  /**
   * 带过滤功能的记录读取器，包装原生SequenceFileRecordReader，仅返回通过过滤器的记录
   */
  private static class FilterRecordReader<K, V>
      extends SequenceFileRecordReader<K, V> {
    
    private Filter filter;
    private K key;
    private V value;
        
    /**
     * 构造函数，从配置实例化过滤器
     * @param conf 配置对象
     * @throws IOException 初始化失败时抛出
     */
    public FilterRecordReader(Configuration conf)
        throws IOException {
      super();
      // 通过反射实例化配置指定的过滤器
      filter = (Filter)ReflectionUtils.newInstance(
        conf.getClass(FILTER_CLASS, PercentFilter.class), conf);
    }
    
    /**
     * 迭代读取下一条通过过滤的记录
     * @throws IOException IO异常
     * @throws InterruptedException 中断异常
     * @return 是否存在下一条有效记录
     */
    public synchronized boolean nextKeyValue() 
        throws IOException, InterruptedException {
      // 循环读取直到找到符合过滤条件的记录或读取完毕
      while (super.nextKeyValue()) {
        key = super.getCurrentKey();
        if (filter.accept(key)) {
          value = super.getCurrentValue();
          return true;
        }
      }
      return false;
    }
    
    @Override
    public K getCurrentKey() {
      return key;
    }
    
    @Override
    public V getCurrentValue() {
      return value;
    }
  }
}