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
 
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件功能说明：SequenceFile文件的输入格式过滤器，支持通过自定义过滤规则对SequenceFile中的记录进行采样，
 * 允许MapReduce作业只处理符合过滤条件的记录样本，常用于数据采样场景。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileInputFilter<K, V>
  extends SequenceFileInputFormat<K, V> {
  
  final private static String FILTER_CLASS = org.apache.hadoop.mapreduce.lib.
      input.SequenceFileInputFilter.FILTER_CLASS;

  public SequenceFileInputFilter() {
  }
    
  /**
   * 为指定输入分片创建记录读取器，应用过滤规则读取记录
   * @param split 待处理的文件分片
   * @param job 作业配置对象
   * @param reporter 任务进度上报器
   * @return 过滤后的记录读取器
   * @throws IOException 创建读取器时IO异常
   */
  public RecordReader<K, V> getRecordReader(InputSplit split,
                                      JobConf job, Reporter reporter)
    throws IOException {
        
    reporter.setStatus(split.toString());
        
    return new FilterRecordReader<K, V>(job, (FileSplit) split);
  }


  /**
   * 将指定过滤类设置到作业配置中，指定作业使用的过滤实现
   * @param conf 应用配置对象
   * @param filterClass 过滤类的Class对象
   */
  public static void setFilterClass(Configuration conf, Class filterClass) {
    conf.set(FILTER_CLASS, filterClass.getName());
  }

         
  /**
   * 过滤器接口，定义记录过滤的统一契约
   */
  public interface Filter extends 
      org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.Filter {
  }
    
  /**
   * 过滤器抽象基类，为所有过滤器提供公共基础实现
   */
  public static abstract class FilterBase extends org.apache.hadoop.mapreduce.
      lib.input.SequenceFileInputFilter.FilterBase
      implements Filter {
  }
    
  /**
   * 基于正则表达式的过滤器，仅保留键匹配正则表达式的记录
   */
  public static class RegexFilter extends FilterBase {
    org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.
      RegexFilter rf;
    /**
     * 将正则表达式模式保存到配置中
     * @param conf 配置对象
     * @param regex 正则表达式字符串
     * @throws PatternSyntaxException 正则语法错误时抛出
     */
    public static void setPattern(Configuration conf, String regex)
        throws PatternSyntaxException {
      org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.
        RegexFilter.setPattern(conf, regex);
    }
        
    public RegexFilter() { 
      rf = new org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.
             RegexFilter();
    }
        
    /**
     * 从配置中加载正则表达式，初始化过滤器
     * @param conf 配置对象
     */
    public void setConf(Configuration conf) {
      rf.setConf(conf);
    }


    /**
     * 判断当前记录是否符合过滤条件
     * @param key 记录的键
     * @return 匹配返回true，保留记录；否则返回false过滤掉记录
     */
    public boolean accept(Object key) {
      return rf.accept(key);
    }
  }

  /**
   * 基于百分比采样的过滤器，按照固定频率对记录进行采样，保留符合记录编号取模条件的记录
   * 例如频率为10时，每10条记录保留1条（记录编号对10取模为0）
   */
  public static class PercentFilter extends FilterBase {
    org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.
	      PercentFilter pf;
    /**
     * 将采样频率保存到配置中
     * @param conf 配置对象
     * @param frequency 采样频率，每frequency条记录保留1条
     */
    public static void setFrequency(Configuration conf, int frequency) {
       org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.
	      PercentFilter.setFrequency(conf, frequency);
    }
	        
    public PercentFilter() { 
      pf = new org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.
        PercentFilter();
    }
	        
    /**
     * 从配置中加载采样频率，初始化过滤器
     * @param conf 配置对象
     */
    public void setConf(Configuration conf) {
      pf.setConf(conf);
    }

    /**
     * 判断当前记录是否符合过滤条件
     * @param key 记录的键
     * @return 取模结果为0返回true，保留记录；否则返回false过滤掉记录
     */
    public boolean accept(Object key) {
      return pf.accept(key);
    }
  }

  /**
   * 基于MD5哈希的过滤器，对键进行MD5哈希后按频率采样，保留哈希值取模符合条件的记录
   * 相比百分比采样，MD5采样可以实现更均匀的随机采样，适合乱序数据
   */
  public static class MD5Filter extends FilterBase {
    public static final int MD5_LEN = org.apache.hadoop.mapreduce.lib.
      input.SequenceFileInputFilter.MD5Filter.MD5_LEN;
    org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.MD5Filter mf;
    /**
     * 将采样频率保存到配置中
     * @param conf 配置对象
     * @param frequency 采样频率，每frequency条记录保留1条
     */
    public static void setFrequency(Configuration conf, int frequency) {
      org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.MD5Filter.
        setFrequency(conf, frequency);
    }
        
    public MD5Filter() { 
      mf = new org.apache.hadoop.mapreduce.lib.input.
        SequenceFileInputFilter.MD5Filter();
    }
        
    /**
     * 从配置中加载采样频率，初始化过滤器
     * @param conf 配置对象
     */
    public void setConf(Configuration conf) {
      mf.setConf(conf);
    }

    /**
     * 判断当前记录是否符合过滤条件
     * @param key 记录的键
     * @return MD5哈希值对频率取模为0返回true，保留记录；否则返回false过滤掉记录
     */
    public boolean accept(Object key) {
      return mf.accept(key);
    }
  }
    
  /**
   * 封装过滤逻辑的记录读取器，在读取SequenceFile记录时应用过滤规则，只返回符合条件的记录
   */
  private static class FilterRecordReader<K, V>
    extends SequenceFileRecordReader<K, V> {
    
    private Filter filter;
        
    /**
     * 构造过滤读取器，根据配置实例化过滤器
     * @param conf 作业配置
     * @param split 文件分片
     * @throws IOException 读取分片时IO异常
     */
    public FilterRecordReader(Configuration conf, FileSplit split)
      throws IOException {
      super(conf, split);
      // 从配置中实例化过滤器，默认使用百分比过滤器
      filter = (Filter)ReflectionUtils.newInstance(
                                                   conf.getClass(FILTER_CLASS, PercentFilter.class), 
                                                   conf);
    }
        
    /**
     * 读取下一条符合过滤条件的记录
     * @param key 存储读取到的键
     * @param value 存储读取到的值
     * @return 成功读取到符合条件的记录返回true，已读完所有记录返回false
     * @throws IOException 读取记录时IO异常
     */
    public synchronized boolean next(K key, V value) throws IOException {
      while (next(key)) {
        if (filter.accept(key)) {
          getCurrentValue(value);
          return true;
        }
      }
            
      return false;
    }
  }
}