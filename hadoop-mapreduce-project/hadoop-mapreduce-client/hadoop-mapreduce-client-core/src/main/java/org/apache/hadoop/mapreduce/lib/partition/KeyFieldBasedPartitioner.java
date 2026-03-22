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

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.KeyFieldHelper.KeyDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于键中指定字段分区的分区器，MapReduce中用于根据键的指定字段将映射输出分发到不同Reduce任务
 * 支持灵活的字段范围定义，格式为-k pos1[,pos2]，其中pos格式为f[.c][opts]
 * f表示使用的键字段编号，c表示字段中起始/结束字符位置，字段和字符从1开始编号
 * pos2中的0表示字段的最后一个字符；pos1省略.c时默认值为1（字段开头），pos2省略.c时默认值为0（字段结尾）
 * 可配合{@link KeyFieldBasedComparator}实现基于相同字段的排序
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class KeyFieldBasedPartitioner<K2, V2> extends Partitioner<K2, V2> 
    implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(
                                   KeyFieldBasedPartitioner.class.getName());
  /** 分区器选项配置项名称 */
  public static String PARTITIONER_OPTIONS = 
    "mapreduce.partition.keypartitioner.options";
  /** 用于分区的字段数量 */
  private int numOfPartitionFields;
  
  /** 键字段解析工具实例 */
  private KeyFieldHelper keyFieldHelper = new KeyFieldHelper();
  
  /** 作业配置对象 */
  private Configuration conf;

  /**
   * 设置配置对象，初始化键字段解析工具，读取分区规则配置
   * @param conf 作业配置
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
    keyFieldHelper = new KeyFieldHelper();
    String keyFieldSeparator = 
      conf.get(MRJobConfig.MAP_OUTPUT_KEY_FIELD_SEPARATOR, "\t");
    // 设置键字段分隔符
    keyFieldHelper.setKeyFieldSeparator(keyFieldSeparator);
    // 处理废弃配置项，兼容旧版本
    if (conf.get("num.key.fields.for.partition") != null) {
      LOG.warn("Using deprecated num.key.fields.for.partition. " +
      		"Use mapreduce.partition.keypartitioner.options instead");
      this.numOfPartitionFields = conf.getInt("num.key.fields.for.partition",0);
      keyFieldHelper.setKeyFieldSpec(1,numOfPartitionFields);
    } else {
      // 读取并解析新版分区规则选项
      String option = conf.get(PARTITIONER_OPTIONS);
      keyFieldHelper.parseOption(option);
    }
  }

  /**
   * 获取当前配置对象
   * @return 当前作业配置
   */
  public Configuration getConf() {
    return conf;
  }
  
  /**
   * 根据键的指定字段计算目标Reduce分区编号
   * @param key 输出键
   * @param value 输出值
   * @param numReduceTasks Reduce任务总数
   * @return 目标Reduce分区编号
   */
  public int getPartition(K2 key, V2 value, int numReduceTasks) {
    byte[] keyBytes;

    List <KeyDescription> allKeySpecs = keyFieldHelper.keySpecs();
    // 未配置分区规则时，使用键整体哈希值分区
    if (allKeySpecs.size() == 0) {
      return getPartition(key.toString().hashCode(), numReduceTasks);
    }

    // 将键转换为UTF-8字节数组用于计算
    keyBytes = key.toString().getBytes(StandardCharsets.UTF_8);
    // 空键固定返回分区0
    if (keyBytes.length == 0) {
      return 0;
    }
    
    // 计算所有字段的长度和起始偏移量
    int []lengthIndicesFirst = keyFieldHelper.getWordLengths(keyBytes, 0, 
        keyBytes.length);
    int currentHash = 0;
    // 遍历所有分区键规则，计算哈希值
    for (KeyDescription keySpec : allKeySpecs) {
      // 获取当前键规则对应字符起始偏移量
      int startChar = keyFieldHelper.getStartOffset(keyBytes, 0, 
        keyBytes.length, lengthIndicesFirst, keySpec);
      // 未找到匹配字段，跳过该规则
      if (startChar < 0) {
        continue;
      }
      // 获取当前键规则对应字符结束偏移量
      int endChar = keyFieldHelper.getEndOffset(keyBytes, 0, keyBytes.length, 
          lengthIndicesFirst, keySpec);
      // 累加计算该范围字符的哈希值
      currentHash = hashCode(keyBytes, startChar, endChar, 
          currentHash);
    }
    // 根据最终哈希计算分区编号并返回
    return getPartition(currentHash, numReduceTasks);
  }
  
  /**
   * 对字节数组指定范围计算哈希值，使用迭代乘法哈希算法
   * @param b 键字节数组
   * @param start 起始下标
   * @param end 结束下标
   * @param currentHash 当前累加哈希值
   * @return 计算后的哈希值
   */
  protected int hashCode(byte[] b, int start, int end, int currentHash) {
    for (int i = start; i <= end; i++) {
      currentHash = 31*currentHash + b[i];
    }
    return currentHash;
  }

  /**
   * 根据哈希值和Reduce任务数计算最终分区编号
   * @param hash 输入哈希值
   * @param numReduceTasks Reduce任务总数
   * @return 分区编号（0 ~ numReduceTasks-1）
   */
  protected int getPartition(int hash, int numReduceTasks) {
    // 通过按位与消除负数，取模得到分区编号
    return (hash & Integer.MAX_VALUE) % numReduceTasks;
  }
  
  /**
   * 设置分区器的键规则选项到作业配置
   * @param job 作业对象
   * @param keySpec 键分区规则描述，格式为-k pos1[,pos2]
   */
  public void setKeyFieldPartitionerOptions(Job job, String keySpec) {
    job.getConfiguration().set(PARTITIONER_OPTIONS, keySpec);
  }
  
  /**
   * 从作业上下文获取分区器的键规则选项
   * @param job 作业上下文
   * @return 键分区规则描述字符串
   */
  public String getKeyFieldPartitionerOption(JobContext job) {
    return job.getConfiguration().get(PARTITIONER_OPTIONS);
  }


}