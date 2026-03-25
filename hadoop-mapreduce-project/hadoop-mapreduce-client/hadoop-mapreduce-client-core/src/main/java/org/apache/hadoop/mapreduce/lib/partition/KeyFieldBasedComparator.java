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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.partition.KeyFieldHelper.KeyDescription;


/**
 * This comparator implementation provides a subset of the features provided
 * by the Unix/GNU Sort. In particular, the supported features are:
 * -n, (Sort numerically)
 * -r, (Reverse the result of comparison)
 * -k pos1[,pos2], where pos is of the form f[.c][opts], where f is the number
 *  of the field to use, and c is the number of the first character from the
 *  beginning of the field. Fields and character posns are numbered starting
 *  with 1; a character position of zero in pos2 indicates the field's last
 *  character. If '.c' is omitted from pos1, it defaults to 1 (the beginning
 *  of the field); if omitted from pos2, it defaults to 0 (the end of the
 *  field). opts are ordering options (any of 'nr' as described above). 
 * We assume that the fields in the key are separated by 
 * {@link JobContext#MAP_OUTPUT_KEY_FIELD_SEPARATOR}.
 *
 * 基于关键词字段的Text键比较器，实现类似Unix sort的按指定字段排序功能，用于MapReduce阶段的键排序
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class KeyFieldBasedComparator<K, V> extends WritableComparator 
    implements Configurable {
  private KeyFieldHelper keyFieldHelper = new KeyFieldHelper();
  public static String COMPARATOR_OPTIONS = "mapreduce.partition.keycomparator.options";
  private static final byte NEGATIVE = (byte)'-';
  private static final byte ZERO = (byte)'0';
  private static final byte DECIMAL = (byte)'.';
  private Configuration conf;

  /**
   * 配置比较器，从配置中读取分隔符和比较选项并初始化解析
   * @param conf 作业配置对象
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
    String option = conf.get(COMPARATOR_OPTIONS);
    String keyFieldSeparator = conf.get(MRJobConfig.MAP_OUTPUT_KEY_FIELD_SEPARATOR,"\t");
    keyFieldHelper.setKeyFieldSeparator(keyFieldSeparator);
    keyFieldHelper.parseOption(option);
  }

  /**
   * 获取当前配置对象
   * @return 当前作业配置
   */
  public Configuration getConf() {
    return conf;
  }
  
  /**
   * 构造函数，默认绑定Text类型作为比较的键类型
   */
  public KeyFieldBasedComparator() {
    super(Text.class);
  }
    
  /**
   * 比较两个二进制序列化后的Text键，按照配置的字段规则进行比较
   * @param b1 第一个键的字节数组
   * @param s1 第一个键在数组中的起始偏移
   * @param l1 第一个键的总长度
   * @param b2 第二个键的字节数组
   * @param s2 第二个键在数组中的起始偏移
   * @param l2 第二个键的总长度
   * @return 比较结果，负数表示b1小于b2，0表示相等，正数表示b1大于b2
   */
  public int compare(byte[] b1, int s1, int l1,
      byte[] b2, int s2, int l2) {
    // 读取VInt长度编码所占字节数
    int n1 = WritableUtils.decodeVIntSize(b1[s1]);
    int n2 = WritableUtils.decodeVIntSize(b2[s2]);
    List <KeyDescription> allKeySpecs = keyFieldHelper.keySpecs();

    // 没有配置字段规则时，直接按字节全比较
    if (allKeySpecs.size() == 0) {
      return compareBytes(b1, s1 + n1, l1 - n1, b2, s2 + n2, l2 - n2);
    }
    
    // 解析两个键，获取所有字段的起始/结束位置索引
    int []lengthIndicesFirst = 
      keyFieldHelper.getWordLengths(b1, s1 + n1, s1 + l1);
    int []lengthIndicesSecond = 
      keyFieldHelper.getWordLengths(b2, s2 + n2, s2 + l2);
    
    // 按顺序逐个匹配配置的字段规则，一旦分出大小就返回结果
    for (KeyDescription keySpec : allKeySpecs) {
      // 计算第一个键当前指定字段的起止字符偏移
      int startCharFirst = keyFieldHelper.getStartOffset(b1, s1 + n1, s1 + l1,
        lengthIndicesFirst, keySpec);
      int endCharFirst = keyFieldHelper.getEndOffset(b1, s1 + n1, s1 + l1, 
        lengthIndicesFirst, keySpec);
      // 计算第二个键当前指定字段的起止字符偏移
      int startCharSecond = keyFieldHelper.getStartOffset(b2, s2 + n2, s2 + l2,
        lengthIndicesSecond, keySpec);
      int endCharSecond = keyFieldHelper.getEndOffset(b2, s2 + n2, s2 + l2, 
        lengthIndicesSecond, keySpec);
      int result;
      if ((result = compareByteSequence(b1, startCharFirst, endCharFirst, b2, 
          startCharSecond, endCharSecond, keySpec)) != 0) {
        return result;
      }
    }
    // 所有配置字段都相等，返回0
    return 0;
  }
  
  /**
   * 比较两个指定范围的字节序列，根据当前字段规则选择字典序或数值比较，最终处理反转选项
   * @param first 第一个字节数组
   * @param start1 第一个比较段起始偏移
   * @param end1 第一个比较段结束偏移
   * @param second 第二个字节数组
   * @param start2 第二个比较段起始偏移
   * @param end2 第二个比较段结束偏移
   * @param key 当前字段的比较规则
   * @return 比较结果
   */
  private int compareByteSequence(byte[] first, int start1, int end1, 
      byte[] second, int start2, int end2, KeyDescription key) {
    // 处理第一个键不存在该字段的边界情况
    if (start1 == -1) {
      if (key.reverse) {
        return 1;
      }
      return -1;
    }
    // 处理第二个键不存在该字段的边界情况
    if (start2 == -1) {
      if (key.reverse) {
        return -1; 
      }
      return 1;
    }
    int compareResult = 0;
    // 非数值比较，直接按字节字典序比较
    if (!key.numeric) {
      compareResult = compareBytes(first, start1, end1-start1 + 1, second,
        start2, end2 - start2 + 1);
    }
    // 数值比较，按数值大小比较
    if (key.numeric) {
      compareResult = numericalCompare (first, start1, end1, second, start2,
        end2);
    }
    // 如果配置反转，反转比较结果
    if (key.reverse) {
      return -compareResult;
    }
    return compareResult;
  }
  
  /**
   * 数值比较两个字节表示的十进制数，处理正负号、前导零和小数部分
   * @param a 第一个数的字节数组
   * @param start1 第一个数起始偏移
   * @param end1 第一个数结束偏移
   * @param b 第二个数的字节数组
   * @param start2 第二个数起始偏移
   * @param end2 第二个数结束偏移
   * @return 比较结果
   */
  private int numericalCompare (byte[] a, int start1, int end1, 
      byte[] b, int start2, int end2) {
    int i = start1;
    int j = start2;
    int mul = 1;
    byte first_a = a[i];
    byte first_b = b[j];
    // 处理第一个数负号情况
    if (first_a == NEGATIVE) {
      if (first_b != NEGATIVE) {
        // 处理一正一负特殊情况，处理-0.0等于0.0的边界
        return oneNegativeCompare(a, start1 + 1, end1, b, start2, end2);
      }
      i++;
    }
    // 处理第二个数负号情况
    if (first_b == NEGATIVE) {
      if (first_a != NEGATIVE) {
        // 处理一正一负特殊情况，处理-0.0等于0.0的边界
        return -oneNegativeCompare(b, start2+1, end2, a, start1, end1);
      }
      j++;
    }
    // 两个都是负数，结果需要反转符号
    if (first_b == NEGATIVE && first_a == NEGATIVE) {
      mul = -1;
    }

    // 跳过所有前导零
    while (i <= end1) {
      if (a[i] != ZERO) {
        break;
      }
      i++;
    }
    while (j <= end2) {
      if (b[j] != ZERO) {
        break;
      }
      j++;
    }
    
    // 逐字符比较相等的数字，直到遇到第一个不同字符或非数字（可能是小数点）
    while (i <= end1 && j <= end2) {
      if (!isdigit(a[i]) || a[i] != b[j]) {
        break;
      }
      i++; j++;
    }
    // 获取第一个不同位置的字符
    if (i <= end1) {
      first_a = a[i];
    }
    if (j <= end2) {
      first_b = b[j];
    }
    // 保存第一个不相等字符的差值，后续可能作为最终结果
    int firstResult = first_a - first_b;
    
    // 检查是否其中一方遇到了小数点，另一方没有，需要继续比较小数部分
    if ((first_a == DECIMAL && (!isdigit(first_b) || j > end2)) ||
            (first_b == DECIMAL && (!isdigit(first_a) || i > end1))) {
      return ((mul < 0) ? -decimalCompare(a, i, end1, b, j, end2) : 
        decimalCompare(a, i, end1, b, j, end2));
    }
    // 统计整数部分剩余的数字位数，位数多的数值更大
    int numRemainDigits_a = 0;
    int numRemainDigits_b = 0;
    while (i <= end1) {
      if (isdigit(a[i++])) {
        numRemainDigits_a++;
      } else break;
    }
    while (j <= end2) {
      if (isdigit(b[j++])) {
        numRemainDigits_b++;
      } else break;
    }
    int ret = numRemainDigits_a - numRemainDigits_b;
    if (ret == 0) { 
      return ((mul < 0) ? -firstResult : firstResult);
    } else {
      return ((mul < 0) ? -ret : ret);
    }
  }

  /**
   * 判断字节是否是数字字符
   * @param b 输入字节
   * @return 是否为0-9数字
   */
  private boolean isdigit(byte b) {
    if ('0' <= b && b <= '9') {
      return true;
    }
    return false;
  }

  /**
   * 比较两个数字的小数部分，处理小数点后不同长度的情况
   * @param a 第一个数字字节数组
   * @param i 第一个数字从小数点开始的偏移
   * @param end1 第一个数字结束偏移
   * @param b 第二个数字字节数组
   * @param j 第二个数字从小数点开始的偏移
   * @param end2 第二个数字结束偏移
   * @return 比较结果
   */
  private int decimalCompare(byte[] a, int i, int end1, 
                             byte[] b, int j, int end2) {
    if (i > end1) {
      // a已经没有小数部分了，检查b剩下的部分是否有非零数字
      return -decimalCompare1(b, ++j, end2);
    }
    if (j > end2) {
      // b已经没有小数部分了，检查a剩下的部分是否有非零数字
      return decimalCompare1(a, ++i, end1);
    }
    if (a[i] == DECIMAL && b[j] == DECIMAL) {
      // 逐位比较小数部分
      while (i <= end1 && j <= end2) {
        if (a[i] != b[j]) {
          if (isdigit(a[i]) && isdigit(b[j])) {
            // 都是数字，直接返回差值
            return a[i] - b[j];
          }
          if (isdigit(a[i])) {
            // a还有数字，b没有了，a更大
            return 1;
          }
          if (isdigit(b[j])) {
            // b还有数字，a没有了，b更大
            return -1;
          }
          // 都没有数字，相等
          return 0;
        }
        i++; j++;
      }
      if (i > end1 && j > end2) {
        // 双方都比较完了，相等
        return 0;
      }
        
      if (i > end1) {
        // a已经结束，检查b剩余部分是否有非零数字
        return -decimalCompare1(b, j, end2);
      }
      if (j > end2) {
        // b已经结束，检查a剩余部分是否有非零数字
        return decimalCompare1(a, i, end1);
      }
    }
    else if (a[i] == DECIMAL) {
      // a遇到小数点，b没有，比较a剩余部分
      return decimalCompare1(a, ++i, end1);
    }
    else if (b[j] == DECIMAL) {
      // b遇到小数点，a没有，比较b剩余部分
      return -decimalCompare1(b, ++j, end2);
    }
    return 0;
  }
  
  /**
   * 从小数点后开始检查是否存在非零数字，用于判断长度不一致的小数大小
   * @param a 数字字节数组
   * @param i 起始偏移
   * @param end 结束偏移
   * @return 存在非零数字返回1，否则返回0
   */
  private int decimalCompare1(byte[] a, int i, int end) {
    while (i <= end) {
      if (a[i] == ZERO) {
        i++;
        continue;
      }
      if (isdigit(a[i])) {
        // 找到非零数字，原数更大
        return 1;
      } else {
        // 遇到非数字，没有非零数字，相等
        return 0;
      }
    }
    // 所有剩余都是零，相等
    return 0;
  }
  
  /**
   * 处理一个正数一个负数的比较特殊情况，处理-0等于0相等的边界
   * @param a 负数部分字节数组（已经去掉负号）
   * @param start1 负数起始偏移
   * @param end1 负数结束偏移
   * @param b 正数部分字节数组
   * @param start2 正数起始偏移
   * @param end2 正数结束偏移
   * @return 比较结果，负数永远小于正数，除非两个都是0则返回0相等
   */
  private int oneNegativeCompare(byte[] a, int start1, int end1, 
      byte[] b, int start2, int end2) {
    // a负b正，a不是零，则a < b，返回-1
    if (!isZero(a, start1, end1)) {
      return -1;
    }
    // a是零，检查b是否是零
    if (!isZero(b, start2, end2)) {
      return -1;
    }
    // 两个都是零，相等返回0