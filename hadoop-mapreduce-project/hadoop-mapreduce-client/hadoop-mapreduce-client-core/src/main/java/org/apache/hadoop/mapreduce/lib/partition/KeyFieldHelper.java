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
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.util.UTF8ByteArrayUtils;

/**
 * 按键分段的工具类，为 {@link KeyFieldBasedComparator} 和 {@link KeyFieldBasedPartitioner} 提供支撑。
 * 核心职责是解析用户指定的键域选择表达式，提供根据域位置切分、提取键片段的能力。
 * <p>
 * 键域表达式格式：-k pos1[,pos2]，pos格式为 f[.c][opts]：
 * <ul>
 * <li>f: 域编号，从1开始计数</li>
 * <li>c: 域内字符位置，从1开始计数，pos2中0表示域的最后一个字符</li>
 * <li>opts: 排序选项，支持n(数值排序)、r(倒序)</li>
 * </ul>
 */

class KeyFieldHelper {
  
  /**
   * 描述单个键域选择规则，存储键片段的起止位置和排序选项
   */
  protected static class KeyDescription {
    int beginFieldIdx = 1;
    int beginChar = 1;
    int endFieldIdx = 0;
    int endChar = 0;
    boolean numeric;
    boolean reverse;
    @Override
    public String toString() {
      return "-k" 
             + beginFieldIdx + "." + beginChar + "," 
             + endFieldIdx + "." + endChar 
             + (numeric ? "n" : "") + (reverse ? "r" : "");
    }
  }
  
  private List<KeyDescription> allKeySpecs = new ArrayList<KeyDescription>();
  private byte[] keyFieldSeparator;
  private boolean keySpecSeen = false;
  
  /**
   * 设置域分隔符，将字符串转换为UTF8字节数组存储
   * @param keyFieldSeparator 域分隔符字符串
   */
  public void setKeyFieldSeparator(String keyFieldSeparator) {
    this.keyFieldSeparator =
      keyFieldSeparator.getBytes(StandardCharsets.UTF_8);
  }
  
  /**
   * 为了兼容{@link KeyFieldBasedPartitioner}旧配置参数num.key.fields.for.partition添加的方法
   * 添加一个从start到end的连续域选择规则
   * @param start 起始域编号
   * @param end 结束域编号
   */
  public void setKeyFieldSpec(int start, int end) {
    if (end >= start) {
      KeyDescription k = new KeyDescription();
      k.beginFieldIdx = start;
      k.endFieldIdx = end;
      keySpecSeen = true;
      allKeySpecs.add(k);
    }
  }
  
  /**
   * 获取所有解析完成的键域规则列表
   * @return 所有键域规则
   */
  public List<KeyDescription> keySpecs() {
    return allKeySpecs;
  }
    
  /**
   * 根据域分隔符计算输入字节数组中各个域的长度，结果数组第一个元素为域总数
   * @param b 输入字节数组
   * @param start 起始偏移
   * @param end 结束偏移
   * @return 长度数组，第一个元素存储域总数，后续元素对应每个域的长度
   */
  public int[] getWordLengths(byte []b, int start, int end) {
    // 没有配置键域规则时，整个键视为一个域
    if (!keySpecSeen) {
      return new int[] {1};
    }
    int[] lengths = new int[10];
    int currLenLengths = lengths.length;
    int idx = 1;
    int pos;
    // 遍历查找所有域分隔符
    while ((pos = UTF8ByteArrayUtils.findBytes(b, start, end, 
        keyFieldSeparator)) != -1) {
      // 动态扩容长度数组
      if (++idx == currLenLengths) {
        int[] temp = lengths;
        lengths = new int[(currLenLengths = currLenLengths*2)];
        System.arraycopy(temp, 0, lengths, 0, temp.length);
      }
      // 保存当前域长度，更新起始位置
      lengths[idx - 1] = pos - start;
      start = pos + 1;
    }
    
    // 处理最后一个域
    if (start != end) {
      lengths[idx] = end - start;
    }
    // 存储域总数到第一个元素
    lengths[0] = idx;
    return lengths;
  }
  
  /**
   * 根据键域规则计算键片段在原字节数组中的起始偏移量
   * @param b 原键字节数组
   * @param start 原键起始偏移
   * @param end 原键结束偏移
   * @param lengthIndices 各个域长度数组，由getWordLengths生成
   * @param k 目标键域规则
   * @return 起始偏移量，超出范围返回-1
   */
  public int getStartOffset(byte[]b, int start, int end, 
      int []lengthIndices, KeyDescription k) {
    // 起始域超出实际域总数，返回无效
    if (lengthIndices[0] >= k.beginFieldIdx) {
      int position = 0;
      // 累加之前所有域和分隔符的长度，得到起始域的起始位置
      for (int i = 1; i < k.beginFieldIdx; i++) {
        position += lengthIndices[i] + keyFieldSeparator.length; 
      }
      // 检查起始字符位置是否合法
      if (position + k.beginChar <= (end - start)) {
        return start + position + k.beginChar - 1; 
      }
    }
    return -1;
  }
  
  /**
   * 根据键域规则计算键片段在原字节数组中的结束偏移量
   * @param b 原键字节数组
   * @param start 原键起始偏移
   * @param end 原键结束偏移
   * @param lengthIndices 各个域长度数组，由getWordLengths生成
   * @param k 目标键域规则
   * @return 结束偏移量
   */
  public int getEndOffset(byte[]b, int start, int end, 
      int []lengthIndices, KeyDescription k) {
    // 未指定结束域，默认到整个键结尾
    if (k.endFieldIdx == 0) {
      return end - 1; 
    }
    if (lengthIndices[0] >= k.endFieldIdx) {
      int position = 0;
      int i;
      // 累加之前所有域和分隔符的长度，得到结束域的起始位置
      for (i = 1; i < k.endFieldIdx; i++) {
        position += lengthIndices[i] + keyFieldSeparator.length;
      }
      // 未指定结束字符，默认到当前域结尾
      if (k.endChar == 0) { 
        position += lengthIndices[i];
      }
      // 检查结束字符位置是否合法
      if (position + k.endChar <= (end - start)) {
        return start + position + k.endChar - 1;
      }
      // 超出范围则返回整个键结尾
      return end - 1;
    }
    // 结束域超出实际域总数，返回整个键结尾
    return end - 1;
  }
  
  /**
   * 解析用户配置的完整选项字符串，提取所有键域规则和全局选项
   * @param option 完整选项字符串
   */
  public void parseOption(String option) {
    if (option == null || option.equals("")) {
      // 无选项使用默认比较规则
      return;
    }
    StringTokenizer args = new StringTokenizer(option);
    KeyDescription global = new KeyDescription();
    // 遍历所有参数令牌
    while (args.hasMoreTokens()) {
      String arg = args.nextToken();
      // 全局数值排序选项
      if (arg.equals("-n")) {  
        global.numeric = true;
      }
      // 全局倒序选项
      if (arg.equals("-r")) {
        global.reverse = true;
      }
      // 全局同时指定n和r
      if (arg.equals("-nr")) {
        global.numeric = true;
        global.reverse = true;
      }
      // 解析-k开头的键域规则
      if (arg.startsWith("-k")) {
        KeyDescription k = parseKey(arg, args);
        if (k != null) {
          allKeySpecs.add(k);
          keySpecSeen = true;
        }
      }
    }
    // 为未指定排序选项的键域规则应用全局选项
    for (KeyDescription key : allKeySpecs) {
      if (!(key.reverse | key.numeric)) {
        key.reverse = global.reverse;
        key.numeric = global.numeric;
      }
    }
    // 没有指定任何-k规则，使用全局选项作为默认规则
    if (allKeySpecs.size() == 0) {
      allKeySpecs.add(global);
    }
  }
  
  /**
   * 解析单个-k键域规则，支持-k<参数>和-k <参数>两种格式
   * @param arg 当前参数令牌
   * @param args 整体令牌迭代器
   * @return 解析完成的键域描述，非法输入返回null
   */
  private KeyDescription parseKey(String arg, StringTokenizer args) {
    String keyArgs = null;
    // 参数分离格式：-k <参数>
    if (arg.length() == 2) {
      if (args.hasMoreTokens()) {
        keyArgs = args.nextToken();
      }
    } else {
      // 参数合并格式：-k<参数>，截取参数部分
      keyArgs = arg.substring(2);
    }
    // 无参数返回null
    if (keyArgs == null || keyArgs.length() == 0) {
      return null;
    }
    // 使用分隔符n r . ,分割令牌，保留分隔符
    StringTokenizer st = new StringTokenizer(keyArgs,"nr.,",true);
       
    KeyDescription key = new KeyDescription();
    
    String token;
    // 解析起始域编号
    if (st.hasMoreTokens()) {
      token = st.nextToken();
      key.beginFieldIdx = Integer.parseInt(token);
    }
    // 解析起始域字符位置
    if (st.hasMoreTokens()) {
      token = st.nextToken();
      if (token.equals(".")) {
        token = st.nextToken();
        key.beginChar = Integer.parseInt(token);
        if (st.hasMoreTokens()) {
          token = st.nextToken();
        } else {
          return key;
        }
      } 
      // 解析起始位置后的排序选项
      do {
        if (token.equals("n")) {
          key.numeric = true;
        }
        else if (token.equals("r")) {
          key.reverse = true;
        }
        else break;
        if (st.hasMoreTokens()) {
          token = st.nextToken();
        } else {
          return key;
        }
      } while (true);
      // 解析结束位置部分
      if (token.equals(",")) {
        token = st.nextToken();
        key.endFieldIdx = Integer.parseInt(token);
        if (st.hasMoreTokens()) {
          token = st.nextToken();
          // 解析结束域字符位置
          if (token.equals(".")) {
            token = st.nextToken();
            key.endChar = Integer.parseInt(token);
            if (st.hasMoreTokens()) {
              token = st.nextToken();
            } else {
              return key;
            }
          }
          // 解析结束位置后的排序选项
          do {
            if (token.equals("n")) {
              key.numeric = true;
            }
            else if (token.equals("r")) {
              key.reverse = true;
            }
            else { 
              throw new IllegalArgumentException("Invalid -k argument. " +
               "Must be of the form -k pos1,[pos2], where pos is of the form " +
               "f[.c]nr");
            }
            if (st.hasMoreTokens()) {
              token = st.nextToken();
            } else {
              break;
            }
          } while (true);
        }
        return key;
      }
      throw new IllegalArgumentException("Invalid -k argument. " +
          "Must be of the form -k pos1,[pos2], where pos is of the form " +
          "f[.c]nr");
    }
    return key;
  }
  
  /**
   * 调试用方法，打印键域规则的所有字段信息
   * @param key 待打印的键域描述
   */
  private void printKey(KeyDescription key) {
    System.out.println("key.beginFieldIdx: " + key.beginFieldIdx);
    System.out.println("key.beginChar: " + key.beginChar);
    System.out.println("key.endFieldIdx: " + key.endFieldIdx);
    System.out.println("key.endChar: " + key.endChar);
    System.out.println("key.numeric: " + key.numeric);
    System.out.println("key.reverse: " + key.reverse);
    System.out.println("parseKey over");
  }  
}