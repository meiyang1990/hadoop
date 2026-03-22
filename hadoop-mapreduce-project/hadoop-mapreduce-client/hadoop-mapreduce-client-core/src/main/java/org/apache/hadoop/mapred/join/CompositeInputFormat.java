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

package org.apache.hadoop.mapred.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

/**
 * 支持对多个排序和分区方式一致的数据源执行连接操作的InputFormat实现
 * <p>
 * 用户可以通过配置 <code>mapred.join.define.&lt;ident&gt;</code> 指定自定义连接类型类，
 * 在连接表达式 <code>mapred.join.expr</code> 中可以使用该标识符引用自定义的ComposableRecordReader实现。
 * <code>mapred.join.keycomparator</code> 可指定用于连接过程中键比较的自定义比较器类。
 * @see #setFormat
 * @see JoinRecordReader
 * @see MultiFilterRecordReader
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CompositeInputFormat<K extends WritableComparable>
      implements ComposableInputFormat<K,TupleWritable> {

  // 代理InputFormat分片请求的表达式解析树根节点
  private Parser.Node root;

  public CompositeInputFormat() { }


  /**
   * 根据作业配置解析复合连接表达式，初始化连接结构
   * <p>
   * 表达式语法：
   * <pre>
   *   func  ::= &lt;ident&gt;([&lt;func&gt;,]*&lt;func&gt;)
   *   func  ::= tbl(&lt;class&gt;,"&lt;path&gt;")
   *   class ::= 输入格式类名
   *   path  ::= 输入路径字符串
   * </pre>
   * 从配置项 <code>mapred.join.expr</code> 读取连接表达式，
   * 从 <code>mapred.join.define.&lt;ident&gt;</code> 读取用户自定义连接类型，
   * tbl函数提供的路径会被设置为对应InputFormat的输入路径。
   * @see #compose(java.lang.String, java.lang.Class, java.lang.String...)
   * @param job 作业配置对象
   * @throws IOException 解析表达式或加载自定义类型失败时抛出
   */
  public void setFormat(JobConf job) throws IOException {
    addDefaults();
    addUserIdentifiers(job);
    root = Parser.parse(job.get("mapred.join.expr", null), job);
  }

  /**
   * 向解析器添加默认内置连接类型标识符
   */
  protected void addDefaults() {
    try {
      Parser.CNode.addIdentifier("inner", InnerJoinRecordReader.class);
      Parser.CNode.addIdentifier("outer", OuterJoinRecordReader.class);
      Parser.CNode.addIdentifier("override", OverrideRecordReader.class);
      Parser.WNode.addIdentifier("tbl", WrappedRecordReader.class);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("FATAL: Failed to init defaults", e);
    }
  }

  /**
   * 从作业配置中加载用户自定义的连接类型，注册到解析器
   * @param job 作业配置对象
   * @throws IOException 加载用户自定义类型失败时抛出
   */
  private void addUserIdentifiers(JobConf job) throws IOException {
    // 匹配用户自定义连接类型的配置项格式：mapred.join.define.<标识符>
    Pattern x = Pattern.compile("^mapred\\.join\\.define\\.(\\w+)$");
    for (Map.Entry<String,String> kv : job) {
      Matcher m = x.matcher(kv.getKey());
      if (m.matches()) {
        try {
          Parser.CNode.addIdentifier(m.group(1),
              job.getClass(m.group(0), null, ComposableRecordReader.class));
        } catch (NoSuchMethodException e) {
          throw (IOException)new IOException(
              "Invalid define for " + m.group(1)).initCause(e);
        }
      }
    }
  }

  /**
   * 从所有子InputFormat构建复合输入分片，将每个子InputFormat的第i个分片组合为第i个复合分片
   * @param job 作业配置对象
   * @param numSplits 期望分片数量
   * @return 组合后的复合分片数组
   * @throws IOException 获取子分片或构建复合分片失败时抛出
   */
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    setFormat(job);
    // 强制设置最小分片大小为Long.MAX_VALUE，保证所有输入数据作为单个分片，维持连接的分片对齐
    job.setLong("mapred.min.split.size", Long.MAX_VALUE);
    return root.getSplits(job, numSplits);
  }

  /**
   * 根据初始化表达式为当前分片构建复合RecordReader，处理连接逻辑
   * <p>
   * 最外层连接只需要可组合接口，不一定需要是复合实现，这里强制返回TupleWritable是为了统一接口。
   * @param split 输入分片
   * @param job 作业配置对象
   * @param reporter 任务报告器
   * @return 处理连接逻辑的ComposableRecordReader实例
   * @throws IOException 构建RecordReader失败时抛出
   */
  @SuppressWarnings("unchecked") // child types unknown
  public ComposableRecordReader<K,TupleWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    setFormat(job);
    return root.getRecordReader(split, job, reporter);
  }

  /**
   * 便捷方法，为单个输入路径生成tbl连接表达式
   * @param inf 输入格式类
   * @param path 输入路径
   * @return 生成的tbl表达式字符串
   */
  public static String compose(Class<? extends InputFormat> inf, String path) {
    return compose(inf.getName().intern(), path, new StringBuffer()).toString();
  }

  /**
   * 便捷方法，基于同一个输入格式生成带多个表的复合连接表达式
   * @param op 连接操作标识符（inner/outer等）
   * @param inf 输入格式类
   * @param path 多个输入路径数组
   * @return 生成的复合连接表达式字符串
   */
  public static String compose(String op, Class<? extends InputFormat> inf,
      String... path) {
    final String infname = inf.getName();
    StringBuffer ret = new StringBuffer(op + '(');
    for (String p : path) {
      compose(infname, p, ret);
      ret.append(',');
    }
    ret.setCharAt(ret.length() - 1, ')');
    return ret.toString();
  }

  /**
   * 便捷方法，基于同一个输入格式和Path数组生成复合连接表达式
   * @param op 连接操作标识符（inner/outer等）
   * @param inf 输入格式类
   * @param path 多个输入Path数组
   * @return 生成的复合连接表达式字符串
   */
  public static String compose(String op, Class<? extends InputFormat> inf,
      Path... path) {
    ArrayList<String> tmp = new ArrayList<String>(path.length);
    for (Path p : path) {
      tmp.add(p.toString());
    }
    return compose(op, inf, tmp.toArray(new String[0]));
  }

  /**
   * 内部工具方法，向StringBuffer追加单个tbl节点表达式
   * @param inf 输入格式类名
   * @param path 输入路径
   * @param sb 用于拼接的StringBuffer
   * @return 拼接后的StringBuffer
   */
  private static StringBuffer compose(String inf, String path,
      StringBuffer sb) {
    sb.append("tbl(" + inf + ",\"");
    sb.append(path);
    sb.append("\")");
    return sb;
  }

}