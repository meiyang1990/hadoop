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

package org.apache.hadoop.mapreduce.lib.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 复合输入格式，用于对多个按相同规则排序和分区的数据源执行连接操作
 * 
 * 用户可通过配置属性定义新的连接类型：将 <code>mapreduce.join.define.&lt;ident&gt;</code> 
 * 设置为对应类名，即可在连接表达式 <code>mapreduce.join.expr</code> 中使用该标识符，
 * 标识符对应一个可组合的记录读取器 ComposableRecordReader。
 * <code>mapreduce.join.keycomparator</code> 可指定用于连接中键比较的自定义比较器类。
 * @see #setFormat
 * @see JoinRecordReader
 * @see MultiFilterRecordReader
 */
@SuppressWarnings("unchecked")
@InterfaceAudience.PPublic
@InterfaceStability.Stable
public class CompositeInputFormat<K extends WritableComparable>
    extends InputFormat<K, TupleWritable> {

  public static final String JOIN_EXPR = "mapreduce.join.expr";
  public static final String JOIN_COMPARATOR = "mapreduce.join.keycomparator";
  
  // 解析连接表达式生成的抽象语法树，输入格式请求会代理到该树处理
  private Parser.Node root;

  public CompositeInputFormat() { }


  /**
   * 根据配置中的连接表达式解析构建复合输入结构
   * {@code
   *   func  ::= <ident>([<func>,]*<func>)
   *   func  ::= tbl(<class>,"<path>")
   *   class ::= @see java.lang.Class#forName(java.lang.String)
   *   path  ::= @see org.apache.hadoop.fs.Path#Path(java.lang.String)
   * }
   * 从 <code>mapreduce.join.expr</code> 配置读取连接表达式，
   * 从 <code>mapreduce.join.define.&lt;ident&gt;</code> 读取用户自定义连接类型，
   * tbl 函数提供的路径会作为输入路径交给指定的 InputFormat 处理。
   * @see #compose(java.lang.String, java.lang.Class, java.lang.String...)
   */
  public void setFormat(Configuration conf) throws IOException {
    addDefaults();
    addUserIdentifiers(conf);
    root = Parser.parse(conf.get(JOIN_EXPR, null), conf);
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
   * 加载用户自定义的连接类型注册到解析器
   */
  private void addUserIdentifiers(Configuration conf) throws IOException {
    // 匹配用户自定义连接类型的配置项格式 mapreduce.join.define.<标识符>
    Pattern x = Pattern.compile("^mapreduce\\.join\\.define\\.(\\w+)$");
    for (Map.Entry<String,String> kv : conf) {
      Matcher m = x.matcher(kv.getKey());
      if (m.matches()) {
        try {
          Parser.CNode.addIdentifier(m.group(1),
              conf.getClass(m.group(0), null, ComposableRecordReader.class));
        } catch (NoSuchMethodException e) {
          throw new IOException("Invalid define for " + m.group(1), e);
        }
      }
    }
  }

  /**
   * 从子输入格式构建复合输入分片，将每个子输入格式的第i个分片组合为第i个复合分片
   */
  @SuppressWarnings("unchecked")
  public List<InputSplit> getSplits(JobContext job) 
      throws IOException, InterruptedException {
    setFormat(job.getConfiguration());
    // 禁止小文件分片，保证每个路径一个分片，确保连接的分片对齐
    job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.minsize", Long.MAX_VALUE);
    // 委托根节点生成分片
    return root.getSplits(job);
  }

  /**
   * 根据连接表达式定义，为输入分片构造组合记录读取器
   * 最外层连接只要求可组合，不一定是复合类型，强制使用TupleWritable作为值类型
   */
  @SuppressWarnings("unchecked") // child types unknown
  public RecordReader<K,TupleWritable> createRecordReader(InputSplit split, 
      TaskAttemptContext taskContext) 
      throws IOException, InterruptedException {
    setFormat(taskContext.getConfiguration());
    // 委托根节点创建记录读取器
    return root.createRecordReader(split, taskContext);
  }

  /**
   * 便捷方法，为单个输入路径生成表节点连接表达式
   * @param inf 输入格式类
   * @param path 输入路径
   * @return 格式化后的tbl连接表达式
   */
  public static String compose(Class<? extends InputFormat> inf, 
      String path) {
    return compose(inf.getName().intern(), path, 
             new StringBuffer()).toString();
  }

  /**
   * 便捷方法，基于多个输入路径生成指定操作的复合连接表达式
   * @param op 连接操作名
   * @param inf 所有路径共用的输入格式类
   * @param path 多个输入路径数组
   * @return 格式化后的连接表达式 {@code <op>(tbl(<inf>,<p1>),...)}
   */
  public static String compose(String op, 
      Class<? extends InputFormat> inf, String... path) {
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
   * 便捷方法，基于Path对象数组生成指定操作的复合连接表达式
   * @param op 连接操作名
   * @param inf 所有路径共用的输入格式类
   * @param path 多个输入Path对象数组
   * @return 格式化后的连接表达式 {@code <op>(tbl(<inf>,<p1>),...)}
   */
  public static String compose(String op, 
      Class<? extends InputFormat> inf, Path... path) {
    ArrayList<String> tmp = new ArrayList<String>(path.length);
    for (Path p : path) {
      tmp.add(p.toString());
    }
    return compose(op, inf, tmp.toArray(new String[0]));
  }

  /**
   * 辅助方法，拼接单个表节点表达式到缓冲区
   * @param inf 输入格式类全限定名
   * @param path 输入路径字符串
   * @param sb 目标字符串缓冲区
   * @return 拼接后的缓冲区
   */
  private static StringBuffer compose(String inf, String path,
      StringBuffer sb) {
    sb.append("tbl(" + inf + ",\"");
    sb.append(path);
    sb.append("\")");
    return sb;
  }
}