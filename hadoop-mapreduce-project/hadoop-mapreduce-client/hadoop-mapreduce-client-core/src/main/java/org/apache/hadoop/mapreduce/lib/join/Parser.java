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

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件说明：MapReduce连接表达式的移位归约语法解析器，用于解析用户定义的多输入连接表达式，生成输入格式抽象语法树
 * 
 * 非常简单的连接表达式移位归约解析器，支持用户扩展自定义连接类型。
 * 当前实现满足基础语法需求，如果后续需要支持更复杂语法则可能需要替换为专业解析器生成器实现。
 * 解析器将用户输入的连接表达式字符串转换为结构化的Node抽象语法树，后续用于生成组合输入格式和记录读取器。
 * 用户可以通过注册自定义节点类型和记录读取器扩展自定义连接逻辑。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Parser {
  /**
   * 解析器词法单元类型枚举，定义连接表达式中允许出现的不同语法元素类型
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public enum TType { CIF, IDENT, COMMA, LPAREN, RPAREN, QUOT, NUM, }

  /**
   * 标签联合类型，存放连接表达式解析后的词法单元，基类定义通用接口
   * @see Parser.TType
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class Token {

    private TType type;

    Token(TType type) {
      this.type = type;
    }

    public TType getType() { return type; }
    
    public Node getNode() throws IOException {
      throw new IOException("Expected nodetype");
    }
    
    public double getNum() throws IOException {
      throw new IOException("Expected numtype");
    }
    
    public String getStr() throws IOException {
      throw new IOException("Expected strtype");
    }
  }

  /**
   * 数值类型词法单元，存储数值字面量
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class NumToken extends Token {
    private double num;
    public NumToken(double num) {
      super(TType.NUM);
      this.num = num;
    }
    public double getNum() { return num; }
  }

  /**
   * 抽象语法树节点类型词法单元，存储已经归约完成的语法树节点
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class NodeToken extends Token {
    private Node node;
    NodeToken(Node node) {
      super(TType.CIF);
      this.node = node;
    }
    public Node getNode() {
      return node;
    }
  }

  /**
   * 字符串类型词法单元，存储标识符、带引号字符串等文本内容
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class StrToken extends Token {
    private String str;
    public StrToken(TType type, String str) {
      super(type);
      this.str = str;
    }
    public String getStr() {
      return str;
    }
  }

  /**
   * 词法分析器，包装StreamTokenizer完成输入字符串，将连接表达式拆分为词法单元流
   */
  private static class Lexer {

    private StreamTokenizer tok;

    /**
     * 构造词法分析器，初始化StreamTokenizer配置
     * @param s 待解析的连接表达式字符串
     */
    Lexer(String s) {
      tok = new StreamTokenizer(new CharArrayReader(s.toCharArray()));
      tok.quoteChar('"');
      tok.parseNumbers();
      tok.ordinaryChar(',');
      tok.ordinaryChar('(');
      tok.ordinaryChar(')');
      tok.wordChars('$','$');
      tok.wordChars('_','_');
    }

    /**
     * 获取下一个词法单元
     * @return 下一个Token，到达输入末尾返回null
     * @throws IOExcepion 遇到未知语法错误
     */
    Token next() throws IOException {
      int type = tok.nextToken();
      switch (type) {
        case StreamTokenizer.TT_EOF:
        case StreamTokenizer.TT_EOL:
          return null;
        case StreamTokenizer.TT_NUMBER:
          return new NumToken(tok.nval);
        case StreamTokenizer.TT_WORD:
          return new StrToken(TType.IDENT, tok.sval);
        case '"':
          return new StrToken(TType.QUOT, tok.sval);
        default:
          switch (type) {
            case ',':
              return new Token(TType.COMMA);
            case '(':
              return new Token(TType.LPAREN);
            case ')':
              return new Token(TType.RPAREN);
            default:
              throw new IOException("Unexpected: " + type);
          }
      }
    }
  }

/**
 * 抽象语法树节点基类，所有连接表达式节点都继承此类，继承自可组合输入格式
 */
@SuppressWarnings("unchecked")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract static class Node extends ComposableInputFormat {
    /**
     * 根据标识符获取对应节点类型，创建新节点实例
     * @param ident 节点标识符名称
     * @return 新建的节点实例
     * @throws IOException 找不到对应节点类型或反射创建失败
     * @see #addIdentifier(java.lang.String, java.lang.Class[], java.lang.Class, java.lang.Class)
     * @see CompositeInputFormat#setFormat(org.apache.hadoop.mapred.JobConf)
     */
    static Node forIdent(String ident) throws IOException {
      try {
        if (!nodeCstrMap.containsKey(ident)) {
          throw new IOException("No nodetype for " + ident);
        }
        return nodeCstrMap.get(ident).newInstance(ident);
      } catch (IllegalAccessException e) {
        throw new IOException(e);
      } catch (InstantiationException e) {
        throw new IOException(e);
      } catch (InvocationTargetException e) {
        throw new IOException(e);
      }
    }

    private static final Class<?>[] ncstrSig = { String.class };
    // 全局标识符到节点构造函数的映射表
    private static final
        Map<String,Constructor<? extends Node>> nodeCstrMap =
        new HashMap<String,Constructor<? extends Node>>();
    // 全局标识符到可组合记录读取器构造函数的映射表
    protected static final Map<String,Constructor<? extends 
        ComposableRecordReader>> rrCstrMap =
        new HashMap<String,Constructor<? extends ComposableRecordReader>>();

    /**
     * 向全局注册新的节点类型和对应的记录读取器类型，建立标识符到构造函数的映射
     * @param ident 节点标识符名称，表达式中使用的名称
     * @param mcstrSig 记录读取器构造函数参数类型签名
     * @param nodetype 节点类型Class对象
     * @param cl 记录读取器类型Class对象
     * @throws NoSuchMethodException 找不到对应构造函数
     */
    protected static void addIdentifier(String ident, Class<?>[] mcstrSig,
                              Class<? extends Node> nodetype,
                              Class<? extends ComposableRecordReader> cl)
        throws NoSuchMethodException {
      Constructor<? extends Node> ncstr =
        nodetype.getDeclaredConstructor(ncstrSig);
      ncstr.setAccessible(true);
      nodeCstrMap.put(ident, ncstr);
      Constructor<? extends ComposableRecordReader> mcstr =
        cl.getDeclaredConstructor(mcstrSig);
      mcstr.setAccessible(true);
      rrCstrMap.put(ident, mcstr);
    }

    // 节点ID，标识在组合节点中的子节点序号
    protected int id = -1;
    // 节点标识符
    protected String ident;
    // 键比较器类，用于连接时键排序
    protected Class<? extends WritableComparator> cmpcl;

    protected Node(String ident) {
      this.ident = ident;
    }

    protected void setID(int id) {
      this.id = id;
    }

    protected void setKeyComparator(
        Class<? extends WritableComparator> cmpcl) {
      this.cmpcl = cmpcl;
    }
    /**
     * 解析词法单元参数列表，构造节点内部结构
     * @param args 词法单元参数列表
     * @param conf 作业配置对象
     * @throws IOException 解析错误
     */
    abstract void parse(List<Token> args, Configuration conf) 
        throws IOException;
  }

  /**
   * 包装输入格式节点，代表一个基础输入格式，对应单个输入源
   */
  static class WNode extends Node {
    private static final Class<?>[] cstrSig =
      { Integer.TYPE, RecordReader.class, Class.class };

    /**
     * 向全局注册当前节点类型和对应的记录读取器
     * @param ident 节点标识符名称
     * @param cl 对应记录读取器类型
     * @throws NoSuchMethodException 找不到对应构造函数
     */
    @SuppressWarnings("unchecked")
	static void addIdentifier(String ident,
                              Class<? extends ComposableRecordReader> cl)
        throws NoSuchMethodException {
      Node.addIdentifier(ident, cstrSig, WNode.class, cl);
    }

    // 输入路径字符串
    private String indir;
    // 包装的输入格式实例
    private InputFormat<?, ?> inf;

    public WNode(String ident) {
      super(ident);
    }

    /**
     * 解析词法参数，第一个参数是输入格式类名，第二个参数是带引号的输入路径
     */
    @Override
    public void parse(List<Token> ll, Configuration conf) throws IOException {
      StringBuilder sb = new StringBuilder();
      Iterator<Token> i = ll.iterator();
      while (i.hasNext()) {
        Token t = i.next();
        if (TType.COMMA.equals(t.getType())) {
          try {
          	inf = (InputFormat<?, ?>)ReflectionUtils.newInstance(
          			conf.getClassByName(sb.toString()), conf);
          } catch (ClassNotFoundException e) {
            throw new IOException(e);
          } catch (IllegalArgumentException e) {
            throw new IOException(e);
          }
          break;
        }
        sb.append(t.getStr());
      }
      if (!i.hasNext()) {
        throw new IOException("Parse error");
      }
      Token t = i.next();
      if (!TType.QUOT.equals(t.getType())) {
        throw new IOException("Expected quoted string");
      }
      indir = t.getStr();
      // no check for ll.isEmpty() to permit extension
    }

    /**
     * 创建配置对象，设置当前节点的输入路径到配置中
     * @param jconf 原始作业配置
     * @return 添加了输入路径的新配置对象
     * @throws IOException 配置创建错误
     */
    private Configuration getConf(Configuration jconf) throws IOException {
      Job job = Job.getInstance(jconf);
      FileInputFormat.setInputPaths(job, indir);
      return job.getConfiguration();
    }
    
    public List<InputSplit> getSplits(JobContext context)
        throws IOException, InterruptedException {
      return inf.getSplits(
                 new JobContextImpl(getConf(context.getConfiguration()), 
                                    context.getJobID()));
    }

    public ComposableRecordReader<?, ?> createRecordReader(InputSplit split, 
        TaskAttemptContext taskContext) 
        throws IOException, InterruptedException {
      try {
        if (!rrCstrMap.containsKey(ident)) {
          throw new IOException("No RecordReader for " + ident);
        }
        Configuration conf = getConf(taskContext.getConfiguration());
        TaskAttemptContext context = 
          new TaskAttemptContextImpl(conf, 
              TaskAttemptID.forName(conf.get(MRJobConfig.TASK_ATTEMPT_ID)), 
              new WrappedStatusReporter(taskContext);
        return rrCstrMap.get(ident).newInstance(id,
            inf.createRecordReader(split, context), cmpcl);
      } catch (IllegalAccessException e) {
        throw new IOException(e);
      } catch (InstantiationException e) {
        throw new IOException(e);
      } catch (InvocationTargetException e) {
        throw new IOException(e);
      }
    }

    public String toString() {
      return ident + "(" + inf.getClass().getName() + ",\"" + indir + "\")";
    }
  }

  /**
   * 包装状态报告器，将原任务上下文的状态报告转发出去，用于包装后的任务上下文
   */
  private static class WrappedStatusReporter extends StatusReporter {

    TaskAttemptContext context;
    
    public WrappedStatusReporter(TaskAttemptContext context) {
      this.context = context; 
    }
    @Override
    public Counter getCounter(Enum<?> name) {
      return context.getCounter(name);
    }

    @Override
    public Counter getCounter(String group, String name) {
      return context.getCounter(group, name);
    }

    @Override
    public void progress() {
      context.progress();
    }

    @Override
    public float getProgress() {
      return context.getProgress();
    }
    
    @Override
    public void setStatus(String status) {
      context.setStatus(status);
    }
  }

  /**
   * 组合输入格式节点，代表一个包含多个子节点的连接组合输入
   */
  static class CNode extends Node {

    private static final Class<?>[] cstrSig =
      { Integer.TYPE, Configuration.class, Integer.TYPE, Class.class };

    /**
     * 向全局注册当前节点类型和对应的记录读取器
     * @param ident 节点标识符名称
     * @param cl 对应记录读取器类型
     * @throws NoSuchMethodException 找不到对应构造函数
     */
    @SuppressWarnings("unchecked")
	static void addIdentifier(String ident,
                              Class<? extends ComposableRecordReader> cl)
        throws NoSuchMethodException {
      Node.addIdentifier(ident, cstrSig, CNode.class, cl);
    }

    // 存储子节点列表
    // 保存所有子输入节点
    private ArrayList<Node> kids = new ArrayList<Node>();

    public CNode(String ident) {
      super(ident);
    }

    @Override
    public void setKeyComparator(Class<? extends WritableComparator> cmpcl) {
      super.setKeyComparator(cmpcl);
      // 向下传播键比较器到所有子节点
      for (Node n : kids) {
        n.setKeyComparator(cmpcl);
      }
    }

    /**
     * 合并所有子节点的输入分片，组合为组合输入分片列表，每个分片对应所有子节点的一个分片
     */
    @SuppressWarnings("unchecked")
	public List<InputSplit> getSplits(JobContext job)
        throws IOException, InterruptedException {
      // 存放每个子节点的分片列表
      List<List<InputSplit>> splits = 
        new ArrayList<List<InputSplit>>(kids.size());
      for (int i = 0; i < kids.size(); ++i) {
        List<InputSplit> tmp = kids.get(i).getSplits(job);
        if (null == tmp) {
          throw new IOException("Error gathering splits from child RReader");
        }
        // 检查所有子节点分片数量必须一致
        if (i > 0 && splits.get(i-1).size() != tmp.size()) {
          throw new IOException("Inconsistent split cardinality from child " +
              i + " (" + splits.get(i-1).size() + "/" + tmp.size() + ")");
        }
        splits.add(i, tmp);
      }
      final int size