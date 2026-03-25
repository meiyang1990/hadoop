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
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件说明：MapReduce连接表达式的移位归约语法解析器，用于将用户定义的多数据源连接表达式解析为可执行的组合输入格式树
 * 
 * Very simple shift-reduce parser for join expressions.
 *
 * This should be sufficient for the user extension permitted now, but ought to
 * be replaced with a parser generator if more complex grammars are supported.
 * In particular, this &quot;shift-reduce&quot; parser has no states. Each set
 * of formals requires a different internal node type, which is responsible for
 * interpreting the list of tokens it receives. This is sufficient for the
 * current grammar, but it has several annoying properties that might inhibit
 * extension. In particular, parenthesis are always function calls; an
 * algebraic or filter grammar would not only require a node type, but must
 * also work around the internals of this parser.
 *
 * For most other cases, adding classes to the hierarchy- particularly by
 * extending JoinRecordReader and MultiFilterRecordReader- is fairly
 * straightforward. One need only override the relevant method(s) (usually only
 * {@link CompositeRecordReader#combine}) and include a property to map its
 * value to an identifier in the parser.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Parser {
  /**
   * 枚举类型定义连接表达式中支持的Token类型
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public enum TType { CIF, IDENT, COMMA, LPAREN, RPAREN, QUOT, NUM, }

  /**
   * 联合类型Token基类，存储连接表达式解析出的词法单元
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
   * 存储数值类型的Token实现类
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
   * 存储语法树节点的Token实现类
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
   * 存储字符串类型的Token实现类
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
   * 词法分析器，基于StreamTokenizer将输入表达式字符串切分为Token流
   */
  private static class Lexer {

    private StreamTokenizer tok;

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
     * 获取下一个Token，返回null表示到达输入末尾
     */
    Token next() throws IOException {
      int type = tok.nextToken();
      switch (type) {
        case StreamTokenizer.TT_EOF:
        case StreamTokenizer.TT_EOL:
          return null;
        case StreamTokenizer.TT_NUMBER:
          // 数值类型Token
          return new NumToken(tok.nval);
        case StreamTokenizer.TT_WORD:
          // 标识符类型Token
          return new StrToken(TType.IDENT, tok.sval);
        case '"':
          // 引用字符串类型Token
          return new StrToken(TType.QUOT, tok.sval);
        default:
          switch (type) {
            case ',':
              // 逗号分隔符
              return new Token(TType.COMMA);
            case '(':
              // 左括号
              return new Token(TType.LPAREN);
            case ')':
              // 右括号
              return new Token(TType.RPAREN);
            default:
              throw new IOException("Unexpected: " + type);
          }
      }
    }
  }

  /**
   * 抽象语法树节点基类，定义节点通用能力和扩展接口，实现了ComposableInputFormat接口
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public abstract static class Node implements ComposableInputFormat {
    /**
     * 根据标识符获取对应的节点实例，创建新的节点对象
     * @param ident 节点标识符
     * @return 对应类型的节点实例
     * @throws IOException 找不到对应节点类型或实例化失败时抛出
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
        throw (IOException)new IOException().initCause(e);
      } catch (InstantiationException e) {
        throw (IOException)new IOException().initCause(e);
      } catch (InvocationTargetException e) {
        throw (IOException)new IOException().initCause(e);
      }
    }

    // 节点构造方法参数签名
    private static final Class<?>[] ncstrSig = { String.class };
    // 标识符到节点构造方法的映射表
    private static final
        Map<String,Constructor<? extends Node>> nodeCstrMap =
        new HashMap<String,Constructor<? extends Node>>();
    // 标识符到RecordReader构造方法的映射表
    protected static final
        Map<String,Constructor<? extends ComposableRecordReader>> rrCstrMap =
        new HashMap<String,Constructor<? extends ComposableRecordReader>>();

    /**
     * 注册新的节点类型标识符，关联对应的节点类和RecordReader类
     * @param ident 节点标识符
     * @param mcstrSig RecordReader构造方法参数签名
     * @param nodetype 节点类对象
     * @param cl RecordReader类对象
     * @throws NoSuchMethodException 找不到对应构造方法时抛出
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

    // 实例变量
    protected int id = -1;
    protected String ident;
    protected Class<? extends WritableComparator> cmpcl;

    protected Node(String ident) {
      this.ident = ident;
    }

    protected void setID(int id) {
      this.id = id;
    }

    protected void setKeyComparator(Class<? extends WritableComparator> cmpcl) {
      this.cmpcl = cmpcl;
    }
    
    /**
     * 抽象方法：解析当前节点的参数列表，构建子节点结构
     * @param args Token形式的参数列表
     * @param job 作业配置对象
     * @throws IOException 解析错误时抛出
     */
    abstract void parse(List<Token> args, JobConf job) throws IOException;
  }

  /**
   * 包装已有输入格式的语法树节点类型，用于将普通输入格式包装为可连接的叶子节点
   */
  static class WNode extends Node {
    private static final Class<?>[] cstrSig =
      { Integer.TYPE, RecordReader.class, Class.class };

    /**
     * 注册包装节点标识符到构造方法的映射
     * @param ident 节点标识符
     * @param cl 对应RecordReader类
     * @throws NoSuchMethodException 找不到构造方法时抛出
     */
    static void addIdentifier(String ident,
                              Class<? extends ComposableRecordReader> cl)
        throws NoSuchMethodException {
      Node.addIdentifier(ident, cstrSig, WNode.class, cl);
    }

    private String indir;
    private InputFormat inf;

    public WNode(String ident) {
      super(ident);
    }

    /**
     * 解析包装节点参数：第一个参数是InputFormat类名，第二个参数是输入路径
     */
    public void parse(List<Token> ll, JobConf job) throws IOException {
      StringBuilder sb = new StringBuilder();
      Iterator<Token> i = ll.iterator();
      while (i.hasNext()) {
        Token t = i.next();
        if (TType.COMMA.equals(t.getType())) {
          // 逗号分隔，反射实例化InputFormat
          try {
          	inf = (InputFormat)ReflectionUtils.newInstance(
          			job.getClassByName(sb.toString()),
                job);
          } catch (ClassNotFoundException e) {
            throw (IOException)new IOException().initCause(e);
          } catch (IllegalArgumentException e) {
            throw (IOException)new IOException().initCause(e);
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

    private JobConf getConf(JobConf job) {
      JobConf conf = new JobConf(job);
      // 设置当前包装节点的输入路径
      FileInputFormat.setInputPaths(conf, indir);
      conf.setClassLoader(job.getClassLoader());
      return conf;
    }

    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      // 委托给底层包装的InputFormat生成分片
      return inf.getSplits(getConf(job), numSplits);
    }

    public ComposableRecordReader getRecordReader(
        InputSplit split, JobConf job, Reporter reporter) throws IOException {
      try {
        if (!rrCstrMap.containsKey(ident)) {
          throw new IOException("No RecordReader for " + ident);
        }
        // 创建对应类型的ComposableRecordReader，传入底层RecordReader实例
        return rrCstrMap.get(ident).newInstance(id,
            inf.getRecordReader(split, getConf(job), reporter), cmpcl);
      } catch (IllegalAccessException e) {
        throw (IOException)new IOException().initCause(e);
      } catch (InstantiationException e) {
        throw (IOException)new IOException().initCause(e);
      } catch (InvocationTargetException e) {
        throw (IOException)new IOException().initCause(e);
      }
    }

    public String toString() {
      return ident + "(" + inf.getClass().getName() + ",\"" + indir + "\")";
    }
  }

  /**
   * 组合输入格式的语法树节点类型，用于组合多个子节点实现多数据源连接
   */
  static class CNode extends Node {

    private static final Class<?>[] cstrSig =
      { Integer.TYPE, JobConf.class, Integer.TYPE, Class.class };

    /**
     * 注册组合节点标识符到构造方法的映射
     * @param ident 节点标识符
     * @param cl 对应RecordReader类
     * @throws NoSuchMethodException 找不到构造方法时抛出
     */
    static void addIdentifier(String ident,
                              Class<? extends ComposableRecordReader> cl)
        throws NoSuchMethodException {
      Node.addIdentifier(ident, cstrSig, CNode.class, cl);
    }

    // 存储子节点列表
    private ArrayList<Node> kids = new ArrayList<Node>();

    public CNode(String ident) {
      super(ident);
    }

    public void setKeyComparator(Class<? extends WritableComparator> cmpcl) {
      super.setKeyComparator(cmpcl);
      // 递归设置所有子节点的键比较器
      for (Node n : kids) {
        n.setKeyComparator(cmpcl);
      }
    }

    /**
     * 合并所有子节点的分片，生成组合分片数组
     */
    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      InputSplit[][] splits = new InputSplit[kids.size()][];
      for (int i = 0; i < kids.size(); ++i) {
        // 从每个子节点获取分片
        final InputSplit[] tmp = kids.get(i).getSplits(job, numSplits);
        if (null == tmp) {
          throw new IOException("Error gathering splits from child RReader");
        }
        // 检查所有子节点分片数量一致
        if (i > 0 && splits[i-1].length != tmp.length) {
          throw new IOException("Inconsistent split cardinality from child " +
              i + " (" + splits[i-1].length + "/" + tmp.length + ")");
        }
        splits[i] = tmp;
      }
      final int size = splits[0].length;
      // 将对应位置的子分片组合为一个组合分片
      CompositeInputSplit[] ret = new CompositeInputSplit[size];
      for (int i = 0; i < size; ++i) {
        ret[i] = new CompositeInputSplit(splits.length);
        for (int j = 0; j < splits.length; ++j) {
          ret[i].add(splits[j][i]);
        }
      }
      return ret;
    }

    @SuppressWarnings("unchecked") // child types unknowable
    public ComposableRecordReader getRecordReader(
        InputSplit split, JobConf job, Reporter reporter) throws IOException {
      if (!(split instanceof CompositeInputSplit)) {
        throw new IOException("Invalid split type:" +
                              split.getClass().getName());
      }
      final CompositeInputSplit spl = (CompositeInputSplit)split;
      final int capacity = kids.size();
      CompositeRecordReader ret = null;
      try {
        if (!rrCstrMap.containsKey(ident)) {
          throw new IOException("No RecordReader for " + ident);
        }
        // 实例化对应类型的组合RecordReader
        ret = (CompositeRecordReader)
          rrCstrMap.get(ident).newInstance(id, job, capacity, cmpcl);
      } catch (IllegalAccessException e) {
        throw (IOException)new IOException().initCause(e);
      } catch (InstantiationException e) {
        throw (IOException)new IOException().initCause(e);