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

package org.apache.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.task.annotation.Checkpointable;

import java.util.Iterator;

/** 
 * MapReduce Reduce阶段核心抽象基类，对相同key的一组中间值进行归约计算，得到最终输出结果。
 * 
 * <p><code>Reducer</code>实现类可以通过 {@link JobContext#getConfiguration()} 方法获取作业配置信息。</p>
 *
 * <p>Reduce处理整体分为3个核心阶段:</p>
 * <ol>
 *   <li>
 *   
 *   <b id="Shuffle">混洗阶段</b>
 *   
 *   <p>Reducer通过HTTP网络从各个Mapper节点拉取排序后的输出数据。</p>
 *   </li>
 *   
 *   <li>
 *   <b id="Sort">排序阶段</b>
 *   
 *   <p>MapReduce框架对拉取到的输入数据按key进行合并排序，不同Mapper输出的相同key会被聚合到一起。</p>
 *   
 *   <p>混洗和排序阶段是同时进行的，即在拉取数据的同时完成合并。</p>
 *      
 *   <b id="SecondarySort">二次排序</b>
 *   
 *   <p>如果需要对value迭代器返回的值进行二次排序，应用可以将二级排序键合并到主key中，
 *   并定义分组比较器。完整key会被用于排序，而分组比较器会决定哪些key-value对被分到同一
 *   次reduce调用。分组比较器通过 {@link Job#setGroupingComparatorClass(Class)} 指定，
 *   整体排序顺序通过 {@link Job#setSortComparatorClass(Class)} 控制。</p>
 *   
 *   
 *   例如需要找出重复网页，并使用"最佳"页面的URL标记所有重复页面，可以按如下方式配置作业:
 *   <ul>
 *     <li>Map输入键: url</li>
 *     <li>Map输入值: 文档内容</li>
 *     <li>Map输出键: 文档校验和 + URL排名</li>
 *     <li>Map输出值: url</li>
 *     <li>分区器: 按校验和分区</li>
 *     <li>输出键比较器: 先按校验和排序，再按排名降序排序</li>
 *     <li>输出值分组比较器: 仅按校验和分组</li>
 *   </ul>
 *   </li>
 *   
 *   <li>   
 *   <b id="Reduce">归约阶段</b>
 *   
 *   <p>该阶段会对排序后输入中的每个 <code>&lt;key, (value集合)&gt;</code> 调用一次 
 *   {@link #reduce(Object, Iterable, org.apache.hadoop.mapreduce.Reducer.Context)} 方法。</p>
 *   <p>reduce方法的输出通常会通过 {@link Context#write(Object, Object)} 写入到 
 *   {@link RecordWriter} 中，最终输出到存储系统。</p>
 *   </li>
 * </ol>
 * 
 * <p>Reducer的输出不会再次进行排序。</p>
 * 
 * <p>示例:</p>
 * <p><blockquote><pre>
 * public class IntSumReducer&lt;Key&gt; extends Reducer&lt;Key,IntWritable,
 *                                                 Key,IntWritable&gt; {
 *   private IntWritable result = new IntWritable();
 * 
 *   public void reduce(Key key, Iterable&lt;IntWritable&gt; values,
 *                      Context context) throws IOException, InterruptedException {
 *     int sum = 0;
 *     for (IntWritable val : values) {
 *       sum += val.get();
 *     }
 *     result.set(sum);
 *     context.write(key, result);
 *   }
 * }
 * </pre></blockquote>
 * 
 * @see Mapper
 * @see Partitioner
 */
/**
 * MapReduce归约阶段核心抽象基类，负责对相同key的一组中间值进行聚合计算，生成最终输出。
 * 是所有用户自定义Reducer实现的父类，定义了Reduce任务的生命周期和默认执行逻辑。
 * @param <KEYIN> 输入key类型
 * @param <VALUEIN> 输入value类型
 * @param <KEYOUT> 输出key类型
 * @param <VALUEOUT> 输出value类型
 */
@Checkpointable
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

  /**
   * 传递给Reducer实现的上下文对象，提供Reducer访问作业运行环境、输出结果的能力。
   */
  public abstract class Context 
    implements ReduceContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
  }

  /**
   * Reduce任务开始执行前的初始化方法，在整个Reduce任务生命周期仅调用一次。
   * 用于加载全局资源、初始化自定义变量等准备工作，默认实现为空。
   * @param context Reduce上下文对象
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  protected void setup(Context context
                       ) throws IOException, InterruptedException {
    // NOTHING
  }

  /**
   * 对每一个相同key的value集合执行一次归约计算，是Reducer的核心业务方法。
   * 用户通常需要覆盖此方法实现自定义归约逻辑，默认实现是直接输出所有输入键值对（恒等函数）。
   * @param key 聚合后的输入key
   * @param values 当前key对应的所有value迭代器
   * @param context Reduce上下文对象
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  @SuppressWarnings("unchecked")
  protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context
                        ) throws IOException, InterruptedException {
    for(VALUEIN value: values) {
      context.write((KEYOUT) key, (VALUEOUT) value);
    }
  }

  /**
   * Reduce任务所有key处理完成后调用的清理方法，在整个Reduce任务生命周期仅调用一次。
   * 用于关闭资源、输出全局统计信息等收尾工作，默认实现为空。
   * @param context Reduce上下文对象
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  protected void cleanup(Context context
                         ) throws IOException, InterruptedException {
    // NOTHING
  }

  /**
   * Reduce任务的核心执行驱动方法，控制整个Reduce任务的执行流程。
   * 高级应用可以覆盖此方法自定义整个Reduce任务的执行逻辑，默认实现按照标准生命周期执行。
   * @param context Reduce上下文对象
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public void run(Context context) throws IOException, InterruptedException {
    // 执行初始化
    setup(context);
    try {
      // 遍历所有分组key，逐个调用reduce处理
      while (context.nextKey()) {
        reduce(context.getCurrentKey(), context.getValues(), context);
        // 如果使用了后备存储，重置存储状态
        Iterator<VALUEIN> iter = context.getValues().iterator();
        if(iter instanceof ReduceContext.ValueIterator) {
          ((ReduceContext.ValueIterator<VALUEIN>)iter).resetBackupStore();        
        }
      }
    } finally {
      // 执行收尾清理
      cleanup(context);
    }
  }
}