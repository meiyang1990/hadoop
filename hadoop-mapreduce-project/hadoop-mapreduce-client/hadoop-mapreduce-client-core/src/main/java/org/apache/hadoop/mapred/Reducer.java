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

import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Closeable;

/** 
 * MapReduce旧API中的Reducer接口，定义了对Mapper输出的分组中间数据进行归约处理的核心规范。
 * 
 * <p>作业的Reducer数量由用户通过 {@link JobConf#setNumReduceTasks(int)} 设置。
 * Reducer实现可以通过 {@link JobConfigurable#configure(JobConf)} 获取作业配置完成初始化，
 * 通过 {@link Closeable#close()} 方法完成资源清理的反初始化。</p>
 *
 * <p>Reducer执行过程分为三个主要阶段：</p>
 * <ol>
 *   <li>
 *   
 *   <b id="Shuffle">混洗阶段</b>
 *   
 *   <p>Reducer需要获取所有Mapper输出中属于当前Reducer分区的数据，框架会通过HTTP从各个Mapper节点拉取对应分区输出。</p>
 *   </li>
 *   
 *   <li>
 *   <b id="Sort">排序阶段</b>
 *   
 *   <p>框架将拉取到的中间数据按照key分组，不同Mapper可能输出相同key，此阶段会将相同key的所有value汇聚在一起。</p>
 *   
 *   <p>混洗和排序阶段是同时进行的，一边拉取数据一边进行合并排序。</p>
 *      
 *   <b id="SecondarySort">二次排序</b>
 *   
 *   <p>如果分组key的比较规则和排序规则不同，可以通过 {@link JobConf#setOutputValueGroupingComparator(Class)}
 *   指定分组比较器，结合 {@link JobConf#setOutputKeyComparatorClass(Class)} 的key排序规则，实现对value的二次排序。</p>
 *   
 *   
 *   例如，需求是找到重复网页，用已知最优网页的URL标记所有重复页，作业配置如下：
 *   <ul>
 *     <li>Map输入Key：网页URL</li>
 *     <li>Map输入Value：网页文档内容</li>
 *     <li>Map输出Key：文档校验和 + URL的PageRank</li>
 *     <li>Map输出Value：网页URL</li>
 *     <li>分区器：按校验和分区</li>
 *     <li>输出Key比较器：先按校验和排序，再按PageRank降序排序</li>
 *     <li>输出值分组比较器：仅按校验和分组</li>
 *   </ul>
 *   </li>
 *   
 *   <li>   
 *   <b id="Reduce">归约阶段</b>
 *   
 *   <p>此阶段对每个分组后的&lt;key, 所有value列表&gt;调用 {@link #reduce(Object, Iterator, OutputCollector, Reporter)}
 *   方法执行自定义归约逻辑。</p>
 *   <p>归约输出结果通常会通过 {@link OutputCollector#collect(Object, Object)} 写入最终文件到 {@link FileSystem}。</p>
 *   </li>
 * </ol>
 * 
 * <p>Reducer的输出不会再次排序。</p>
 * 
 * <p>示例实现：</p>
 * <p><blockquote><pre>
 *     public class MyReducer&lt;K extends WritableComparable, V extends Writable&gt; 
 *     extends MapReduceBase implements Reducer&lt;K, V, K, V&gt; {
 *     
 *       static enum MyCounters { NUM_RECORDS }
 *        
 *       private String reduceTaskId;
 *       private int noKeys = 0;
 *       
 *       public void configure(JobConf job) {
 *         reduceTaskId = job.get(JobContext.TASK_ATTEMPT_ID);
 *       }
 *       
 *       public void reduce(K key, Iterator&lt;V&gt; values,
 *                          OutputCollector&lt;K, V&gt; output, 
 *                          Reporter reporter)
 *       throws IOException {
 *       
 *         // Process
 *         int noValues = 0;
 *         while (values.hasNext()) {
 *           V value = values.next();
 *           
 *           // Increment the no. of values for this key
 *           ++noValues;
 *           
 *           // Process the &lt;key, value&gt; pair (assume this takes a while)
 *           // ...
 *           // ...
 *           
 *           // Let the framework know that we are alive, and kicking!
 *           if ((noValues%10) == 0) {
 *             reporter.progress();
 *           }
 *         
 *           // Process some more
 *           // ...
 *           // ...
 *           
 *           // Output the &lt;key, value&gt; 
 *           output.collect(key, value);
 *         }
 *         
 *         // Increment the no. of &lt;key, list of values&gt; pairs processed
 *         ++noKeys;
 *         
 *         // Increment counters
 *         reporter.incrCounter(NUM_RECORDS, 1);
 *         
 *         // Every 100 keys update application-level status
 *         if ((noKeys%100) == 0) {
 *           reporter.setStatus(reduceTaskId + " processed " + noKeys);
 *         }
 *       }
 *     }
 * </pre></blockquote>
 * 
 * @see Mapper
 * @see Partitioner
 * @see Reporter
 * @see MapReduceBase
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Reducer<K2, V2, K3, V3> extends JobConfigurable, Closeable {
  
  /** 
   * 对同一个key的所有value执行归约处理，是Reducer的核心方法。
   * 
   * <p>框架会对每个分组后的&lt;key, value列表&gt;调用一次此方法。输入key对象不能被修改，
   * 框架会复用传入的key和value对象，如果需要保存对象引用，必须手动克隆对象。
   * 通常会将所有value归约为0个或1个输出值。</p>
   *   
   * <p>归约结果通过 {@link OutputCollector#collect(Object,Object)} 输出。</p>
   *
   * <p>可以通过 {@link Reporter} 报告任务进度，表明任务还在正常运行。如果处理单个key/value组需要较长时间，
   * 必须定期报告进度，否则框架会认为任务超时并将其杀死。也可以通过调整
   * <a href="{@docRoot}/../hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml#mapreduce.task.timeout">
   * mapreduce.task.timeout</a> 参数增大超时时间，设置为0表示关闭超时检查。</p>
   * 
   * @param key 分组后的输入key
   * @param values 当前key对应的所有value迭代器
   * @param output 输出收集器，用于收集归约结果
   * @param reporter 任务进度和指标报告工具
   */
  void reduce(K2 key, Iterator<V2> values,
              OutputCollector<K3, V3> output, Reporter reporter)
    throws IOException;

}