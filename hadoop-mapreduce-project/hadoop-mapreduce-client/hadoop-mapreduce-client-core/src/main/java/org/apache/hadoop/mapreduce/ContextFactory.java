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
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
 
/**
 * 为兼容Hadoop 0.20版本与后续版本之间的MapReduce Context对象API差异提供工厂类，
 * 允许上层应用在不同版本API下统一创建和克隆Context对象。
 */
public class ContextFactory {

  private static final Constructor<?> JOB_CONTEXT_CONSTRUCTOR;
  private static final Constructor<?> TASK_CONTEXT_CONSTRUCTOR;
  private static final Constructor<?> MAP_CONTEXT_CONSTRUCTOR;
  private static final Constructor<?> MAP_CONTEXT_IMPL_CONSTRUCTOR;
  private static final boolean useV21;
  
  private static final Field REPORTER_FIELD;
  private static final Field READER_FIELD;
  private static final Field WRITER_FIELD;
  private static final Field OUTER_MAP_FIELD;
  private static final Field WRAPPED_CONTEXT_FIELD;
  
  static {
    boolean v21 = true;
    final String PACKAGE = "org.apache.hadoop.mapreduce";
    // 探测当前环境是否使用v21版本API
    try {
      Class.forName(PACKAGE + ".task.JobContextImpl");
    } catch (ClassNotFoundException cnfe) {
      v21 = false;
    }
    useV21 = v21;
    Class<?> jobContextCls;
    Class<?> taskContextCls;
    Class<?> taskIOContextCls;
    Class<?> mapCls;
    Class<?> mapContextCls;
    Class<?> innerMapContextCls;
    try {
      // 根据API版本加载对应Context类
      if (v21) {
        jobContextCls = 
          Class.forName(PACKAGE+".task.JobContextImpl");
        taskContextCls = 
          Class.forName(PACKAGE+".task.TaskAttemptContextImpl");
        taskIOContextCls = 
          Class.forName(PACKAGE+".task.TaskInputOutputContextImpl");
        mapContextCls = Class.forName(PACKAGE + ".task.MapContextImpl");
        mapCls = Class.forName(PACKAGE + ".lib.map.WrappedMapper");
        innerMapContextCls = 
          Class.forName(PACKAGE+".lib.map.WrappedMapper$Context");
      } else {
        jobContextCls = 
          Class.forName(PACKAGE+".JobContext");
        taskContextCls = 
          Class.forName(PACKAGE+".TaskAttemptContext");
        taskIOContextCls = 
          Class.forName(PACKAGE+".TaskInputOutputContext");
        mapContextCls = Class.forName(PACKAGE + ".MapContext");
        mapCls = Class.forName(PACKAGE + ".Mapper");
        innerMapContextCls = 
          Class.forName(PACKAGE+".Mapper$Context");      
      }
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Can't find class", e);
    }
    try {
      // 获取JobContext构造方法并设置可访问
      JOB_CONTEXT_CONSTRUCTOR = 
        jobContextCls.getConstructor(Configuration.class, JobID.class);
      JOB_CONTEXT_CONSTRUCTOR.setAccessible(true);
      // 获取TaskAttemptContext构造方法并设置可访问
      TASK_CONTEXT_CONSTRUCTOR = 
        taskContextCls.getConstructor(Configuration.class, 
                                      TaskAttemptID.class);
      TASK_CONTEXT_CONSTRUCTOR.setAccessible(true);
      // v21版本额外处理，获取MapContextImpl和包装类构造方法
      if (useV21) {
        MAP_CONTEXT_CONSTRUCTOR = 
          innerMapContextCls.getConstructor(mapCls,
                                            MapContext.class);
        MAP_CONTEXT_IMPL_CONSTRUCTOR =
          mapContextCls.getDeclaredConstructor(Configuration.class, 
                                               TaskAttemptID.class,
                                               RecordReader.class,
                                               RecordWriter.class,
                                               OutputCommitter.class,
                                               StatusReporter.class,
                                               InputSplit.class);
        MAP_CONTEXT_IMPL_CONSTRUCTOR.setAccessible(true);
        WRAPPED_CONTEXT_FIELD = 
          innerMapContextCls.getDeclaredField("mapContext");
        WRAPPED_CONTEXT_FIELD.setAccessible(true);
      } else {
        // 旧版本初始化v21特有成员为null
        MAP_CONTEXT_CONSTRUCTOR = 
          innerMapContextCls.getConstructor(mapCls,
                                            Configuration.class, 
                                            TaskAttemptID.class,
                                            RecordReader.class,
                                            RecordWriter.class,
                                            OutputCommitter.class,
                                            StatusReporter.class,
                                            InputSplit.class);
        MAP_CONTEXT_IMPL_CONSTRUCTOR = null;
        WRAPPED_CONTEXT_FIELD = null;
      }
      MAP_CONTEXT_CONSTRUCTOR.setAccessible(true);
      // 通过反射获取需要的内部字段并设置可访问
      REPORTER_FIELD = taskContextCls.getDeclaredField("reporter");
      REPORTER_FIELD.setAccessible(true);
      READER_FIELD = mapContextCls.getDeclaredField("reader");
      READER_FIELD.setAccessible(true);
      WRITER_FIELD = taskIOContextCls.getDeclaredField("output");
      WRITER_FIELD.setAccessible(true);
      OUTER_MAP_FIELD = innerMapContextCls.getDeclaredField("this$0");
      OUTER_MAP_FIELD.setAccessible(true);
    } catch (SecurityException e) {
      throw new IllegalArgumentException("Can't run constructor ", e);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Can't find constructor ", e);
    } catch (NoSuchFieldException e) {
      throw new IllegalArgumentException("Can't find field ", e);
    }
  }

  /**
   * 使用新配置克隆JobContext或TaskAttemptContext对象，保持上下文信息复用配置变更。
   * @param original 原始上下文对象
   * @param conf 新的配置对象
   * @return 克隆后的新上下文对象
   * @throws InterruptedException 
   * @throws IOException 
   */
  @SuppressWarnings("unchecked")
  public static JobContext cloneContext(JobContext original,
                                        Configuration conf
                                        ) throws IOException, 
                                                 InterruptedException {
    try {
      if (original instanceof MapContext<?,?,?,?>) {
        return cloneMapContext((Mapper.Context) original, conf, null, null);        
      } else if (original instanceof ReduceContext<?,?,?,?>) {
        throw new IllegalArgumentException("can't clone ReduceContext");
      } else if (original instanceof TaskAttemptContext) {
        TaskAttemptContext spec = (TaskAttemptContext) original;
        return (JobContext) 
          TASK_CONTEXT_CONSTRUCTOR.newInstance(conf, spec.getTaskAttemptID());
      } else {
        return (JobContext) 
        JOB_CONTEXT_CONSTRUCTOR.newInstance(conf, original.getJobID());           
      }
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Can't clone object", e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Can't clone object", e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Can't clone object", e);
    }
  }
  
  /**
   * 克隆WrappedMapper.Context对象，可选择替换输入读取器和输出写入器，
   * 用于在自定义处理逻辑中修改上下文而不影响原有对象。
   * @param <K1> 输入键类型
   * @param <V1> 输入值类型
   * @param <K2> 输出键类型
   * @param <V2> 输出值类型
   * @param context 待克隆的Map上下文
   * @param conf 新配置
   * @param reader 新的输入读取器，null则从原上下文复制
   * @param writer 新的输出写入器，null则从原上下文复制
   * @return 克隆后的新Context对象，类不一定与原对象相同
   * @throws IOException
   * @throws InterruptedException
   */
  @SuppressWarnings("unchecked")
  public static <K1,V1,K2,V2> Mapper<K1,V1,K2,V2>.Context 
       cloneMapContext(MapContext<K1,V1,K2,V2> context,
                       Configuration conf,
                       RecordReader<K1,V1> reader,
                       RecordWriter<K2,V2> writer
                      ) throws IOException, InterruptedException {
    try {
      // 获取内部Map对象的外部实例引用
      Object outer = OUTER_MAP_FIELD.get(context);
      // 解包v21版本的包装Context，获取实际MapContext实例
      if ("org.apache.hadoop.mapreduce.lib.map.WrappedMapper$Context".equals
            (context.getClass().getName())) {
        context = (MapContext<K1,V1,K2,V2>) WRAPPED_CONTEXT_FIELD.get(context);
      }
      // 未传入新reader/writer时，从原上下文获取复用
      if (reader == null) {
        reader = (RecordReader<K1,V1>) READER_FIELD.get(context);
      }
      if (writer == null) {
        writer = (RecordWriter<K2,V2>) WRITER_FIELD.get(context);
      }
      // 根据API版本构造新Context对象
      if (useV21) {
        Object basis = 
          MAP_CONTEXT_IMPL_CONSTRUCTOR.newInstance(conf, 
                                                   context.getTaskAttemptID(),
                                                   reader, writer,
                                                   context.getOutputCommitter(),
                                                   REPORTER_FIELD.get(context),
                                                   context.getInputSplit());
        return (Mapper.Context) 
          MAP_CONTEXT_CONSTRUCTOR.newInstance(outer, basis);
      } else {
        return (Mapper.Context)
          MAP_CONTEXT_CONSTRUCTOR.newInstance(outer,
                                              conf, context.getTaskAttemptID(),
                                              reader, writer,
                                              context.getOutputCommitter(),
                                              REPORTER_FIELD.get(context),
                                              context.getInputSplit());
      }
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Can't access field", e);
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Can't create object", e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Can't invoke constructor", e);
    }
  }
}