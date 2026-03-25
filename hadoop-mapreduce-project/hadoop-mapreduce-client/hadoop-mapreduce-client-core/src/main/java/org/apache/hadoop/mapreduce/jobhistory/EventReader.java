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

package org.apache.hadoop.mapreduce.jobhistory;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.util.StringInterner;

/**
 * MapReduce作业历史事件读取器，负责从持久化的作业历史日志文件中读取Avro序列化的事件数据，
 * 将其反序列化为MapReduce作业历史系统可识别的{@link HistoryEvent}对象，供作业历史查询使用。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class EventReader implements Closeable {
  private String version;
  private Schema schema;
  private DataInputStream in;
  private Decoder decoder;
  private DatumReader reader;

  /**
   * 从指定HDFS路径构造事件读取器，读取作业历史事件
   * @param fs 文件系统对象，用于打开日志文件
   * @param name 日志文件路径
   * @throws IOException 打开文件或读取日志头时发生IO异常
   */
  public EventReader(FileSystem fs, Path name) throws IOException {
    this (fs.open(name));
  }

  /**
   * 从已打开的输入流构造事件读取器，读取版本信息和Schema，初始化解码器
   * @param in 已打开的日志文件输入流
   * @throws IOException 读取日志头或解析Schema时发生异常
   */
  @SuppressWarnings("deprecation")
  public EventReader(DataInputStream in) throws IOException {
    this.in = in;
    // 读取日志版本号
    this.version = in.readLine();

    // 获取当前代码版本Event类的Schema
    Schema myschema = new SpecificData(Event.class.getClassLoader()).getSchema(Event.class);
    Schema.Parser parser = new Schema.Parser();
    // 读取日志中存储的事件Schema定义
    String eventschema = in.readLine();
    if (null != eventschema) {
      try {
        // 解析日志中的事件Schema
        this.schema = parser.parse(eventschema);
        // 创建Avro数据读取器，兼容日志Schema和当前版本Schema
        this.reader = new SpecificDatumReader(schema, myschema);
        // 根据版本选择对应的解码器
        if (EventWriter.VERSION.equals(version)) {
          // JSON格式日志
          this.decoder = DecoderFactory.get().jsonDecoder(schema, in);
        } else if (EventWriter.VERSION_BINARY.equals(version)) {
          // 二进制格式日志
          this.decoder = DecoderFactory.get().binaryDecoder(in, null);
        } else {
          throw new IOException("Incompatible event log version: " + version);
        }
      } catch (AvroRuntimeException e) {
        throw new IOException(e);
      }
    } else {
      throw new IOException("Event schema string not parsed since its null");
    }
  }
  
  /**
   * 从日志流中读取下一个作业历史事件
   * @return 解析后的历史事件，到达文件末尾返回null
   * @throws IOException 读取或反序列化事件时发生异常
   */
  @SuppressWarnings("unchecked")
  public HistoryEvent getNextEvent() throws IOException {
    Event wrapper;
    try {
      // 从Avro流中读取事件包装对象
      wrapper = (Event)reader.read(null, decoder);
    } catch (EOFException e) {            // 到达文件末尾
      return null;
    }
    HistoryEvent result;
    // 根据事件类型创建对应具体事件实例
    switch (wrapper.getType()) {
    case JOB_SUBMITTED:
      result = new JobSubmittedEvent(); break;
    case JOB_INITED:
      result = new JobInitedEvent(); break;
    case JOB_FINISHED:
      result = new JobFinishedEvent(); break;
    case JOB_PRIORITY_CHANGED:
      result = new JobPriorityChangeEvent(); break;
    case JOB_QUEUE_CHANGED:
      result = new JobQueueChangeEvent(); break;
    case JOB_STATUS_CHANGED:
      result = new JobStatusChangedEvent(); break;
    case JOB_FAILED:
      result = new JobUnsuccessfulCompletionEvent(); break;
    case JOB_KILLED:
      result = new JobUnsuccessfulCompletionEvent(); break;
    case JOB_ERROR:
      result = new JobUnsuccessfulCompletionEvent(); break;
    case JOB_INFO_CHANGED:
      result = new JobInfoChangeEvent(); break;
    case TASK_STARTED:
      result = new TaskStartedEvent(); break;
    case TASK_FINISHED:
      result = new TaskFinishedEvent(); break;
    case TASK_FAILED:
      result = new TaskFailedEvent(); break;
    case TASK_UPDATED:
      result = new TaskUpdatedEvent(); break;
    case MAP_ATTEMPT_STARTED:
      result = new TaskAttemptStartedEvent(); break;
    case MAP_ATTEMPT_FINISHED:
      result = new MapAttemptFinishedEvent(); break;
    case MAP_ATTEMPT_FAILED:
      result = new TaskAttemptUnsuccessfulCompletionEvent(); break;
    case MAP_ATTEMPT_KILLED:
      result = new TaskAttemptUnsuccessfulCompletionEvent(); break;
    case REDUCE_ATTEMPT_STARTED:
      result = new TaskAttemptStartedEvent(); break;
    case REDUCE_ATTEMPT_FINISHED:
      result = new ReduceAttemptFinishedEvent(); break;
    case REDUCE_ATTEMPT_FAILED:
      result = new TaskAttemptUnsuccessfulCompletionEvent(); break;
    case REDUCE_ATTEMPT_KILLED:
      result = new TaskAttemptUnsuccessfulCompletionEvent(); break;
    case SETUP_ATTEMPT_STARTED:
      result = new TaskAttemptStartedEvent(); break;
    case SETUP_ATTEMPT_FINISHED:
      result = new TaskAttemptFinishedEvent(); break;
    case SETUP_ATTEMPT_FAILED:
      result = new TaskAttemptUnsuccessfulCompletionEvent(); break;
    case SETUP_ATTEMPT_KILLED:
      result = new TaskAttemptUnsuccessfulCompletionEvent(); break;
    case CLEANUP_ATTEMPT_STARTED:
      result = new TaskAttemptStartedEvent(); break;
    case CLEANUP_ATTEMPT_FINISHED:
      result = new TaskAttemptFinishedEvent(); break;
    case CLEANUP_ATTEMPT_FAILED:
      result = new TaskAttemptUnsuccessfulCompletionEvent(); break;
    case CLEANUP_ATTEMPT_KILLED:
      result = new TaskAttemptUnsuccessfulCompletionEvent(); break;
    case AM_STARTED:
      result = new AMStartedEvent(); break;
    default:
      throw new RuntimeException("unexpected event type: " + wrapper.getType());
    }
    // 将Avro中的数据设置到事件对象中
    result.setDatum(wrapper.getEvent());
    return result;
  }

  /**
   * 关闭事件读取器，释放输入流资源
   * @throws IOException 关闭流时发生IO异常
   */
  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
    in = null;
  }

  /**
   * 将Avro格式的计数器转换为MapReduce原生Counters对象
   * @param counters Avro序列化的计数器对象
   * @return 转换后的原生MapReduce计数器对象
   */
  static Counters fromAvro(JhCounters counters) {
    Counters result = new Counters();
    if(counters != null) {
      // 遍历所有计数器分组
      for (JhCounterGroup g : counters.getGroups()) {
        // 添加分组到结果，使用弱引用字符串驻留节省内存
        CounterGroup group =
            result.addGroup(StringInterner.weakIntern(g.getName().toString()),
                StringInterner.weakIntern(g.getDisplayName().toString()));
        // 遍历分组内所有计数器
        for (JhCounter c : g.getCounts()) {
          // 添加计数器到分组，同样使用字符串驻留
          group.addCounter(StringInterner.weakIntern(c.getName().toString()),
              StringInterner.weakIntern(c.getDisplayName().toString()),
                  c.getValue());
        }
      }
    }
    return result;
  }

}