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
package org.apache.hadoop.mapred.lib;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.GenericsUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * 文件级注释：为旧版MapReduce API的ChainMapper和ChainReducer提供公共基础功能，实现多处理单元的链式调用
 * The Chain class provides all the common functionality for the
 * {@link ChainMapper} and the {@link ChainReducer} classes.
 */
/**
 * 链式处理基类，为ChainMapper和ChainReducer提供公共能力，支持多个Map/Reduce处理单元按顺序链式执行
 * 继承自new MapReduce API的Chain基础类，兼容旧版mapred API
 */
class Chain extends org.apache.hadoop.mapreduce.lib.chain.Chain {

  private static final String MAPPER_BY_VALUE = "chain.mapper.byValue";
  private static final String REDUCER_BY_VALUE = "chain.reducer.byValue";

  private JobConf chainJobConf;

  private List<Mapper> mappers = new ArrayList<Mapper>();
  private Reducer reducer;

  // to cache the key/value output class serializations for each chain element
  // to avoid everytime lookup.
  // 缓存每个Map单元输出键的序列化实例，避免重复查找
  private List<Serialization> mappersKeySerialization =
    new ArrayList<Serialization>();
  // 缓存每个Map单元输出值的序列化实例，避免重复查找
  private List<Serialization> mappersValueSerialization =
    new ArrayList<Serialization>();
  // Reduce单元输出键的序列化实例缓存
  private Serialization reducerKeySerialization;
  // Reduce单元输出值的序列化实例缓存
  private Serialization reducerValueSerialization;

  /**
   * 创建链式处理实例，指定链式属于Mapper还是Reducer
   *
   * @param isMap TRUE表示是Mapper的链，FALSE表示是Reducer的链
   */
  Chain(boolean isMap) {
    super(isMap);
  }

  /**
   * 将Mapper类添加到链式任务的JobConf配置中
   * 链式任务本身的配置优先级高于单个Mapper的配置
   *
   * @param isMap            标识当前链属于Mapper还是Reducer
   * @param jobConf          链式任务的JobConf，用于添加Mapper配置
   * @param klass            要添加的Mapper类
   * @param inputKeyClass    Mapper输入键类型
   * @param inputValueClass  Mapper输入值类型
   * @param outputKeyClass   Mapper输出键类型
   * @param outputValueClass Mapper输出值类型
   * @param byValue          是否按值传递键值对给链中下一个Mapper
   * @param mapperConf       Mapper自身的配置，建议使用不加载默认配置的JobConf减少开销
   */
  public static <K1, V1, K2, V2> void addMapper(boolean isMap, JobConf jobConf,
                           Class<? extends Mapper<K1, V1, K2, V2>> klass,
                           Class<? extends K1> inputKeyClass,
                           Class<? extends V1> inputValueClass,
                           Class<? extends K2> outputKeyClass,
                           Class<? extends V2> outputValueClass,
                           boolean byValue, JobConf mapperConf) {
    String prefix = getPrefix(isMap);

    // if a reducer chain check the Reducer has been already set
    checkReducerAlreadySet(isMap, jobConf, prefix, true);
	    
    // set the mapper class
    int index = getIndex(jobConf, prefix);
    jobConf.setClass(prefix + CHAIN_MAPPER_CLASS + index, klass, Mapper.class);
	    
    validateKeyValueTypes(isMap, jobConf, inputKeyClass, inputValueClass,
      outputKeyClass, outputValueClass, index, prefix);
	    
    // if the Mapper does not have a private JobConf create an empty one
    if (mapperConf == null) {
    // using a JobConf without defaults to make it lightweight.
    // still the chain JobConf may have all defaults and this conf is
    // overlapped to the chain JobConf one.
      mapperConf = new JobConf(true);
    }
    // store in the private mapper conf if it works by value or by reference
    mapperConf.setBoolean(MAPPER_BY_VALUE, byValue);
    
    setMapperConf(isMap, jobConf, inputKeyClass, inputValueClass,
	      outputKeyClass, outputValueClass, mapperConf, index, prefix);
  }

  /**
   * 设置Reducer类到链式任务的JobConf配置中
   * 链式任务本身的配置优先级高于Reducer的配置
   *
   * @param jobConf          链式任务的JobConf，用于添加Reducer配置
   * @param klass            要添加的Reducer类
   * @param inputKeyClass    Reducer输入键类型
   * @param inputValueClass  Reducer输入值类型
   * @param outputKeyClass   Reducer输出键类型
   * @param outputValueClass Reducer输出值类型
   * @param byValue          是否按值传递键值对给链中后续Mapper（仅Reducer链生效）
   * @param reducerConf      Reducer自身的配置，建议使用不加载默认配置的JobConf减少开销
   */
  public static <K1, V1, K2, V2> void setReducer(JobConf jobConf,
                          Class<? extends Reducer<K1, V1, K2, V2>> klass,
                          Class<? extends K1> inputKeyClass,
                          Class<? extends V1> inputValueClass,
                          Class<? extends K2> outputKeyClass,
                          Class<? extends V2> outputValueClass,
                          boolean byValue, JobConf reducerConf) {
    String prefix = getPrefix(false);
    checkReducerAlreadySet(false, jobConf, prefix, false);

    jobConf.setClass(prefix + CHAIN_REDUCER_CLASS, klass, Reducer.class);
    
    // if the Reducer does not have a private JobConf create an empty one
    if (reducerConf == null) {
      // using a JobConf without defaults to make it lightweight.
      // still the chain jobConf may have all defaults and this conf is
      // overlapped to the chain jobConf one.
      reducerConf = new JobConf(false);
    }

    // store in the private reducer conf the input/output classes of the reducer
    // and if it works by value or by reference
    reducerConf.setBoolean(REDUCER_BY_VALUE, byValue);

    setReducerConf(jobConf, inputKeyClass, inputValueClass, outputKeyClass,
      outputValueClass, reducerConf, prefix);
  }

  /**
   * 初始化链中所有处理单元，反射实例化并缓存序列化对象
   *
   * @param jobConf 链式任务的JobConf配置
   */
  public void configure(JobConf jobConf) {
    String prefix = getPrefix(isMap);
    chainJobConf = jobConf;
    SerializationFactory serializationFactory =
      new SerializationFactory(chainJobConf);
    // 获取链中Mapper的数量
    int index = jobConf.getInt(prefix + CHAIN_MAPPER_SIZE, 0);
    // 遍历实例化每个Mapper
    for (int i = 0; i < index; i++) {
      Class<? extends Mapper> klass =
        jobConf.getClass(prefix + CHAIN_MAPPER_CLASS + i, null, Mapper.class);
      // 获取当前Mapper的私有配置，与全局配置合并
      JobConf mConf = new JobConf(
        getChainElementConf(jobConf, prefix + CHAIN_MAPPER_CONFIG + i));
      // 反射实例化Mapper
      Mapper mapper = ReflectionUtils.newInstance(klass, mConf);
      mappers.add(mapper);
      // 如果按值传递，缓存序列化实例，否则缓存null
      if (mConf.getBoolean(MAPPER_BY_VALUE, true)) {
        mappersKeySerialization.add(serializationFactory.getSerialization(
          mConf.getClass(MAPPER_OUTPUT_KEY_CLASS, null)));
        mappersValueSerialization.add(serializationFactory.getSerialization(
          mConf.getClass(MAPPER_OUTPUT_VALUE_CLASS, null)));
      } else {
        mappersKeySerialization.add(null);
        mappersValueSerialization.add(null);
      }
    }
    // 实例化Reducer（仅存在于Reducer链）
    Class<? extends Reducer> klass =
      jobConf.getClass(prefix + CHAIN_REDUCER_CLASS, null, Reducer.class);
    if (klass != null) {
      JobConf rConf = new JobConf(
        getChainElementConf(jobConf, prefix + CHAIN_REDUCER_CONFIG));
      reducer = ReflectionUtils.newInstance(klass, rConf);
      // 如果按值传递，缓存序列化实例，否则置空
      if (rConf.getBoolean(REDUCER_BY_VALUE, true)) {
        reducerKeySerialization = serializationFactory
          .getSerialization(rConf.getClass(REDUCER_OUTPUT_KEY_CLASS, null));
        reducerValueSerialization = serializationFactory
          .getSerialization(rConf.getClass(REDUCER_OUTPUT_VALUE_CLASS, null));
      } else {
        reducerKeySerialization = null;
        reducerValueSerialization = null;
      }
    }
  }

  /**
   * 获取当前链式任务的全局JobConf配置
   *
   * @return 链式任务的JobConf
   */
  protected JobConf getChainJobConf() {
    return chainJobConf;
  }

  /**
   * 获取链中第一个Mapper实例
   *
   * @return 第一个Mapper实例，如果没有则返回null
   */
  public Mapper getFirstMap() {
    return (mappers.size() > 0) ? mappers.get(0) : null;
  }

  /**
   * 获取链中Reducer实例
   *
   * @return Reducer实例，如果不存在则返回null
   */
  public Reducer getReducer() {
    return reducer;
  }

  /**
   * 获取指定索引Mapper对应的输出收集器，处理链式调用逻辑
   *
   * @param mapperIndex 当前Mapper在链中的索引
   * @param output      任务原始输出收集器
   * @param reporter    任务进度汇报器
   * @return 适配链式逻辑的输出收集器
   */
  @SuppressWarnings({"unchecked"})
  public OutputCollector getMapperCollector(int mapperIndex,
                                            OutputCollector output,
                                            Reporter reporter) {
    Serialization keySerialization = mappersKeySerialization.get(mapperIndex);
    Serialization valueSerialization =
      mappersValueSerialization.get(mapperIndex);
    return new ChainOutputCollector(mapperIndex, keySerialization,
                                    valueSerialization, output, reporter);
  }

  /**
   * 获取Reducer对应的输出收集器，处理链式调用逻辑
   *
   * @param output   任务原始输出收集器
   * @param reporter 任务进度汇报器
   * @return 适配链式逻辑的输出收集器
   */
  @SuppressWarnings({"unchecked"})
  public OutputCollector getReducerCollector(OutputCollector output,
                                             Reporter reporter) {
    return new ChainOutputCollector(reducerKeySerialization,
                                    reducerValueSerialization, output,
                                    reporter);
  }

  /**
   * 关闭链中所有处理单元，释放资源
   *
   * @throws IOException 任意处理单元关闭时抛出IO异常则向上抛出
   */
  public void close() throws IOException {
    for (Mapper map : mappers) {
      map.close();
    }
    if (reducer != null) {
      reducer.close();
    }
  }

  // using a ThreadLocal to reuse the ByteArrayOutputStream used for ser/deser
  // it has to be a thread local because if not it would break if used from a
  // MultiThreadedMapRunner.
  // 使用ThreadLocal缓存序列化缓冲区，支持多线程环境下的复用，避免并发冲突
  private final ThreadLocal<DataOutputBuffer> threadLocalDataOutputBuffer =
    new ThreadLocal<DataOutputBuffer>() {
      protected DataOutputBuffer initialValue() {
        return new DataOutputBuffer(1024);
      }
    };

  /**
   * 链式处理专用输出收集器，实现处理单元之间的链式调用
   * 非链尾输出会自动调用下一个处理单元，链尾输出才会写入原始收集器
   */
  private class ChainOutputCollector<K, V> implements OutputCollector<K, V> {
    private int nextMapperIndex;
    private Serialization<K> keySerialization;
    private Serialization<V> valueSerialization;
    private OutputCollector output;
    private Reporter reporter;

    /*
     * Mapper专用构造函数
     */
    public ChainOutputCollector(int index, Serialization<K> keySerialization,
                                Serialization<V> valueSerialization,
                                OutputCollector output, Reporter reporter) {
      this.nextMapperIndex = index + 1;
      this.keySerialization = keySerialization;
      this.valueSerialization = valueSerialization;
      this.output = output;
      this.reporter = reporter;
    }

    /*
     * Reducer专用构造函数
     */
    public ChainOutputCollector(Serialization<K> keySerialization,
                                Serialization<V> valueSerialization,
                                OutputCollector output, Reporter reporter) {
      this.nextMapperIndex = 0;
      this.keySerialization = keySerialization;
      this.valueSerialization = valueSerialization;
      this.output = output;
      this.reporter = reporter;
    }

    @SuppressWarnings({"unchecked"})
    public void collect(K key, V value) throws IOException {
      if (nextMapperIndex < mappers.size()) {
        // 链还有下一个Mapper，触发下一个Mapper处理

        // 需要按值传递时，通过序列化深拷贝键值对
        if (keySerialization != null) {
          key = makeCopyForPassByValue(keySerialization, key);
          value = makeCopyForPassByValue(valueSerialization, value);
        }

        // 获取下一个Mapper的序列化信息和实例
        Serialization nextKeySerialization =
          mappersKeySerialization.get(nextMapperIndex);
        Serialization nextValueSerialization =
          mappersValueSerialization.get(nextMapperIndex);
        Mapper nextMapper = mappers.get(nextMapperIndex);

        // 调用下一个Mapper处理当前输出
        nextMapper.map(key, value,
                       new ChainOutputCollector(nextMapperIndex,
                                                nextKeySerialization,
                                                nextValueSerialization,
                                                output, reporter),
                       reporter);
      } else {
        // 已经是链尾，输出到原始收集器
        output.collect(key, value);
      }
    }

    /**
     * 通过序列化深拷贝对象，实现按值传递
     *
     * @param serialization 序列化实例
     * @param obj 原始对象
     * @return 深拷贝后的新对象
     * @throws IOException 序列化/反序列化IO异常
     */
    private <E> E makeCopyForPassByValue(Serialization<E> serialization,
                                          E obj) throws IOException {
      Serializer<E> ser =
        serialization.getSerializer(GenericsUtil.getClass(obj));
      Deserializer<E> deser =
        serialization.getDeserializer(GenericsUtil.getClass(obj));

      // 获取线程缓存的输出缓冲区
      DataOutputBuffer dof = threadLocalDataOutputBuffer.get();

      // 序列化原始对象到缓冲区
      dof.reset();
      ser.open(dof);
      ser.serialize(obj);
      ser.close();
      // 创建新对象实例
      obj = ReflectionUtils.newInstance(GenericsUtil.getClass(obj),
                                        getChainJobConf());
      // 反序列化到新对象完成拷贝
      ByteArrayInputStream bais =
        new ByteArrayInputStream(dof.getData(), 0, dof.getLength());
      deser.open(bais);
      deser.deserialize(obj);
      deser.close();
      return obj;
    }

  }

}