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
package org.apache.hadoop.mapreduce.lib.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/**
 * MapReduce数据库读写功能接口，定义对象与关系型数据库交互的序列化协议。
 * 该接口类似{@link Writable}，区别在于它专门用于JDBC交互：
 * 将当前对象的字段写入JDBC PreparedStatement，或从JDBC ResultSet读取字段填充对象。
 * 实现类需要自行处理对象字段和数据库表列之间的映射关系，用于MapReduce从数据库读取输入、
 * 将计算结果写入数据库的场景。
 * 
 * <p>使用示例:</p>
 * 如果数据库中有如下表:
 * <pre>
 * CREATE TABLE MyTable (
 *   counter        INTEGER NOT NULL,
 *   timestamp      BIGINT  NOT NULL,
 * );
 * </pre>
 * 可以实现该接口来读写表中记录:
 * <p><pre>
 * public class MyWritable implements Writable, DBWritable {
 *   // 业务数据
 *   private int counter;
 *   private long timestamp;
 *       
 *   //Writable#write()实现
 *   public void write(DataOutput out) throws IOException {
 *     out.writeInt(counter);
 *     out.writeLong(timestamp);
 *   }
 *       
 *   //Writable#readFields()实现
 *   public void readFields(DataInput in) throws IOException {
 *     counter = in.readInt();
 *     timestamp = in.readLong();
 *   }
 *       
 *   // 将对象字段写入JDBC预编译语句
 *   public void write(PreparedStatement statement) throws SQLException {
 *     statement.setInt(1, counter);
 *     statement.setLong(2, timestamp);
 *   }
 *       
 *   // 从JDBC结果集读取数据填充对象
 *   public void readFields(ResultSet resultSet) throws SQLException {
 *     counter = resultSet.getInt(1);
 *     timestamp = resultSet.getLong(2);
 *   } 
 * }
 * </pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface DBWritable {

  /**
   * 将当前对象的所有字段写入到JDBC PreparedStatement中，用于SQL执行
   * @param statement 预编译SQL语句对象，对象字段将按顺序绑定到该语句的参数
   * @throws SQLException 如果数据库操作发生异常则抛出
   */
	public void write(PreparedStatement statement) throws SQLException;
	
	/**
	 * 从JDBC ResultSet当前行读取数据，填充当前对象的所有字段
	 * @param resultSet 查询结果集，从当前行读取字段值填充对象
	 * @throws SQLException 如果数据库操作发生异常则抛出
	 */
	public void readFields(ResultSet resultSet) throws SQLException ; 
}