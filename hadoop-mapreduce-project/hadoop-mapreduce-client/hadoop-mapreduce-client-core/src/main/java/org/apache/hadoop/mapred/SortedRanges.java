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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 维护按起始索引排序的非重叠索引区间集合，提供跳过这些区间的迭代器，用于MapReduce任务处理中跳过已完成/失败的数据分片
 */
class SortedRanges implements Writable{
  
  private static final Logger LOG =
      LoggerFactory.getLogger(SortedRanges.class);
  
  // 使用TreeSet维护排序的区间集合，保证区间按起始索引有序
  private TreeSet<Range> ranges = new TreeSet<Range>();
  // 所有区间包含的总索引数
  private long indicesCount;
  
  /**
   * 获取可跳过本集合中存储区间的迭代器，用于遍历非跳过范围的索引
   * @return 跳过指定区间的迭代器实例
   */
  synchronized SkipRangeIterator skipRangeIterator(){
    return new SkipRangeIterator(ranges.iterator());
  }
  
  /**
   * 获取所有区间包含的总索引数
   * @return 总索引数
   */
  synchronized long getIndicesCount() {
    return indicesCount;
  }
  
  /**
   * 获取排序后的区间集合
   * @return 排序后的区间集合
   */
  synchronized SortedSet<Range> getRanges() {
  	return ranges;
 	}
  
  /**
   * 添加一个区间，自动合并重叠区间，保证集合中所有区间始终非重叠
   * 如果区间长度为0则不执行任何操作
   * @param range 要添加的区间
   */
  synchronized void add(Range range){
    if(range.isEmpty()) {
      return;
    }
    
    long startIndex = range.getStartIndex();
    long endIndex = range.getEndIndex();
    // 获取所有起始索引小于当前区间的子集
    SortedSet<Range> headSet = ranges.headSet(range);
    if(headSet.size()>0) {
      // 获取最后一个（起始索引最大）的前置区间
      Range previousRange = headSet.last();
      LOG.debug("previousRange "+previousRange);
      if(startIndex<previousRange.getEndIndex()) {
        // 前置区间与当前区间重叠，移除前置区间并更新总计数
        if(ranges.remove(previousRange)) {
          indicesCount-=previousRange.getLength();
        }
        // 扩展当前区间覆盖重叠部分
        startIndex = previousRange.getStartIndex();
        endIndex = endIndex>=previousRange.getEndIndex() ?
                          endIndex : previousRange.getEndIndex();
      }
    }
    
    // 遍历所有起始索引大于等于当前区间的后续区间
    Iterator<Range> tailSetIt = ranges.tailSet(range).iterator();
    while(tailSetIt.hasNext()) {
      Range nextRange = tailSetIt.next();
      LOG.debug("nextRange "+nextRange +"   startIndex:"+startIndex+
          "  endIndex:"+endIndex);
      if(endIndex>=nextRange.getStartIndex()) {
        // 后续区间与当前区间重叠，移除后续区间并更新总计数
        tailSetIt.remove();
        indicesCount-=nextRange.getLength();
        if(endIndex<nextRange.getEndIndex()) {
          // 扩展当前区间覆盖重叠部分
          endIndex = nextRange.getEndIndex();
          break;
        }
      } else {
        break;
      }
    }
    // 添加合并后的新区间
    add(startIndex,endIndex);
  }
  
  /**
   * 移除指定区间，分割原有重叠区间，保证集合中区间始终非重叠
   * 如果区间长度为0则不执行任何操作
   * @param range 要移除的区间
   */
  synchronized void remove(Range range) {
    if(range.isEmpty()) {
      return;
    }
    long startIndex = range.getStartIndex();
    long endIndex = range.getEndIndex();
    // 获取所有起始索引小于当前区间的子集
    SortedSet<Range> headSet = ranges.headSet(range);
    if(headSet.size()>0) {
      Range previousRange = headSet.last();
      LOG.debug("previousRange "+previousRange);
      if(startIndex<previousRange.getEndIndex()) {
        // 前置区间与要移除的区间重叠
        if(ranges.remove(previousRange)) {
          indicesCount-=previousRange.getLength();
          LOG.debug("removed previousRange "+previousRange);
        }
        // 添加移除区间前的剩余部分
        add(previousRange.getStartIndex(), startIndex);
        // 添加移除区间后的剩余部分
        if(endIndex<=previousRange.getEndIndex()) {
          add(endIndex, previousRange.getEndIndex());
        }
      }
    }
    
    // 遍历所有起始索引大于等于当前区间的后续区间
    Iterator<Range> tailSetIt = ranges.tailSet(range).iterator();
    while(tailSetIt.hasNext()) {
      Range nextRange = tailSetIt.next();
      LOG.debug("nextRange "+nextRange +"   startIndex:"+startIndex+
          "  endIndex:"+endIndex);
      if(endIndex>nextRange.getStartIndex()) {
        // 后续区间与要移除的区间重叠
        tailSetIt.remove();
        indicesCount-=nextRange.getLength();
        // 添加移除区间后的剩余部分
        if(endIndex<nextRange.getEndIndex()) {
          add(endIndex, nextRange.getEndIndex());
          break;
        }
      } else {
        break;
      }
    }
  }
  
  /**
   * 内部添加区间方法，维护总索引计数
   */
  private void add(long start, long end) {
    if(end>start) {
      Range recRange = new Range(start, end-start);
      ranges.add(recRange);
      indicesCount+=recRange.getLength();
      LOG.debug("added "+recRange);
    }
  }
  
  /**
   * 反序列化读取区间集合数据
   */
  public synchronized void readFields(DataInput in) throws IOException {
    indicesCount = in.readLong();
    ranges = new TreeSet<Range>();
    int size = in.readInt();
    for(int i=0;i<size;i++) {
      Range range = new Range();
      range.readFields(in);
      ranges.add(range);
    }
  }

  /**
   * 序列化区间集合数据到输出流
   */
  public synchronized void write(DataOutput out) throws IOException {
    out.writeLong(indicesCount);
    out.writeInt(ranges.size());
    Iterator<Range> it = ranges.iterator();
    while(it.hasNext()) {
      Range range = it.next();
      range.write(out);
    }
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Iterator<Range> it = ranges.iterator();
    while(it.hasNext()) {
      Range range = it.next();
      sb.append(range.toString()+"\n");
    }
    return sb.toString();
  }
  
  /**
   * 表示一个长整型索引区间，存储起始索引和长度，支持排序和序列化
   * 区间遵循左闭右开规则：包含起始索引，不包含结束索引
   */
  static class Range implements Comparable<Range>, Writable{
    private long startIndex;
    private long length;
        
    Range(long startIndex, long length) {
      if(length<0) {
        throw new RuntimeException("length can't be negative");
      }
      this.startIndex = startIndex;
      this.length = length;
    }
    
    Range() {
      this(0,0);
    }
    
    /**
     * 获取区间起始索引（包含）
     * @return 起始索引
     */
    long getStartIndex() {
      return startIndex;
    }
    
    /**
     * 获取区间结束索引（不包含）
     * @return 结束索引
     */
    long getEndIndex() {
      return startIndex + length;
    }
    
   /**
    * 获取区间长度
    * @return 区间长度
    */
    long getLength() {
      return length;
    }
    
    /**
     * 判断区间是否为空（长度为0）
     * @return true表示区间为空
     */
    boolean isEmpty() {
      return length==0;
    }
    
    public boolean equals(Object o) {
      if (o instanceof Range) {
        Range range = (Range)o;
        return startIndex==range.startIndex &&
        length==range.length;
      }
      return false;
    }
    
    public int hashCode() {
      return Long.valueOf(startIndex).hashCode() +
          Long.valueOf(length).hashCode();
    }
    
    /**
     * 按起始索引排序，起始索引相同则按长度排序
     */
    public int compareTo(Range o) {
      // Ensure sgn(x.compareTo(y) == -sgn(y.compareTo(x))
      return this.startIndex < o.startIndex ? -1 :
          (this.startIndex > o.startIndex ? 1 :
          (this.length < o.length ? -1 :
          (this.length > o.length ? 1 : 0)));
    }

    public void readFields(DataInput in) throws IOException {
      startIndex = in.readLong();
      length = in.readLong();
    }

    public void write(DataOutput out) throws IOException {
      out.writeLong(startIndex);
      out.writeLong(length);
    }
    
    public String toString() {
      return startIndex +":" + length;
    }    
  }
  
  /**
   * 迭代器实现，遍历索引时自动跳过指定区间集合，用于跳过需要跳过的已处理/失败数据索引
   */
  static class SkipRangeIterator implements Iterator<Long> {
    // 待跳过区间的迭代器
    Iterator<Range> rangeIterator;
    // 当前处理的待跳过区间
    Range range = new Range();
    // 下一个要返回的索引
    long next = -1;
    
    /**
     * 构造跳过区间的迭代器
     * @param rangeIterator 待跳过区间的迭代器
     */
    SkipRangeIterator(Iterator<Range> rangeIterator) {
      this.rangeIterator = rangeIterator;
      doNext();
    }
    
    /**
     * 判断是否还有下一个可用索引
     * @return true存在下一个索引，false已到达最大索引值
     */
    public synchronized boolean hasNext() {
      return next<Long.MAX_VALUE;
    }
    
    /**
     * 获取下一个不落在跳过区间中的索引
     * @return 下一个可用索引
     */
    public synchronized Long next() {
      long ci = next;
      doNext();
      return ci;
    }
    
    /**
     * 计算下一个可用索引，自动跳过当前落在跳过区间内的索引
     */
    private void doNext() {
      next++;
      LOG.debug("currentIndex "+next +"   "+range);
      // 如果当前索引在当前待跳过区间内，直接跳到区间结束位置
      skipIfInRange();
      // 当前待跳过区间处理完后，移动到下一个待跳过区间继续处理
      while(next>=range.getEndIndex() && rangeIterator.hasNext()) {
        range = rangeIterator.next();
        skipIfInRange();
      }
    }
    
    /**
     * 如果当前索引落在当前待跳过区间内，直接跳到区间结束位置
     */
    private void skipIfInRange() {
      if(next>=range.getStartIndex() && 
          next<range.getEndIndex()) {
        // 需要跳过该区间内的所有索引
        LOG.warn("Skipping index " + next +"-" + range.getEndIndex());
        next = range.getEndIndex();
        
      }
    }
    
    /**
     * 判断是否已经跳过了所有待跳过区间
     * @return true所有区间都已跳过，false还有未处理区间
     */
    synchronized boolean skippedAllRanges() {
      return !rangeIterator.hasNext() && next>range.getEndIndex();
    }
    
    /**
     * 不支持移除操作
     */
    public void remove() {
      throw new UnsupportedOperationException("remove not supported.");
    }
    
  }

}