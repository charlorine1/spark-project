package com.usst.sparkproject.spark.product;

import java.util.Arrays;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {
	// 指定输入数据的字段与类型
	private StructType inputSchema = DataTypes
			.createStructType(Arrays.asList(DataTypes.createStructField("cityInfo", DataTypes.StringType, true)));

	// 指定缓冲数据的字段与类型
	private StructType bufferSchema = DataTypes
			.createStructType(Arrays.asList(DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)));
	// 指定返回类型
	private DataType dataType = DataTypes.StringType;

	private static final long serialVersionUID = 1L;

	/**
	 * 确保一致性 一般用true,用以标记针对给定的一组输入，UDAF是否总是生成相同的结果。 指定是否是确定性的
	 */
	@Override
	public boolean deterministic() {
		return true;
	}

	/**
	 * 初始化 可以认为是，你自己在内部指定一个初始的值""
	 */
	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, "");
	}

	// 指定输入数据的字段与类型
	@Override
	public StructType inputSchema() {
		return inputSchema;
	}

	// 指定缓冲数据的字段与类型
	@Override
	public StructType bufferSchema() {
		return bufferSchema;
	}

	// 指定返回类型
	@Override
	public DataType dataType() {
		return dataType;
	}

	/**
	 * 更新 可以认为一个一个地将组内的字段值传递进来 实现拼接的逻辑
	 * buffer.getInt(0)获取的是上一次聚合后的值
	  * 相当于map端的combiner，combiner就是对每一个map task的处理结果进行一次小聚合
          *大聚和发生在reduce端.
	  * 这里即是:在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
	 * buffer:上一次聚合放到buffer.update(0, ""); 里面的值
	 * input 是传入的一条数据
	 */
	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		// 缓冲中的已经拼接过的城市信息串
		String bufferCityInfo = buffer.getString(0);
		// 刚刚传递进来的某个城市信息
		String cityInfo = input.getString(0);
		
		// 在这里要实现去重的逻辑
		// 判断：之前没有拼接过某个城市信息，那么这里才可以接下去拼接新的城市信息
		if(!bufferCityInfo.contains(cityInfo)) {
			if("".equals(bufferCityInfo)) {
				bufferCityInfo += cityInfo;
			} else {
				// 比如1:北京
				// 1:北京,2:上海
				bufferCityInfo += "," + cityInfo;
			}
			
			//更新进去
			buffer.update(0, bufferCityInfo);  
		}

	}
	
    /**
         * 合并 update操作，可能是针对一个分组内的部分数据，在某个节点上发生的 但是可能一个分组内的数据，会分布在多个节点上处理
         * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来
     * buffer1.getInt(0) : 大聚合的时候 上一次聚合后的值，最终要返回的值，前面每个节点聚合后的值       
     * buffer2.getInt(0) : 这次计算传入进来的update的结果，每个节点小聚合的值：云南，普洱
          * 这里即是：最后在分布式节点完成后需要进行全局级别的Merge操作
          * 也可以是一个节点里面的多个executor合并
     */
	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		String bufferCityInfo1 = buffer1.getString(0);
		String bufferCityInfo2 = buffer2.getString(0);
		
		for(String cityInfo : bufferCityInfo2.split(",")) {
			if(!bufferCityInfo1.contains(cityInfo)) {
				if("".equals(bufferCityInfo1)) {
					bufferCityInfo1 += cityInfo;
				} else {
					bufferCityInfo1 += "," + cityInfo;
				}
 			}
		}
		
		buffer1.update(0, bufferCityInfo1);  

	}

	
	//最后返回一个String值就好了
	@Override
	public Object evaluate(Row row) {
		 return row.getString(0);
	}



}
