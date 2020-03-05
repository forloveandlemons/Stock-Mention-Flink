/*
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

package bobostockanalysis;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import java.util.Properties;
import java.lang.System;
import bobostockanalysis.datatypes.StockMentionCount;
import bobostockanalysis.sources.StockMentionCountSource;
import bobostockanalysis.connector.StockMentionCountSchema;
import bobostockanalysis.calculation.WindowCorrelation;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		String inputTopic = "bobo_stock_data";
		String brokerList = "localhost:9092";
		String outputTopic = "bobo_stock_data_output";
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		produceData(env, inputTopic, brokerList);
		consumeData(env, inputTopic, outputTopic, brokerList);
		env.execute("Flink Streaming Java API Skeleton");

	}

	private static void produceData(StreamExecutionEnvironment env, String topic, String brokerList) throws Exception {
		// set up the streaming execution environment
		//final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// execute program
		DataStream<String> AAPLStream = env.addSource(new StockMentionCountSource("AAPL"));
		DataStream<String> FBStream = env.addSource(new StockMentionCountSource("FB"));
		DataStream<String> AMZNStream = env.addSource(new StockMentionCountSource("AMZN"));
		DataStream<String> GOOGStream = env.addSource(new StockMentionCountSource("GOOG"));

		FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(brokerList, topic, new SimpleStringSchema());

		myProducer.setWriteTimestampToKafka(true);

		AAPLStream.addSink(myProducer);
		FBStream.addSink(myProducer);
		AMZNStream.addSink(myProducer);
		GOOGStream.addSink(myProducer);

	}

	private static void consumeData(StreamExecutionEnvironment env, String inputTopic, String outputTopic, String brokerList) throws Exception {
		// set up the streaming execution environment
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", brokerList);
		props.setProperty("group.id", "test2");

		DataStream<StockMentionCount> stream = env.addSource(new FlinkKafkaConsumer<>(inputTopic, new StockMentionCountSchema(), props))
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<StockMentionCount>() {
					@Override
					public long extractAscendingTimestamp(StockMentionCount smc) {
						return System.currentTimeMillis();
					}
				});

		DataStream<Tuple5<String, String, String, Long, Double>> rollingCorrelation = stream
				.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(5000)))
				.process(new WindowCorrelation());

		CassandraSink.addSink(rollingCorrelation)
				.setQuery("INSERT INTO bobo_stock_analysis.corrrelation_result(ds, stock1, stock2, eventtime, correlation) values (?, ?, ?, ?, ?);")
				.setHost("127.0.0.1")
				.build();

//		DataStream<String> rollingCorrelation = stream
//				.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(5000)))
//				.process(new WindowCorrelation())
//				.map(t -> t.f0 + "," + t.f1 + "," + t.f2.toString() + "," + t.f3.toString() + "," + t.f4);

//		FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(brokerList, outputTopic, new SimpleStringSchema());
//		myProducer.setWriteTimestampToKafka(true);
//		rollingCorrelation.addSink(myProducer);

	}


//	public static class SimpleStringGenerator implements SourceFunction<String> {
//		private static final long serialVersionUID = 2174904787118597072L;
//		boolean running = true;
//		long i = 0;
//		@Override
//		public void run(SourceContext<String> ctx) throws Exception {
//			while(running) {
//				ctx.collect("element-"+ (i++));
//				Thread.sleep(10);
//			}
//		}
//
//		@Override
//		public void cancel() {
//			running = false;
//		}
//	}
}
