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

import bobostockanalysis.datatypes.StockMentionCount;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.api.TimeCharacteristic;

import java.io.File;
import java.util.Properties;

import bobostockanalysis.sources.StockMentionCountSource;
import bobostockanalysis.datatypes.StockMentionCount;
import bobostockanalysis.connector.StockMentionCountSchema;


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
		produceData(inputTopic, brokerList);
		consumeData(inputTopic, brokerList);

	}

	public static void produceData(String topic, String brokerList) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

		env.execute("Flink Streaming Java API Skeleton");
	}

	public static void consumeData(String topic, String brokerList) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", brokerList);
		props.setProperty("group.id", "test");

		DataStream<String> stream = env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props));

		env.execute("Flink Streaming Java API Skeleton");
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
