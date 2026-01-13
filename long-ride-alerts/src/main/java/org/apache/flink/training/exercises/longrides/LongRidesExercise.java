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

package org.apache.flink.training.exercises.longrides;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Duration;

/**
 * The "Long Ride Alerts" exercise.
 *
 * <p>The goal for this exercise is to emit the rideIds for taxi rides with a duration of more than
 * two hours. You should assume that TaxiRide events can be lost, but there are no duplicates.
 *
 * <p>You should eventually clear any state you create.
 */
public class LongRidesExercise {
    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<Long> sink;

    /** Creates a job using the source and sink provided. */
    public LongRidesExercise(SourceFunction<TaxiRide> source, SinkFunction<Long> sink) {
        this.source = source;
        this.sink = sink;
    }

    /**
     * Creates and executes the long rides pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(source);

        // the WatermarkStrategy specifies how to extract timestamps and generate watermarks
        WatermarkStrategy<TaxiRide> watermarkStrategy =
                WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner(
                                (ride, streamRecordTimestamp) -> ride.getEventTimeMillis());

        // create the pipeline
        rides.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ride -> ride.rideId)
                .process(new AlertFunction())
                .addSink(sink);

        // execute the pipeline and return the result
        return env.execute("Long Taxi Rides");
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        LongRidesExercise job =
                new LongRidesExercise(new TaxiRideGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    @VisibleForTesting
    public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {

        private static final long TWO_HOURS_MS = Duration.ofHours(2).toMillis();
        private static final long MAX_OUT_OF_ORDER_MS = Duration.ofSeconds(60).toMillis();

        private transient ValueState<TaxiRide> startState;
        private transient ValueState<TaxiRide> endState;

        private transient ValueState<Long> longRideTimerTs;
        private transient ValueState<Long> endCleanupTimerTs;

        @Override
        public void open(Configuration config) throws Exception {
            startState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("start", TaxiRide.class)
            );
            endState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("end", TaxiRide.class)
            );

            longRideTimerTs = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("longRideTimer", Long.class)
            );
            endCleanupTimerTs = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("endCleanupTimer", Long.class)
            );
        }

        @Override
        public void processElement(TaxiRide ride, Context ctx, Collector<Long> out)
                throws Exception {
            if (ride.isStart) {
                // START arrived
                TaxiRide end = endState.value();
                if (end != null) {
                    long duration = end.getEventTimeMillis() - ride.getEventTimeMillis();
                    if (duration > TWO_HOURS_MS) {
                        out.collect(ride.rideId);
                    }
                    // cleanup + cancel long timer
                    clearStartAndLongTimer(ctx);
                } else {
                    startState.update(ride);

                    long timerTs = ride.getEventTimeMillis() + TWO_HOURS_MS;
                    ctx.timerService().registerEventTimeTimer(timerTs);
                    longRideTimerTs.update(timerTs);
                }
            } else {
                // END arrived
                TaxiRide start = startState.value();
                if (start != null) {
                    long duration = ride.getEventTimeMillis() - start.getEventTimeMillis();
                    if (duration > TWO_HOURS_MS) {
                        out.collect(ride.rideId);
                    }
                    // cleanup + cancel long timer
                    clearStartAndLongTimer(ctx);
                } else {
                    // START missing
                    endState.update(ride);

                    long cleanupTs = ride.getEventTimeMillis() + MAX_OUT_OF_ORDER_MS + 1;
                    ctx.timerService().registerEventTimeTimer(cleanupTs);
                    endCleanupTimerTs.update(cleanupTs);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Long> out)
                throws Exception {
            Long longTs = longRideTimerTs.value();
            if (longTs != null && timestamp == longTs) {
                // 2 hours passed since START; if END still not seen, alert.
                TaxiRide start = startState.value();
                if (start != null) {
                    out.collect(start.rideId);
                }
                clearStartAndLongTimer(ctx);
                return;
            }

            Long cleanupTs = endCleanupTimerTs.value();
            if (cleanupTs != null && timestamp == cleanupTs) {
                // END-only record expired; drop it
                clearEndAndCleanupTimer(ctx);
            }
        }

        private void clearStartAndLongTimer(KeyedProcessFunction<Long, TaxiRide, Long>.Context ctx) throws Exception {
            Long ts = longRideTimerTs.value();
            if (ts != null) {
                ctx.timerService().deleteEventTimeTimer(ts);
            }
            longRideTimerTs.clear();
            startState.clear();
        }

        private void clearEndAndCleanupTimer(KeyedProcessFunction<Long, TaxiRide, Long>.Context ctx) throws Exception {
            Long ts = endCleanupTimerTs.value();
            if (ts != null) {
                ctx.timerService().deleteEventTimeTimer(ts);
            }
            endCleanupTimerTs.clear();
            endState.clear();
        }
    }
}
