package com.github.mbode.flink_prometheus_example;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ExampleJob {
  private final ParameterTool parameters;

  public static void main(String[] args) throws Exception {
    new ExampleJob(ParameterTool.fromArgs(args)).run();
  }

  private ExampleJob(ParameterTool parameters) {
    this.parameters = parameters;
  }

  private void run() throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(500);
    env.disableOperatorChaining();

    env.addSource(new RandomSourceFunction(parameters.getInt("elements", Integer.MAX_VALUE)))
        .name(RandomSourceFunction.class.getSimpleName())
        .map(value -> value * 2)
        .name("Multiplicador")
        .print();


    env.execute(ExampleJob.class.getSimpleName());
  }
}
