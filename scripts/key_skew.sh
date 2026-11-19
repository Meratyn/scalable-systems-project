java -jar java-job/target/ema-crossover-1.0.jar --input data/raw --output output/stats --stage symbolstats
java -cp java-job/target/ema-crossover-1.0.jar ema.SkewReport output/stats/symbolstats