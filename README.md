# Spark Examples
A sandbox project for learning the basics of Spark. I'll add multiple examples as I continue to learn.

## Dependencies
* Java 1.8
* Spark 2.4.3

## Building
Execute `./gradlew build`

## Running

### Word Count
Execute `spark-submit --class example.WordCount ./build/libs/spark-examples-1.0.jar <input text file> <output directory>`