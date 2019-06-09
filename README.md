# Spark Examples
A sandbox project for learning the basics of Spark. I'll add multiple examples as I continue to learn.

## Dependencies
* Java 1.8
* Spark 2.4.3

Spark 2.4 is not compatible with Java versions greater than 1.8. Problems with building or testing in an IDE can occur
if the ID is using a newer version of java to run gradle or the test runner.

## WordCounts
Simple word count example

### Building
Execute `./gradlew build`

### Running
Execute `spark-submit --class example.WordCount ./build/libs/spark-examples-1.0.jar <input text file> <output directory>`

### Testing
Execute `./gradlew test`

##DatasetWordCounts
Word count example using the SparkSQL Datasets

### building
Execute `./gradlew build`

### Running
Execute `spark-submit --class example.DatasetWordCount ./build/libs/spark-examples-1.0.jar <input text file> <output directory`

### Testing
Execute `./gradlew test`