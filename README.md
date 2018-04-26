# Wikipedia Search Project

### Group Info
+ Name: Network Beasts (cs132g4)
+ Members: Shu Lin Chan, Jonathan Maeda, James Wang, Yaoming Zhan

### Project Info
This is a web search interface that allows users to search through Wikipedia data. The application uses Hadoop MapReduce to process the wiki data and generate inverted index for each word, then use a Spark job to search through the index in the HDFS.

### Getting Started Locally
+ Run Hadoop MapReduce job
  + Comment out the line 174-185 in `pom.xml`
  + Move one of the wiki csv's to `./data/` folder
  + Run `mvn clean install`, then `yarn jar ./target/cs132g4-0.7.jar ./data/wiki_00.csv ./output10`
+ Run Spark and Spring Web interface
  + Uncomment line 174-185 in `pom.xml`. Comment out line 141-172 instead
  + Comment out line 79-84 in `search.SearchService.java` and uncomment line 86-88
  + Run `mvn clean install`, then `java -jar ./target/cs132g4-0.7.jar`