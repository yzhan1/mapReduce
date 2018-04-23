package configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Shu Lin Chan, Jonathan Maeda, James Wang, Yaoming Zhan
 * Final Project
 */
@Configuration
public class ApplicationConfiguration {
    @Bean
    public SparkConf sparkConf() {
        return new SparkConf().setAppName("cs132g4searcher").setMaster(System.getenv("MASTER")).set("spark.executor.instances", "8");
    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(sparkConf());
    }
}
