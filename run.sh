if [ "$1" = "mr" ]
then
    rm -rf ./output && \
    mvn clean package && \
    ../../hadoop/bin/yarn jar ./target/cs132g4-0.7.jar ./data/wiki_00.csv ./output
elif [ "$1" = "spark" ]
then
    mvn clean package && \
    ../../spark/bin/spark-submit --class "search.SearchService" --master local[4] ./target/cs132g4-0.7.jar "abrar & ababa | aaa"
elif [ "$1" = "web" ]
then
    mvn clean package && java -cp ./target/cs132g4-0.7.jar drivers.Application
else
    echo "App not recognized"
fi