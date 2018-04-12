if [ "$1" = "mr" ]
then
    rm -rf ./output && \
    mvn package && \
    ../../hadoop/bin/yarn jar ./target/cs132g4-0.7.jar ./data/wiki_00.csv ./output
elif [ "$1" = "spark" ]
then
    mvn package && \
    ../../spark/bin/spark-submit --class "search.WordSearcher" --master local[4] ./target/cs132g4-0.7.jar
elif [ "$1" = "web" ]
then
    echo "Not implemented"
else
    echo "App not recognized"
fi