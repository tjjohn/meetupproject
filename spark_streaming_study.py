#enviroment is Pyspark
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from json import dumps
appName = "My Spark Application"
sc = SparkContext("local[*]", appName)   # sc is the spark object
ssc = StreamingContext(sc, 1)


# Define the input sources by creating input DStreams.
inputdstream=ssc.textFileStream(r"C:\\Users\\tj john\\Desktop\\Technoloy\\testspark.txt")

print(inputdstream)  # DStream


# Define the streaming computations by applying transformation and output operations to DStreams.
words = inputdstream.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)
wordCounts.pprint() # showing TransformedDStream



# Start receiving data and processing it using streamingContext.start().
ssc.start()
# Wait for the processing to be stopped (manually or due to any error) using streamingContext.awaitTermination().
ssc.awaitTermination()

#run