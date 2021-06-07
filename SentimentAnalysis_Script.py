from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
import sparknlp
from sparknlp.pretrained import PretrainedPipeline
import json
from sparknlp.annotator import *
from sparknlp.base import *
import os.path
import shutil
import sys
import time
import csv
import settings


# Get the index from the command line arguments, which comes from ClientGenerator.
# The index are mapped to keywords in the settings.py file\
# For example, the 0th keyword in the keyword list refers to index = 0
try:
    index = int(sys.argv[1])
except IndexError:
    print('Missing inputs!')
    sys.exit()
    
# Preprocess the text in the stream
def preprocessing(stream_input):
    rows = stream_input.select(F.explode(F.split(stream_input.value, "t_end")).alias("tweet_text"))
    rows = rows.na.replace('', None)
    rows = rows.na.drop()
    rows = rows.withColumn('tweet_text', F.regexp_replace('tweet_text', r'http\S+', ''))
    rows = rows.withColumn('tweet_text', F.regexp_replace('tweet_text', '@\w+', ''))
    rows = rows.withColumn('tweet_text', F.regexp_replace('tweet_text', '#', ''))
    rows = rows.withColumn('tweet_text', F.regexp_replace('tweet_text', 'RT', ''))
    rows = rows.withColumn('tweet_text', F.regexp_replace('tweet_text', ':', ''))
    return rows


# Get the socket settings from the settings.py file. The port will be incremented according to the index
host = settings.SOCKET_HOST
port = settings.SOCKET_PORT + index

# Get the search keyword according to the index
keyword = settings.KEYWORDS[index]

# Initialization section: Start the spark (sparknlp) session, set up the incoming data stream,
# clear out the checkpoints
spark = sparknlp.start()
print('Spark session started')

# read the tweet data from socket
raw_streaming_text = spark.readStream.format("socket") \
    .option("host", host) \
    .option("port", port) \
    .option("failOnDataLoss", "false") \
    .load()

print('Spark readStream ready.')

# Preprocess the data
processed_text_df = preprocessing(raw_streaming_text)

# Load a pre-generated pipeline, or create a new one if one is not already saved.
try:
    print('Loading saved pipeline...')
    pipeline_model = PretrainedPipeline.from_disk('./sentiment_analysis_pipeline')
    print('Saved pipeline loaded successfully.')
except:
    print('No saved pipeline found. Creating new pipeline...')

    documentAssembler = DocumentAssembler() \
        .setInputCol("text") \
        .setOutputCol("document")

    use = UniversalSentenceEncoder.pretrained(name="tfhub_use", lang="en") \
        .setInputCols(["document"]) \
        .setOutputCol("sentence_embeddings")

    sentimentdl = SentimentDLModel.pretrained(name='sentimentdl_use_twitter', lang="en") \
        .setInputCols(["sentence_embeddings"]) \
        .setOutputCol("sentiment")

    nlpPipeline = Pipeline(
        stages=[
            documentAssembler,
            use,
            sentimentdl
        ])
    print('New pipeline created. Saving pipeline model...')

    pipeline_model = nlpPipeline.fit(processed_text_df)
    pipeline_model.save('./sentiment_analysis_pipeline')
    print('New pipeline model saved.')

# Extract the text value from the tweet (document.result) and the sentiment value (sentiment.result),
# which is either positive, negative, or neutral
df_result = pipeline_model.transform(processed_text_df.withColumnRenamed("tweet_text", "text"))

df_result = df_result.select(F.explode(F.arrays_zip('document.result', 'sentiment.result')).alias("cols")) \
                     .select(F.expr("cols['0']").alias("tweet"),
                             F.expr("cols['1']").alias("sentiment"))

# csv output file for sentiment analysis for specific keyword
output_filename = str(index) + '_sentiments.csv' 
if not os.path.isfile(output_filename):
    with open(output_filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['keyword', 'timestamp', 'positive', 'neutral', 'negative'])

def aggregate(df, epoch_id): #takes in batch of processed data and adds up sentiments
    current_time = time.time()
    
    pandas_df = df.toPandas()
    sentiments = list(pandas_df['sentiment'])
    if len(sentiments) == 0:
        pass
    else:
        options = ['positive', 'neutral', 'negative']
        sums = [(option, sum([x == option for x in sentiments])) for option in options]
        positive_value = sums[0][1]
        neutral_value = sums[1][1]
        negative_value = sums[2][1]
        with open(output_filename, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([keyword, str(current_time), str(positive_value), str(neutral_value), str(negative_value)])

# Write the results to a parquet file
#query = df_result.writeStream.queryName("all_tweets")\
#   .outputMode("append").format("parquet")\
#   .option("path", "./parc_"+str(index))\
#   .option("checkpointLocation", "./check_"+str(index))\
#   .trigger(processingTime='60 seconds').start()

query = df_result.writeStream.foreachBatch(aggregate).start() #write stream intializes stream process
query.awaitTermination()