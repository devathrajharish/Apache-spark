#!/usr/bin/env python
# coding: utf-8

# In[ ]:



import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'


# In[ ]:


#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json


# In[ ]:


sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
sc.setLogLevel("WARN")


# In[ ]:


ssc = StreamingContext(sc, 60)


# In[ ]:


kafkaStream = KafkaUtils.createStream(ssc, 'cdh57-01-node-01.moffatt.me:2181', 'spark-streaming', {'twitter':1})


# In[ ]:


parsed = kafkaStream.map(lambda v: json.loads(v[1]))


# In[ ]:


parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()


# In[ ]:


authors_dstream = parsed.map(lambda tweet: tweet['user']['screen_name'])


# In[ ]:


author_counts = authors_dstream.countByValue()
author_counts.pprint()


# In[ ]:


author_counts_sorted_dstream = author_counts.transform(  (lambda foo:foo   .sortBy(lambda x:( -x[1]))))
#   .sortBy(lambda x:(x[0].lower(), -x[1]))\
#  ))


# In[ ]:


top_five_authors = author_counts_sorted_dstream.transform  (lambda rdd:sc.parallelize(rdd.take(5)))
top_five_authors.pprint()


# In[ ]:


filtered_authors = author_counts.filter(lambda x:                                                x[1]>1                                                 or                                                 x[0].lower().startswith('rm'))


# In[ ]:


filtered_authors.transform  (lambda rdd:rdd  .sortBy(lambda x:-x[1]))  .pprint()


# In[ ]:


parsed.    flatMap(lambda tweet:tweet['text'].split(" "))    .countByValue()    .transform      (lambda rdd:rdd.sortBy(lambda x:-x[1]))    .pprint()


# In[ ]:


ssc.start()
ssc.awaitTermination(timeout=180)

