## Left-discords Streaming Time-Series Anomaly Detection in Apache Flink

A left-discord is a subsequence that is significantly dissimilar from all the sub sequences that precede it. In this sample, we demonstrate how to use the concept of left-discords to identify time-series anomalies in streams using Kinesis Data Analytics for Apache Flink.

Let’s consider an unbounded stream and all its subsequences of length n. The m most recent subsequences will be stored and used for inference. When a new data-point arrives, a new subsequence that includes the new event is formed. We compare this latest subsequence (query) to the m subsequences retained from the model, with the exclusion of the latest n subsequences as they overlap with the query and would thus characterise a self-match. Once we have computed these distances, we classify the query as an anomaly if its distance from its closest non-self-matching subsequence is above a certain moving threshold.

For this experiment, you will use an Amazon Kinesis Data Stream to ingest the input data, a Kinesis Data Analytics application to run the Flink anomaly detection program, and another Kinesis Data Stream to ingest the output produced by your application. For visualisation purposes, you will be consuming from the output stream using Kinesis Data Analytics Studio, which provides an Apache Zeppelin Notebook from which can be used to visualise and interact with the data in real-time.

**IMPORTANT: This sample is provided for experimental purposes only and is not intended for use in production settings as it is.**