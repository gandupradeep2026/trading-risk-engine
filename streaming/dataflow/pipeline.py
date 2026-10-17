"""Cloud Dataflow Pipeline Builder for Real-Time Trade Stream Ingestion.

Builds the Apache Beam pipeline DAG:
1. Consumes trade execution events from Google Cloud Pub/Sub using Pub/Sub IO.
2. Decodes UTF-8 JSON payloads and validates quantitative invariants.
3. Separates valid trade executions from corrupt/malformed records into tagged outputs.
4. Routes deadlettered records to the dedicated Cloud Pub/Sub Dead-Letter Queue (DLQ).
5. Exposes the valid orders stream for downstream windowing (Day 09) and multi-output sinks (Day 10).
"""

from dataclasses import dataclass
import logging
from typing import Optional

import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub, WriteToPubSub

from streaming.dataflow.decoders import (
    TAG_DEADLETTER,
    TAG_VALID_ORDERS,
    DecodeAndValidateOrders,
    FormatDeadLetterForPubSubFn,
)
from streaming.dataflow.options import RiskEnginePipelineOptions

logger = logging.getLogger(__name__)


@dataclass
class DataflowPipelineBundle:
    """Encapsulates the constructed Apache Beam pipeline and its core output PCollections."""

    pipeline: beam.Pipeline
    valid_orders: beam.PCollection
    deadletter_records: beam.PCollection
    raw_messages: Optional[beam.PCollection] = None
    dlq_messages: Optional[beam.PCollection] = None


def build_pipeline(
    options: RiskEnginePipelineOptions,
    pipeline: Optional[beam.Pipeline] = None,
    test_input_pcoll: Optional[beam.PCollection] = None,
) -> DataflowPipelineBundle:
    """Construct the Apache Beam trading risk streaming pipeline.

    Args:
        options: Configured RiskEnginePipelineOptions instance.
        pipeline: Optional pre-initialized Beam pipeline (useful for TestPipeline).
        test_input_pcoll: Optional PCollection to use as the input source for zero-cost tests.

    Returns:
        DataflowPipelineBundle containing the pipeline and its branched PCollections.
    """
    p = pipeline or beam.Pipeline(options=options)

    # 1. Source Step: Test input PCollection OR Cloud Pub/Sub IO
    if test_input_pcoll is not None:
        raw_messages = test_input_pcoll
    elif options.input_subscription:
        logger.info("Configuring Pub/Sub IO consumer on subscription: %s", options.input_subscription)
        raw_messages = p | "ReadFromPubSubSubscription" >> ReadFromPubSub(
            subscription=options.input_subscription,
            with_attributes=options.enable_attributes,
        )
    elif options.input_topic:
        logger.info("Configuring Pub/Sub IO consumer on topic: %s", options.input_topic)
        raw_messages = p | "ReadFromPubSubTopic" >> ReadFromPubSub(
            topic=options.input_topic,
            with_attributes=options.enable_attributes,
        )
    else:
        raise ValueError(
            "Streaming pipeline requires either --input_subscription or --input_topic to be specified."
        )

    # 2. Decoding & Validation Step: Split into Valid Orders and Dead-Letter Records
    outputs = raw_messages | "DecodeAndValidateOrderEvents" >> DecodeAndValidateOrders()
    valid_orders = outputs[TAG_VALID_ORDERS]
    deadletter_records = outputs[TAG_DEADLETTER]

    # 3. Sink Step: Dead-Letter Queue (DLQ) Publishing
    dlq_messages = None
    if options.deadletter_topic and options.enable_deadletter_sink:
        logger.info("Configuring Pub/Sub DLQ sink to topic: %s", options.deadletter_topic)
        dlq_messages = deadletter_records | "FormatDeadLetterForPubSub" >> beam.ParDo(
            FormatDeadLetterForPubSubFn()
        )
        _ = dlq_messages | "WriteToDeadLetterTopic" >> WriteToPubSub(
            topic=options.deadletter_topic,
            with_attributes=True,
        )

    return DataflowPipelineBundle(
        pipeline=p,
        valid_orders=valid_orders,
        deadletter_records=deadletter_records,
        raw_messages=raw_messages,
        dlq_messages=dlq_messages,
    )
