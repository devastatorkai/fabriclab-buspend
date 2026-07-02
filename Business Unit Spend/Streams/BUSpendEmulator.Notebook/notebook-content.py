# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "event_stream": {
# META       "known_event_streams": [
# META         {
# META           "artifact_id": "cfcc69d6-324d-413f-b724-35416b128d3b",
# META           "stream_id": "cfcc69d6-324d-413f-b724-35416b128d3b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

! python --version
! pip install azure-eventhub==5.11.5 --upgrade --force --quiet
! pip install semantic-link-labs --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Business Unit Spending Analytics – Synthetic Stream
# ## Real-time cost, trends, and budget variance data generator

# CELL ********************

import json
from azure.eventhub import EventHubProducerClient, EventData
import os
import socket
import uuid
import datetime
import random
from random import randrange
import time
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, List
import sempy_labs as labs
import sempy_labs.variable_library as sempy_variable_library
import sempy_labs.eventstream as sempy_eventstream

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Resolve Eventstream backing Event Hub for business unit spending analytics
# Assumes an Eventstream named "BusinessUnitSpendingStream" with a custom source

eventstream = "BUSpendStream"
eventstream_source_name = "CustomEndpoint-Source"

es_topology = sempy_eventstream.get_eventstream_topology(eventstream=eventstream)
es_source = es_topology[es_topology["Eventstream Source Name"] == eventstream_source_name]
es_source_id = es_source["Eventstream Source Id"].iloc[0]

es_source_connection = sempy_eventstream.get_eventstream_source_connection(
    eventstream=eventstream,
    source_id=es_source_id
)

es_eventhub_name = es_source_connection["EventHub Name"].iloc[0]
es_eventhub_connstring = es_source_connection["Primary Connection String"].iloc[0]

producer = EventHubProducerClient.from_connection_string(
    conn_str=es_eventhub_connstring,
    eventhub_name=es_eventhub_name,
)

hostname = socket.gethostname()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configuration and reference data for spending & budget variance analytics

BUSINESS_UNITS = [
    "Corporate",
    "Sales",
    "Marketing",
    "Operations",
    "R&D",
    "Customer Success",
]

COST_CENTERS = [
    "BU10-IT",
    "BU20-FieldSales",
    "BU30-DigitalMarketing",
    "BU40-Manufacturing",
    "BU50-ProductDev",
    "BU60-Support",
]

SPEND_CATEGORIES = [
    "Cloud Infrastructure",
    "Travel & Entertainment",
    "Advertising",
    "Software Licenses",
    "Contractors",
    "Training",
]

CURRENCIES = ["USD", "EUR", "GBP", "AUD", "CAD"]

# Thresholds and heuristics to flag potential overspend conditions
BUDGET_RULES = {
    "VARIANCE_ALERT_PCT": 0.10,   # 10% over budget
    "VARIANCE_CRITICAL_PCT": 0.25,  # 25% over budget
    "RAPID_SPEND_WINDOW_SECONDS": 60,  # window for bursty spend
    "RAPID_SPEND_COUNT_THRESHOLD": 5,  # events within window to flag
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class BusinessSpendingDataGenerator:
    """Synthetic generator for business unit spending and budget variance events."""

    def __init__(self):
        # Track spend activity per (business_unit, cost_center, category)
        self.spend_sessions = {}
        self.last_timestamp = datetime.now()

    def _key(self, bu: str, cc: str, category: str) -> str:
        return f"{bu}|{cc}|{category}"

    def _init_session(self, key: str, budget_amount: float, fiscal_period: str):
        self.spend_sessions[key] = {
            "fiscal_period": fiscal_period,
            "budget_amount": budget_amount,
            "cumulative_spend": 0.0,
            "events": [],
        }

    def _update_session(self, key: str, amount: float, event_time: datetime):
        session = self.spend_sessions[key]
        session["cumulative_spend"] += amount
        session["events"].append(event_time)

        # Trim events outside the rapid-spend window
        window_seconds = BUDGET_RULES["RAPID_SPEND_WINDOW_SECONDS"]
        cutoff = event_time - timedelta(seconds=window_seconds)
        session["events"] = [ts for ts in session["events"] if ts >= cutoff]

    def _assess_budget_health(self, key: str) -> Dict:
        session = self.spend_sessions[key]
        budget = session["budget_amount"]
        spend = session["cumulative_spend"]
        variance = spend - budget
        variance_pct = variance / budget if budget > 0 else 0.0

        is_over_budget = variance > 0
        alert_level = "None"
        reasons = []

        if is_over_budget:
            if variance_pct >= BUDGET_RULES["VARIANCE_CRITICAL_PCT"]:
                alert_level = "Critical"
                reasons.append("SevereBudgetOverrun")
            elif variance_pct >= BUDGET_RULES["VARIANCE_ALERT_PCT"]:
                alert_level = "Warning"
                reasons.append("EarlyBudgetOverrun")

        # Rapid/bursty spending pattern
        if len(session["events"]) >= BUDGET_RULES["RAPID_SPEND_COUNT_THRESHOLD"]:
            if alert_level == "None":
                alert_level = "Info"
            reasons.append("RapidSpendingPattern")

        return {
            "is_over_budget": is_over_budget,
            "variance_amount": round(variance, 2),
            "variance_pct": round(variance_pct, 4),
            "alert_level": alert_level,
            "alert_reasons": reasons,
        }

    def generate_event(self, *, force_over_budget: bool = False) -> Dict:
        """Generate a single synthetic spending event."""
        # Evolve time forward a bit for each event
        self.last_timestamp += timedelta(seconds=random.randint(5, 120))

        business_unit = random.choice(BUSINESS_UNITS)
        cost_center = random.choice(COST_CENTERS)
        spend_category = random.choice(SPEND_CATEGORIES)
        currency = random.choice(CURRENCIES)

        # Monthly budget for this (BU, CC, category)
        base_budget = random.randint(10_000, 250_000)
        fiscal_period = f"FY{datetime.now().year}-M{random.randint(1, 12):02d}"

        key = self._key(business_unit, cost_center, spend_category)
        if key not in self.spend_sessions:
            self._init_session(key, budget_amount=base_budget, fiscal_period=fiscal_period)

        # Typical spend amount
        amount = random.uniform(500, 15_000)

        # If forcing budget pressure, push higher amounts
        if force_over_budget:
            amount *= random.uniform(2.0, 4.0)

        event_time = self.last_timestamp
        self._update_session(key, amount, event_time)

        budget_state = self._assess_budget_health(key)

        data = {
            "event_id": str(uuid.uuid4()),
            "timestamp": event_time,
            "ingest_hostname": hostname,
            "business_unit": business_unit,
            "cost_center": cost_center,
            "spend_category": spend_category,
            "currency": currency,
            "fiscal_period": self.spend_sessions[key]["fiscal_period"],
            "budget_amount": self.spend_sessions[key]["budget_amount"],
            "transaction_amount": round(amount, 2),
            "cumulative_spend": round(self.spend_sessions[key]["cumulative_spend"], 2),
        }

        data.update(budget_state)
        return data

    def generate_batch(self, count: int = 25, over_budget_ratio: float = 0.2) -> List[Dict]:
        """Generate a batch of spending events.

        :param count: number of events in the batch.
        :param over_budget_ratio: fraction of events that intentionally
                                  push scenarios closer to or beyond budget.
        """
        events: List[Dict] = []
        over_budget_events = int(count * over_budget_ratio)

        for i in range(count):
            force_over_budget = i < over_budget_events
            events.append(self.generate_event(force_over_budget=force_over_budget))

        return events

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize generator and stream business spending events to EventHub

generator = BusinessSpendingDataGenerator()

sleep_seconds = 1
max_events = None  # set to an int for a finite run
batch_size = 25    # Send events in batches for efficiency

events: List[Dict] = []
event_count = 0

print("Connecting to EventHub for Business Unit Spending Analytics ...")
print("Streaming synthetic spending events – press Stop/Interrupt to end.\n")


event_batch = producer.create_batch()

while max_events is None or event_count < max_events:
    # Roughly 30% of events will try to push scenarios closer to/over budget
    force_over_budget = random.random() < 0.30
    event = generator.generate_event(force_over_budget=force_over_budget)
    events.append(event)
    event_count += 1

    # Keep a rolling window to avoid unbounded memory growth for local snapshot
    if len(events) > 2000:
        events = events[-2000:]

    event_json = json.dumps(event, default=str)

    try:
        event_batch.add(EventData(event_json))
    except ValueError:
        # Batch is full, send it and create a new batch
        producer.send_batch(event_batch)
        print(f"✓ Sent batch of {event_count} events to EventHub")
        event_batch = producer.create_batch()
        event_batch.add(EventData(event_json))

    # Send batch every batch_size events
    if event_count % batch_size == 0:
        producer.send_batch(event_batch)

        last_batch = events[-batch_size:]
        over_budget_cnt = sum(1 for e in last_batch if e.get("is_over_budget"))
        critical_cnt = sum(1 for e in last_batch if e.get("alert_level") == "Critical")

        print(
            f"✓ Sent {event_count} events | "
            f"Last batch: {over_budget_cnt}/{batch_size} over budget, "
            f"{critical_cnt} critical alerts"
        )

        event_batch = producer.create_batch()

    time.sleep(sleep_seconds)

# Send any remaining events in the batch
if len(event_batch) > 0:
    producer.send_batch(event_batch)
    print("✓ Sent final batch to EventHub")

# Latest snapshot as DataFrame
snapshot_df = pd.DataFrame(events)
snapshot_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
