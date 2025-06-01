# Raft Consensus Protocol — Distributed Key-Value Store

A robust distributed systems project implementing the [Raft consensus algorithm](https://raft.github.io/) to ensure consistent replication across fault-prone servers.

> “Good systems disappear into the background. Great ones never leave.”

---

## 🚀 Overview

This project was developed as part of Boston University's Distributed Systems Capstone (CS 351). It implements a fault-tolerant **key-value store** using Raft, a consensus protocol designed to be understandable and practical for real-world use.

- ⚙️ Built in **Go** using RPC
- 🧠 Supports **leader election**, **log replication**, and **crash recovery**
- 🔒 Ensures **strong consistency** in the presence of unreliable networks
- ✅ Passes 30+ rigorous tests on concurrency, partitioning, and crash scenarios

---

## 📌 Features

- **Leader Election** with randomized timeouts and majority voting
- **Log Replication** from leader to followers using AppendEntries RPCs
- **Commit Index Tracking** to guarantee consistency and durability
- **Crash Recovery** with persisted log and snapshot replay
- **Concurrency-safe** using Go channels and synchronization primitives

---

## 📦 Tech Stack

- **Language**: Go (Golang)
- **Communication**: RPC
- **Testing**: Provided unit and integration tests simulating network failures, slow disks, and node crashes

---

## 🧪 Testing & Results

Passed all MIT 6.824-style tests for:
- Log consistency under unreliable networks
- Fault tolerance with server crashes and restarts
- Election correctness with split votes and timeouts
