# Design and implementation of software for the analysis of a set of metrics
## Description 
Creation of software based on four microservices (contained in Docker containers) that communicate with each other via a Kafka broker that manages the exchange of various messages. These take a set of metrics provided by a Prometheus exporter and analyze them in order to calculate various statistics (autocorrelation, seasonality, stationarity, execution times, past and future violations, etc.) and store everything within a MySQL database.

## Documetation
For a detailed explanation of how the project works, see the documentation:
- ITA: [qui](Docs/Documentazione.pdf).
- ENG: [qui](Docs/Documentation.pdf).
