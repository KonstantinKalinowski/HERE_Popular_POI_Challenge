# HERE_Popular_POI_Challenge

This repository contains a solution for Popular POI Challenge.

## Directories:

- input_data_sets - contains input data files (Uber rides data and POI data)

- output - contains the output file with the names of POIs sorted down by popularity (number of visits), i.e. most visited POIs are in top. 
(Note: Due to lack of calculation resources (see Test configuration Software below), the popularity was calculated for a limited set of POIs, so not all input POIs appear in the output file)

- src - contains PopularPOIs.scala file with the solution of Popular POI Challenge

- unit_tests - contains test.scala file with unit test of the functions

## Test configuration

### Hardware
- T2 Micro EC2 Instance (AWS)
- 1 CPU
- 1GB RAM

### Software
- Red Hat Enterprise Linux Server 7.6 (Maipo)
- Spark 2.4.0
- Scala 2.12.7
