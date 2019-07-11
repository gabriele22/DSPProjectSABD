# DSP-Project-SABD

The goal of this project is to answer the first two queries in the document: <https://drive.google.com/file/d/1mVxpYWk_OQdDZBCSqUck_pTQ8WwbEC21/view?usp=sharing>.
Furthermore it is possible to create a standalone cluster with docker and docker-compose to test everything.

## Core Components
* The **Generator** will replicate the data on kafka topic "comments"
  * between sending a comment and the next one will wait a number of milliseconds equal to the number of minutes between the creation date of the two comments
* **Query** will read the data from the topic "comments" and publishing the results on others kafka topics 
  * one topic for each window of each query (6 topic)

## Pre requisites
You need to have installed:
* docker
* docker-compose
* gnome-terminal

You need to download dataset file:
<https://drive.google.com/file/d/1DHyqsNoQVs0waY3q6nCqNKimUuqQLb-D/view?usp=sharing>

## Launch environment
First download and unzip the file: <https://drive.google.com/file/d/1dsEvkwntI-bqKXuqIpgOgvUB9YiZJBrF/view?usp=sharing>
In the directory created, open a terminal and execute this command:
```bash
sh launchEnvironment.sh  /yourPathFileDataset/..
```

## Usage  
Execute this command to start the **Generator** and **Queries**: 
```bash
sh startGeneratorAndQueries.sh  true
```
(if you don't want to activate custom latency tracking, **don't enter true**)

Execute this command to view all results (in 6 tab of your terminal):
```bash
sh viewResults.sh
```


## Access Flink Web Dashboard
To access the **Apache Flink Web Dashboard** copy this link on your browser: 
<http://localhost:8081/> 


## Stop environment
```bash
sh stopEnvironment.sh
```
