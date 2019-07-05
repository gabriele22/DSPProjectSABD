# DSP-Project-SABD

The goal of this project is to answer the first two queries in the document: <https://drive.google.com/file/d/1mVxpYWk_OQdDZBCSqUck_pTQ8WwbEC21/view?usp=sharing>.
Furthermore it is possible to create a standalone cluster with docker and docker-compose to test everything.

## Pre requisites
You need to have installed:
* docker
* docker-compose
* gnome-terminal

You need to download dataset file:
<https://drive.google.com/file/d/1DHyqsNoQVs0waY3q6nCqNKimUuqQLb-D/view?usp=sharing>

## Launch environment
First download and unzip the file: <https://drive.google.com/file/d/1LfUSs_qrdwZ6MCySFLrciwRBKwBt2wrw/view?usp=sharing>
In the directory created, open a terminal and execute this command:
```bash
sh launchEnvironment.sh  yourPathFileDataset
```

## Usage
* The **Generator** will replicate the data on a kafka topic
  * between sending a comment and the next one will wait a number of milliseconds equal to the number of minutes between the creation date of the two comments
* **Flink Job** will read the data from the topic kafka and execute the two queries by publishing the results on other kafka topics 
  * one topic for each window of each query (six topics)
  
Execute this command to start the **Generator** and **Flink Job**: 
```bash
sh startGeneratorAndFlinkJob.sh
```
Execute this command to view all results (in six tab of your terminal):
```bash
sh viewAllResults.sh
```
it is also possible to display a result (of a window) at a time by going to the /results folder and starting the scripts

To access the **Apache Flink Web Dashboard** copy this link on your browser: 
<http://localhost:8081/> 


## Stop environment
```bash
sh stopAndClean.sh
```
