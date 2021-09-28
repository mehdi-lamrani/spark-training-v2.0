## SPARK SHELL introduction

The Spark consoles are two command line interfaces that are provided natively by Spark Core  
One is provided in scala (native spark)-spark-shell, the other is provided in Python (out-of-the-box) -pyspark. 
Although they may not be convenient for day to day in depth work, they do provide a robust interface for testing and exucuting spark commands in REPL mode.  

We will stick to the python shell for this training, ignoring scala shell for now.<br>
*PS : There are many considerations as to when it is more convenient to use one language API over the other.*  
*If you are interested in this topic, feel free to ask the trainer, he'll be gland to provide you with some insights*

### Caution !!!
As will be explained later, do NOT launch more than ONE shell per user. 
Always quit your sessions properly (:q in scalla or exit() in pyspark).  
Being careless regarding this warning might result in freezing your shells,  
and requiring to restart services and killing ghost jobs.
While this is not a big deal, it might cause some inconvenience.

A thorough explanation on how to properly tune your ressources to avoid that will be provided later in the course. 


#### PART ONE : Connecting to Terminal

- [Connecting to Terminal](https://github.com/mehdi-lamrani/spark-training-v2.0/blob/main/session%201/part%201/shell/exercices/00-terminal.md)

#### PART TWO : Loading Data into HDFS

- [Loading Data](https://github.com/mehdi-lamrani/spark-training-v2.0/blob/main/session%201/part%201/shell/exercices/01.load-data.md)

#### PART THREE : First steps with pyspark shell

- [Hello Pyspark Shell](https://github.com/mehdi-lamrani/spark-training-v2.0/blob/main/session%201/part%201/shell/exercices/02-pyspark-shell.md)
