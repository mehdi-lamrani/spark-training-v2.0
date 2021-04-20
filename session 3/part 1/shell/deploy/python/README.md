# Build & Deploy a Spark Program (Python version)

#### Goals :  
#### Understanding the impacts of Resource Management and Configuration  
#### Understanding the Role of Different Deploy Modes 
```
sudo su
cd
```


- create a project folder /root/deploy
- cd into to it
- `nano spark_program.py`
- copy the python [spark program](https://github.gamma.bcg.com/spark-training/spark-c-training/blob/master/day%203/part%201/shell/deploy/python/spark-program.py) in it 
- press `Ctrl X`  then `Enter` to Exit
- take a look at the code you just copied

- Session is initialized in the main
- Pretty straightoforward. 


- submit to Spark from the deploy folder : 

https://spark.apache.org/docs/latest/submitting-applications.html

- You Cluster counts :  

  * One Master
  * 3 Cores (Workers)
  
  * 4 CPU vCores per Node
  * 16 Gb RAM per Node


Play around with various scenarios :  
**Replace x's with experimental values**

```
spark-submit \
--deploy-mode <deploy-mode>  \
--driver-memory xg \
--conf spark.executor.memory=xg \
--conf spark.executor.core=x \
--conf spark.executors.max=x \
spark_program.py
```

try different deploy modes and executor settings  
try to monitor the execution in the Yarn Resource Manager accordingly

- Possible scenarios : 
  * User A (or client A) launches a submit with an agressive configuration using all cores and slots available,  
    saturating the cluster and exhausting its resources
  * User B (or client B) has no resources left available to launch a submit
  * After restarting resource manager (systemctl restart hadoop-yarn-resourcemanager) : 
  * User A launches a submit with a more minimal configuration
  * User B can now launch a submit and have resources 
  
- Question regarding Deploy Modes : 
  * What do you notice about logs, when you use cluster mode vs default/client mode ?
  * You can check the logs in the followinf hdfs folder : `/var/log/spark/apps`

- **Your are encouraged to customize the python code and make it do something heavier**
  **like reading all the prebuilt BOTS DATA from HDFS and performing operations on it.**

- CAUTION : As your program ends, so does your session.  
  Your Spark WEB UI is short-lived. You might have to check the History Server.
  In the Ressource Manager Web UI, The tracking URL will update from `ApplicationMaster` to `History`

- Check that your program runs as expected and produced the desired results

