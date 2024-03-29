### Using MLFlow to Tracks, Log, & Serve Machine Learning Models :

### PLEASE BE SUPER CAREFUL AND DO NOT SKIP/FORGET ANY INSTRUCTION
Things can easily go south if you misconfigure any part.   
In case of doubt, Call the trainer in immediately. 
**BE WARNED**  

### Install MLFlow :

- ALL THE FOLLOWING STEPS NEED TO BE PERFORMED FROM THE SSH LINUX TERMINAL

```
sudo su 
```

```
pip3 install pyspark
```

```
pip3 install mlflow
```

### Set Environment Variables :

```
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
```

````
cd
nano  .bashrc
````

Add :

````
export PATH=$PATH:/usr/local/bin
````

Ctrl-X Enter

````
source .bashrc
````

### Initialize the MLRun DB  
- do not forget the ampersand symbol & at the end of the command

```
mlflow ui --backend-store-uri /mlruns --host 0.0.0.0&

Hit Ctrl-C to clear out
```

### MANDATORY PERMISSIONS 

- This will make it possible to log your model from Jupyter

```
chmod -R 777 /mlruns
```


### Run MLflow projects :

MLflow allows you to package code and its dependencies as a project that can be run in a reproducible fashion on other data.   
Each project includes its code and a MLproject file that defines its dependencies.

```
# DO NOT RUN THIS
mlflow run sklearn_elasticnet_wine -P alpha=0.5
mlflow run https://github.com/mlflow/mlflow-example.git -P alpha=5.0
```

By default MLflow run installs all dependencies using conda.   
To run a project without using conda, you can provide the --no-conda option to mlflow run.   
In this case, you must ensure that the necessary dependencies are already installed in your Python environment.


### Saving Models :

MLflow includes a generic MLmodel format for saving models from a variety of tools.  
Many models can be served as Python functions, so an MLmodel file can declare how each model should be interpreted as a Python function in order to let various tools serve it.

***These are the steps you need to perfom in your notebook :***  
*(Example)*

```
    mlflow.start_run()
    
    mlflow.log_metric("rmse", rmse)
    mlflow.spark.log_model(lr, "model")

    print("Model saved in run %s" % mlflow.active_run().info.run_uuid)
    
    mlflow.end_run()
    
```

### Take a Look at the the output folder structure :

```
cd /mlruns/
tree
````
```
[root@ip-172-31-47-65:~]# cd /mlruns/
[root@ip-172-31-47-65:/mlruns]# tree
.
`-- 0
    |-- feb6865daf96493686eff1149147fd0f
    |   |-- artifacts
    |   |   `-- ALSmodel_Lite7
    |   |       |-- conda.yaml
    |   |       |-- MLmodel
    |   |       `-- sparkml
    |   |           |-- metadata
    |   |           |   |-- part-00000
    |   |           |   `-- _SUCCESS
    |   |           `-- stages
    |   |               `-- 0_ALS_08b1eaeb93b3
    |   |                   |-- itemFactors
    |   |                   |   |-- part-00000-22e913cc-434f-47cc-8df7-4687cd33ad0a-c000.snappy.parquet
    |   |                   |   |-- part-00001-22e913cc-434f-47cc-8df7-4687cd33ad0a-c000.snappy.parquet
    |   |                   |   `-- _SUCCESS
    |   |                   |-- metadata
    |   |                   |   |-- part-00000
    |   |                   |   `-- _SUCCESS
    |   |                   `-- userFactors
    |   |                       |-- part-00000-41e2ed6c-1496-4b34-bcf5-8f19db45bd93-c000.snappy.parquet
    |   |                       |-- part-00001-41e2ed6c-1496-4b34-bcf5-8f19db45bd93-c000.snappy.parquet
    |   |                       `-- _SUCCESS
    |   |-- meta.yaml
    |   |-- metrics
    |   |-- params
    |   `-- tags
    |       |-- mlflow.log-model.history
    |       |-- mlflow.source.name
    |       |-- mlflow.source.type
    |       `-- mlflow.user
    `-- meta.yaml
```

The model run saves a model folder containing an MLmodel description file and a pickled model.

```
ll /mlruns/0/7de69dabe1254ed09e681b33143ba05e/artifacts/yourmodelversion
total 12
-rw-rw-r-- 1 jupyter root 150 Nov  2 16:37 conda.yaml
-rw-rw-r-- 1 jupyter root  349 Nov  2 16:37 MLmodel
-rw-rw-r-- 1 jupyter root  717 Nov  2 16:37 model.pkl
```

### Use the tracking UI for monitoring :

- do not forget the ampersand symbol & at the end of the command

- you might need to hit ctrl-c to clear your console

```
mlflow ui --backend-store-uri /mlruns --host 0.0.0.0&
```

go to http://YOUR.IP.ADD.RESS:5000 :

(Locking might occur while logging your model and viewing the UI depending on your settings, so careful with that)


### Serving Your Model :

You can pass the run ID and the path of the model within the artifacts directory (here “model”) to various tools. For example, MLflow includes a simple REST server for python-based models:

Before running the command below : 
- change *ALSmodel_Lite2* with the name of *YOUR* model
- change *6c8050941d0744b8ac3652ff22d40983* with the UUID of *YOUR* model

```
mlflow models serve -m /mlruns/0/6c8050941d0744b8ac3652ff22d40983/artifacts/ALSmodel_Lite2 --port 9999 --no-conda
```

Once you have started the server, you can pass some sample data to it, and see the predictions.

The following example uses curl to send a JSON-serialized pandas DataFrame with the split orientation to the model server.

replace `[[1], [2]]` with convenient values (userid's)
```
curl -d '{"columns":["x"], "data":[[1], [2]]}' -H 'Content-Type: application/json; format=pandas-split' -X POST localhost:1234/invocations
```

### Feed the Model with input to get predictions :

You have 2 data files at your disposal in the repository :   
  
`mini_data.json`  
`mlf_data.json`  

go ahead and create those two files on your server (ssh, nano, and copy the content)


replace `data.json` with  `mini_data.json` or `mlf_data.json`in the following request : 


```
curl -X POST http://localhost:1234/invocations -H 'Content-Type:application/json; format=pandas-split' --data @data.json
```

Predictions we get as a response :
```
[4.185961723327637, 5.017632007598877, 4.221134185791016, 4.160998821258545, 4.070070743560791, 3.9219417572021484, 4.536161422729492, 4.624823570251465, 3.9956836700439453, 4.27530574798584, 3.757713556289673, 3.5301849842071533, 4.527366638183594, 3.1451196670532227, 4.3990559577941895, 4.171773910522461, 4.676153182983398, 2.3953475952148438, 3.2655463218688965, 2.738560438156128, 3.380716323852539, 3.8774654865264893, 2.6467792987823486, 2.395434856414795, 2.8622171878814697, 3.0062966346740723, 2.8033201694488525, 1.7942183017730713, 2.9179649353027344, 2.179030656814575, 3.141751289367676, 3.07222318649292, 2.8560099601745605, 3.8560731410980225, 3.7004659175872803, 3.8146746158599854, 4.351219654083252, 3.917738437652588, 4.250244140625, 4.247183322906494, 3.844766616821289, 1.2435023784637451, 4.221549034118652, 3.9631974697113037, 3.678105115890503, 4.139392852783203, 4.297109603881836, 3.7283849716186523, 4.264794826507568, 4.399022579193115, 4.006529808044434, 3.7217979431152344, 3.925201177597046, 3.6419496536254883, 4.039632797241211, 4.193242073059082, 3.5420167446136475, 3.6923434734344482, 3.125166893005371, 4.210026741027832, 3.4013400077819824, 3.238482713699341, 3.635953426361084, 2.5744640827178955, 3.237940788269043, 4.278573513031006, 3.8227856159210205, 3.5024781227111816, 4.061182022094727, 3.9683773517608643, 3.9475533962249756, 4.997380256652832, 3.791321277618408, 5.832394599914551, 3.7191879749298096, 5.213862419128418, 4.370209693908691, 4.574703216552734, 4.5095133781433105, 4.840439796447754, 4.183916091918945, 4.2347187995910645, 4.190977573394775, 6.775339126586914, 3.3537826538085938, 4.3036580085754395, 4.018661975860596, 3.0708861351013184, 4.434290409088135, 4.1051201820373535, 4.570611000061035, 5.592574119567871, 3.9886388778686523, 4.841955184936523, 4.624236106872559, 4.026106357574463, 3.9284157752990723, 2.7694056034088135, 4.089580535888672, 3.770362377166748]
```

### Training & logging a Model from a python file :

On the command line we can give to the program values for maxIter and regParam parameters :

```
spark-submit movie-lens-mlflow-model.py 2 0.005
```

This is a manual process, for demo, but it can of course be generalized and looped/automated
