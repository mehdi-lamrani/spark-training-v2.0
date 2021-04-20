# Spark Workshop
# Getting acquainted to your working environment
  This is your goto reference page when you want to get a refresher about your technical resources

## Prerequisites : 

- **Your personal IP Address should be whitelisted by the trainer.** 

    * You need to retrieve your public IP address and fill it in [This Document]()

- **An EMR cluster will be assigned to you :**

    * Note : The Training coordinator will assign this during the training

### EMR Links to UI's & Resources

- Make **SURE** you are not behind a strict firewall and that all the following ports are open<br>
- In which case you might need to use your 4G connection (SSH & HTTP are light, no worries)
- Keep an eye on your antivirus as well, if you have one and experience connection issues (just force disable it)<br>

### Test URLs & Ports : 

- Once you have an EMR Cluster IP addressed to you :
- You should be able to access the following useful URLs

replace `provided.ip.add.ress` by the address that was assigned to you.

| JupyterHub | https://provided.ip.add.ress:9443/  |  **HTTPS**  DO NOT FORGET THE "S" otherwise it will NOT work |
|---|---|---|
| Yarn Resource manager | http://provided.ip.add.ress:8088/  |     |
| Zeppelin | http://provided.ip.add.ress:8890/  |    |
| Spark History Server |  http://provided.ip.add.ress:18080/	 |   |


		
For some URLs (JupyterHub) you might have this message : 

<img src="/res/img/your-connection-is-not-private.png" width="400">

On most browsers you can click on Advanced > Proceed

<img src="/res/img/your-connection-is-not-private-proceed.png" width="400">

On Some versions of Chrome however, there is an ugly hack :

    * Click a blank section of the denial page.
    * Using your keyboard, type thisisunsafe

Strange hack, but it works. 

### Test your SSH Connection : 

- After that, Please proceed to [ssh testing](https://github.com/mehdi-lamrani/spark-training-v2.0/blob/master/session%201/part%201/shell/exercices/00-terminal.md)

### Using Jupyter / Zeppelin  (Notebook Exercices)

A presentation will be made of both so you can choose which suits you best

- For Jupyter :
    * Reminder : **HTTPS**  DO NOT FORGET THE "S" in the URL otherwise it will NOT work 
    * you need to login : 
````
user : jovyan
password jupyter
````

<img src="/res/img/jupyter-login.png" width="300">




- For Zeppelin you can use anonymous user 

### LOADING NOTEBOOKS INTO JUPYTER & ZEPPELIN
  Notebooks should be downloaded (using the RAW Button, with the right extension) or pulled from github,**  
  and uploaded into jupyter/zeppelin using the import feature. 
  *If you need help with help ask the trainer for demo/assistance*

<br>
 <br>
  <br>
   <br>
    <br>
     <br>
      <br>
       <br>
        <br>
	 <br>
	  <br>
	   <br>
	    <br>
	     <br>
	      <br>
	       <br>
	       
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  


