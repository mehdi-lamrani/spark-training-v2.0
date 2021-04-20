# using SSH & Terminal

This guide will guide you through establishing an SSH connection to your cluster

## Windows : 

- Install [Mobaxterm](https://download.mobatek.net/2022020030522248/MobaXterm_Portable_v20.2.zip)

- Download the following private key : 

  Head to thie following page : [ppk file](/res/bin/ALTEN440.ppk)<br>
  **IMPORTANT : Select "Raw" (upper right corner button above the github document), click, then save (Ctrl-S or Cmd-S).**   
  **Make SURE to remove .txt extension and keep .ppk**

- Use MobaXterm to ssh to the test machine : Sessions > New Session > SSH TAB
```
    User : ec2-user
    Remote Host : your.ip.add.ress
    Check Use Private Key : select ppk file path
```
![mobaxterm](/res/img/mobaxterm.png)



## MacOS / Linux : 

- Download the following private key : 

  Head to thie following page : [pem file](/res/bin/ALTEN440.pem)<br>
  **IMPORTANT : Select "Raw" (upper right corner button above the github document), click, then save (Ctrl-S or Cmd-S).**   
  **Make SURE to remove .txt extension and keep .pem**


- After downloading, you need to assign the right permissions to the key

```
    chmod 400 GAMMA0511.pem
```

- Connect via SSH 
    * replace assigned.cluster.ip.address with the provided cluster address. 

```
    ssh -i GAMMA0511.pem ec2-user@assigned.cluster.ip.address
```

### AS SOON AS YOU SSH IN

Your access is confirmed. 
