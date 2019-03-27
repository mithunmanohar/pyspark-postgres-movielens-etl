# spark-postgre-etl
Apache spark job to pull MovieLens data, process it and store results to post-gres database


## Setting up the environments

Requirements:
- git 
- docker
- docker-compose

1. Clone the code base from this github repo using command:

    **``git clone https://github.com/mithunmanohar/spark-postgre-etl.git``**
  
2. Change directory to folder:
  
    **``cd spark-postgre-etl``**

3. Build and start the spark container using docker compose file from this folder using command:
  
    **``docker-compose up --build -d``**
  
4. Log into the master docker container using command:

    **``docker exec -it sparkpostgresetl_master_1 /bin/bash``**
  
5. move to /spark_etl folder

    **``cd /spark_etl``**
  
6. install the python requirements using pip:
 
    **``pip install -r requirements.txt``**
  
6. Run the first spark job using spark-submit

    **``/usr/spark-2.4.0/bin/spark-submit /spark_etl/top_category_movies.py``**
  
  
