# SPARK-ETL-SYSTEM-OF-MOVIES-DATASET
This project implements a robust ETL (Extract, Transform, Load) pipeline using Apache Spark to handle large-scale data efficiently. The pipeline extracts data from multiple sources, applies transformations including data cleaning, type casting, and aggregations, and loads the processed data into the target storage system. Optimizations such as partitioning, caching, and parallel processing were applied to enhance performance and reduce runtime.

The system is orchestrated using Airflow, allowing automated scheduling, dependency management, and monitoring of ETL tasks. Error handling, logging, and retry mechanisms were integrated to ensure reliability and fault tolerance. The project showcases scalable, production-ready ETL workflows capable of processing complex datasets with Spark’s distributed computing power.

SPARK TERMS USED
spark terms:

Spark Architecture			    
Catalyst Optimizer Overview                 
DataFrame vs RDD Optimization		    
Narrow vs Wide Transformations              
Filter & Column Pruning Optimization         
Join Optimization Techniques		    
Broadcast Joins				    
Handling Data Skew			    
Partitioning & Parallelism Tuning           
Repartition vs Coalesce			    
Caching & Persistence Strategies 	    
Optimizing Shuffle Operations  		    
File Format Optimization (Parquet/ORC)       
Using Compression Efficiently		    
UDF Optimization (Avoiding Python UDFs)     
Pandas UDFs				    
Adaptive Query Execution (AQE)		    
Memory Tuning & Executor Sizing		    
Garbage Collection Optimization
Spark UI Performance Monitoring		    
Cluster Resource Optimization
Checkpointing & Lineage Management	
