import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, when, explode, avg, count, sum,
    regexp_replace, from_json, expr, rank, broadcast,
    concat_ws, lit, rand, pandas_udf, to_json
)
from pyspark.sql.types import (
    ArrayType, StructType, StructField, StringType, IntegerType, DoubleType
)
from pyspark.sql.window import Window
from typing import Optional

# ============================
# PANDAS UDF DEFINITION
# ============================
@pandas_udf(DoubleType())
def normalize_udf(col_series: pd.Series) -> pd.Series:
    """
    Pandas UDF (Vectorized UDF) to normalize a column using z-score:
    (X - mean) / std_dev. Runs faster than traditional UDFs.
    """
    # Handle the edge case where standard deviation is zero (all values are the same)
    std_dev = col_series.std()
    if std_dev == 0 or col_series.empty:
        # Return 0.0 for all elements if std_dev is zero
        return pd.Series([0.0] * len(col_series), dtype=float)
    return (col_series - col_series.mean()) / std_dev

def full_spark_pipeline(data_path: str = "file:///tmp/kaggle_data/*.csv", 
                        output_base_path: str = "file:///tmp/tableau_output") -> Optional[SparkSession]:
    """
    Executes an ultra-optimized PySpark data processing pipeline.
    
    Returns the active SparkSession instance on success.
    """

    # ===============================
    # SPARK SESSION WITH OPTIMIZATION
    # ===============================
    print("🚀 Initializing Spark Session with Optimizations...")
    spark = SparkSession.builder \
        .appName("UltraOptimizedSparkPipeline") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.memory.fraction", "0.80") \
        .getOrCreate()

    # Set checkpoint directory for lineage breaking
    spark.sparkContext.setCheckpointDir("file:///tmp/spark_checkpoint")
    
    try:
        # ============================
        # PHASE 1: EXTRACTION, CLEANING & PARQUET REWRITE
        # ============================
        print("\n=== PHASE 1: EXTRACTION, CLEANING & PARQUET REWRITE ===")

        # Read CSV data (Lazy transformation)
        df = spark.read.csv(
            data_path,
            header=True,
            inferSchema=True
        )

        # 1. Column Pruning (Spark Optimization Rule)
        df = df.select(
            "title", "release_date", "genres",
            "vote_average", "revenue", "budget"
        )

        # 2. Extract release year safely
        df = df.withColumn(
            "release_year",
            when(col("release_date").rlike(r"\d{4}"),
                regexp_extract(col("release_date"), r"(\d{4})", 1).cast("int"))
            .otherwise(None)
        )

        # 3. Optimized Parquet Write (ACTION & Partitioning)
        # Partitioning helps with data skipping and faster reads later.
        parquet_path = "file:///tmp/kaggle_data_parquet"
        df.write.mode("overwrite") \
            .option("compression", "snappy") \
            .partitionBy("release_year") \
            .parquet(parquet_path)

        print(f"Parquet created and partitioned by 'release_year' at {parquet_path}")

        # Reload the optimized Parquet to start the lineage fresh (Performance Boost)
        df = spark.read.parquet(parquet_path)
        print(f"Schema after reload: {df.printSchema()}")

        # ============================
        # PHASE 2: TYPE FIXING + GENRE EXPLOSION
        # ============================
        print("\n=== PHASE 2: TYPE FIXING + GENRE EXPLOSION ===")

        # Fix types using try_cast for robustness
        df = df.withColumn("vote_average_double", expr("try_cast(vote_average as double)")) \
               .withColumn("revenue_double", expr("try_cast(revenue as double)")) \
               .withColumn("budget_double", expr("try_cast(budget as double)"))

        # Cache a filtered subset for re-use (narrow transformation)
        df_filtered_high_rated = df.filter(col("vote_average_double") > 7)
        df_filtered_high_rated.cache()

        # Clean genres string and parse JSON
        df = df.withColumn("genres_json", regexp_replace("genres", "'", '"'))

        genre_schema = ArrayType(
            StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True)
            ])
        )

        df = df.withColumn("genres_array", from_json(col("genres_json"), genre_schema))

        # Wide Transformation: Explode genres (causes a shuffle)
        df_genre = df.withColumn("genre", explode("genres_array")) \
                     .withColumn("genre_name", col("genre.name"))

        df_genre_clean = df_genre.select(
            "title", "release_year", "genre_name",
            "vote_average_double", "revenue_double", "budget_double"
        ).filter(col("genre_name").isNotNull())


        # ============================
        # PHASE 3: ROI + RANKING
        # ============================
        print("\n=== PHASE 3: ROI + RANKING (Window Function) ===")

        df_clean = df.filter(
            (col("budget_double").isNotNull()) & (col("budget_double") > 0)
        )
        
        # <<< CRITICAL FIX: DROP COMPLEX COLUMN BEFORE OPERATIONS LEADING TO CSV EXPORT >>>
        # The 'genres_array' column is not needed for ROI ranking and will break CSV export.
        df_clean = df_clean.drop("genres_array") 
        # <<< END FIX >>>

        df_clean = df_clean.withColumn("profit", col("revenue_double") - col("budget_double")) \
                           .withColumn("roi", col("revenue_double") / col("budget_double"))

        # Repartition for better local aggregation/sorting before the Window function
        df_clean = df_clean.repartition("release_year")

        window = Window.partitionBy("release_year").orderBy(col("roi").desc())

        # Window Function (Wide Transformation - Shuffle)
        df_ranked = df_clean.withColumn("roi_rank", rank().over(window))

        # ACTION: Show top 5 movies by ROI per year
        print("Top 5 ROI Movies per Year:")
        df_ranked.filter(col("roi_rank") <= 5).show(20, truncate=False)

        # ============================
        # PHASE 4: SALTING FOR SKEW FIX (Grouped Aggregation)
        # ============================
        print("\n=== PHASE 4: DATA SKEW FIX WITH SALTING ===")
        # This technique handles data skew in a high-volume key (e.g., 'Drama' or 'Comedy' genre)

        # 1. Salt key (creates 10 copies of each genre)
        df_salted = df_genre_clean.withColumn(
            "salt",
            (rand() * 10).cast("int") # 10 salts
        ).withColumn(
            "genre_salted",
            concat_ws("_", col("genre_name"), col("salt"))
        )

        # 2. Use salted key in grouped aggregation (Spreads the work across more executors/partitions)
        genre_salted_agg = df_salted.groupBy("genre_salted").agg(
            count("*").alias("movie_count"),
            avg("vote_average_double").alias("avg_rating"),
            sum("revenue_double").alias("total_revenue")
        )

        # 3. Remove salt and re-aggregate for final genre stats
        genre_final = genre_salted_agg.withColumn(
            "genre_name", regexp_extract("genre_salted", r"(.*)_\d+", 1)
        )

        # Final aggregation (Should be fast as the data is less skewed now)
        genre_stats = genre_final.groupBy("genre_name").agg(
            sum("movie_count").alias("movie_count"),
            avg("avg_rating").alias("avg_rating"), # Average the averages
            sum("total_revenue").alias("total_revenue")
        ).orderBy(col("movie_count").desc())

        # ACTION: Show final genre stats
        print("\n📊 Genre Stats after SALTING and Re-aggregation:")
        genre_stats.show(20, truncate=False)

        # ============================
        # PHASE 5: BROADCAST JOIN
        # ============================
        print("\n=== PHASE 5: BROADCAST JOIN ===")
        # genre_stats is small, so we broadcast it to all executors to avoid a costly shuffle/sort-merge join.
        
        df_joined = df_genre_clean.join(
            broadcast(genre_stats),
            "genre_name",
            "inner"
        )

        # ACTION: Show joined data
        print("Joined data with Genre Statistics:")
        df_joined.show(5, truncate=False)

        # ============================
        # PHASE 6: CHECKPOINTING / LINEAGE RESET
        # ============================
        # Breaks the long chain of transformations (lineage) to prevent stack overflows
        # or overly long recovery times in case of node failure.
        df_checkpointed = df_joined.checkpoint(eager=True)
        print("\nCheckpointed (Lineage Reset) to break long lineage.")

        # ============================
        # PHASE 7: PANDAS UDF EXAMPLE
        # ============================
        # Apply the fast vectorized UDF for rating normalization
        df_norm = df_checkpointed.withColumn("normalized_rating", normalize_udf(col("vote_average_double")))

        # ACTION: Show normalized data
        print("\nPandas UDF Applied (Normalized Rating):")
        df_norm.show(5, truncate=False)

        # ============================
        # PHASE 8: EXPORT FOR ANALYTICS/TABLEAU
        # ============================
        print("\n=== PHASE 8: EXPORTING FILES ===")

        # 1. Save the ROI Rankings (No coalesce(1) for large datasets)
        roi_output_path = f"{output_base_path}/roi_rankings"
        df_ranked.filter(col("roi_rank") <= 50) \
            .write.mode("overwrite") \
            .option("header", "true") \
            .csv(roi_output_path)
        
        print(f"Saved ROI Rankings (multi-part CSV) to {roi_output_path}")

        # 2. Save the Aggregated Genre Stats (No coalesce(1))
        genre_stats_path = f"{output_base_path}/genre_stats"
        genre_stats \
            .write.mode("overwrite") \
            .option("header", "true") \
            .csv(genre_stats_path)

        print(f"Saved Genre Stats (multi-part CSV) to {genre_stats_path}")

        # 3. Save the Final Normalized Data (Parquet is efficient)
        final_parquet_path = f"{output_base_path}/final_detailed_data"
        df_norm.write.mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(final_parquet_path)
        
        print(f"Saved Final Detailed Data to {final_parquet_path}")

        return spark

    except Exception as e:
        print(f"❌ ERROR DURING PIPELINE EXECUTION: {e}")
        # Re-raise the exception to be caught in the main block
        raise
        
# -----------------------------------------

if __name__ == "__main__":
    spark_session = None
    try:
        # Execute the pipeline
        spark_session = full_spark_pipeline() 
        spark_session.stop()
        print("Spark session stopped cleanly.")
        print("\n✨ ALL SPARK OPTIMIZATIONS COMPLETED SUCCESSFULLY ✨")
    except Exception as e:
        # Handle the error outside the function
        print(f"PIPELINE FAILED. Spark will stop now.")
    # finally:
    #     # Ensure Spark session is always stopped
    #     if spark_session:
    #         spark_session.stop()
    #         print("Spark session stopp

