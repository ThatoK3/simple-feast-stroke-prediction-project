from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
import json

def load_data():
    """Load sample data"""
    with open("./data/raw/sample_patients.json", "r") as f:
        patients = json.load(f)
    return patients

def train_stroke_model():
    """Train a simple stroke prediction model"""
    
    # Start Spark session
    spark = SparkSession.builder \
        .appName("StrokePredictionTraining") \
        .master("local[*]") \
        .getOrCreate()
    
    # Load data
    patients = load_data()
    
    # Create DataFrame
    df = spark.createDataFrame(patients)
    print(f"Total records: {df.count()}")
    print(f"Stroke cases: {df.filter(df.stroke == 1).count()}")
    
    # Show sample
    print("\nSample data:")
    df.show(5)
    
    # Basic preprocessing
    # Convert categorical variables
    gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_idx")
    smoking_indexer = StringIndexer(inputCol="smoking_status", outputCol="smoking_idx")
    
    # Encode categorical variables
    encoder = OneHotEncoder(inputCols=["gender_idx", "smoking_idx"], 
                          outputCols=["gender_enc", "smoking_enc"])
    
    # Assemble features
    feature_cols = ["age", "hypertension", "heart_disease", 
                    "avg_glucose_level", "bmi", "gender_enc", "smoking_enc"]
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
    # Split data
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    
    # Train Random Forest model
    rf = RandomForestClassifier(
        labelCol="stroke",
        featuresCol="features",
        numTrees=50,
        maxDepth=5,
        seed=42
    )
    
    # Create pipeline
    pipeline = Pipeline(stages=[
        gender_indexer,
        smoking_indexer,
        encoder,
        assembler,
        rf
    ])
    
    # Train model
    print("\nTraining model...")
    model = pipeline.fit(train_df)
    
    # Evaluate on test set
    predictions = model.transform(test_df)
    
    # Calculate accuracy
    correct = predictions.filter(predictions.prediction == predictions.stroke).count()
    total = test_df.count()
    accuracy = correct / total
    
    print(f"\nModel Evaluation:")
    print(f"Test set size: {total}")
    print(f"Correct predictions: {correct}")
    print(f"Accuracy: {accuracy:.2%}")
    
    # Save model
    model.write().overwrite().save("../models/stroke_model")
    print("\nModel saved to '../models/stroke_model'")
    
    # Show feature importance
    rf_model = model.stages[-1]
    feature_importance = list(zip(feature_cols, rf_model.featureImportances))
    
    print("\nFeature Importance:")
    for feature, importance in feature_importance:
        print(f"{feature}: {importance:.4f}")
    
    spark.stop()
    return model

if __name__ == "__main__":
    train_stroke_model()
