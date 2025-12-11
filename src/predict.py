from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import json

class StrokePredictor:
    """Simple stroke prediction service"""
    
    def __init__(self, model_path="../models/stroke_model"):
        self.spark = SparkSession.builder \
            .appName("StrokePredictionInference") \
            .master("local[*]") \
            .getOrCreate()
        
        # Load trained model
        self.model = PipelineModel.load(model_path)
        print("Model loaded successfully!")
    
    def predict_single(self, patient_data):
        """Predict stroke risk for a single patient"""
        
        # Define schema
        schema = StructType([
            StructField("patient_id", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
            StructField("hypertension", IntegerType(), True),
            StructField("heart_disease", IntegerType(), True),
            StructField("avg_glucose_level", FloatType(), True),
            StructField("bmi", FloatType(), True),
            StructField("smoking_status", StringType(), True),
        ])
        
        # Create single-row DataFrame
        df = self.spark.createDataFrame([patient_data], schema=schema)
        
        # Make prediction
        prediction = self.model.transform(df)
        
        # Extract results
        result = prediction.select(
            "patient_id", 
            "prediction", 
            "probability"
        ).collect()[0]
        
        # Format output
        stroke_prob = float(result["probability"][1])  # Probability of stroke (class 1)
        
        return {
            "patient_id": result["patient_id"],
            "stroke_prediction": int(result["prediction"]),
            "stroke_probability": round(stroke_prob, 3),
            "risk_level": "High" if stroke_prob > 0.5 else "Low"
        }
    
    def predict_batch(self, patient_list):
        """Predict stroke risk for multiple patients"""
        results = []
        
        for patient in patient_list:
            try:
                result = self.predict_single(patient)
                results.append(result)
            except Exception as e:
                print(f"Error predicting for patient {patient.get('patient_id')}: {e}")
                results.append({
                    "patient_id": patient.get("patient_id"),
                    "error": str(e)
                })
        
        return results
    
    def predict_from_file(self, file_path):
        """Predict from a JSON file"""
        with open(file_path, "r") as f:
            patients = json.load(f)
        
        return self.predict_batch(patients)

# Example usage
if __name__ == "__main__":
    # Initialize predictor
    predictor = StrokePredictor()
    
    # Example patient (similar to your sample)
    example_patient = {
        "patient_id": "P10001",
        "age": 40,
        "gender": "Female",
        "hypertension": 0,
        "heart_disease": 0,
        "avg_glucose_level": 87.7,
        "bmi": 29.8,
        "smoking_status": "smokes"
    }
    
    print("Making prediction for example patient:")
    print(json.dumps(example_patient, indent=2))
    
    # Make prediction
    result = predictor.predict_single(example_patient)
    
    print("\nPrediction Result:")
    print(json.dumps(result, indent=2))
    
    # Example batch prediction
    print("\n" + "="*50)
    print("Batch Prediction Example:")
    
    batch_patients = [
        {
            "patient_id": "P10002",
            "age": 65,
            "gender": "Male",
            "hypertension": 1,
            "heart_disease": 0,
            "avg_glucose_level": 180.5,
            "bmi": 32.1,
            "smoking_status": "formerly smoked"
        },
        {
            "patient_id": "P10003",
            "age": 28,
            "gender": "Female",
            "hypertension": 0,
            "heart_disease": 0,
            "avg_glucose_level": 92.3,
            "bmi": 22.5,
            "smoking_status": "never smoked"
        }
    ]
    
    batch_results = predictor.predict_batch(batch_patients)
    for i, result in enumerate(batch_results, 1):
        print(f"\nPatient {i}:")
        print(json.dumps(result, indent=2))
