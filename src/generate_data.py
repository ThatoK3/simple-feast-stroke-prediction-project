import json
import random
from datetime import datetime, timedelta

def generate_sample_data(num_patients=100, save_path="./data/raw/sample_patients.json"):
    """Generate simple stroke prediction sample data"""
    
    patients = []
    
    for i in range(num_patients):
        # Base patient info
        age = random.randint(30, 80)
        gender = random.choice(["Male", "Female"])
        
        # Health metrics
        hypertension = 1 if random.random() < 0.1 else 0
        heart_disease = 1 if random.random() < 0.05 else 0
        avg_glucose_level = round(random.uniform(70, 200), 1)
        bmi = round(random.uniform(18, 40), 1)
        
        # Smoking status
        smoking_options = ["never smoked", "formerly smoked", "smokes", "Unknown"]
        smoking_status = random.choice(smoking_options)
        
        # Generate stroke label (realistic distribution: ~5% strokes)
        stroke = 0
        stroke_risk = 0.01  # Base risk
        
        # Increase risk factors
        if hypertension: stroke_risk += 0.15
        if heart_disease: stroke_risk += 0.20
        if age > 60: stroke_risk += 0.25
        if avg_glucose_level > 140: stroke_risk += 0.15
        if bmi > 30: stroke_risk += 0.10
        if smoking_status == "smokes": stroke_risk += 0.15
        
        stroke = 1 if random.random() < stroke_risk else 0
        
        patient = {
            "patient_id": f"P{10000 + i}",
            "age": age,
            "gender": gender,
            "hypertension": hypertension,
            "heart_disease": heart_disease,
            "avg_glucose_level": avg_glucose_level,
            "bmi": bmi,
            "smoking_status": smoking_status,
            "stroke": stroke,
            "last_checkup": (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat()
        }
        
        patients.append(patient)
    
    # Save to file
    with open(save_path, "w") as f:
        json.dump(patients, f, indent=2)
    
    print(f"Generated {num_patients} patient records")
    print(f"Stroke cases: {sum(p['stroke'] for p in patients)}")
    
    return patients

if __name__ == "__main__":
    generate_sample_data()
