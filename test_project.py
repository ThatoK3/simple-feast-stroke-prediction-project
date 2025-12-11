import subprocess
import json
import os

def test_complete_flow():
    """Test the complete project flow"""
    
    print("="*60)
    print("Testing Complete Stroke Prediction Project")
    print("="*60)
    
    # Step 1: Generate data
    print("\n1. Generating sample data...")
    subprocess.run(["python", "src/generate_data.py"])
    
    # Step 2: Train model
    print("\n2. Training model...")
    subprocess.run(["python", "src/train_model.py"])
    
    # Step 3: Test prediction
    print("\n3. Testing predictions...")
    subprocess.run(["python", "src/predict.py"])
    
    # Step 4: Verify files exist
    print("\n4. Verifying project structure...")
    required_files = [
        "data/raw/sample_patients.json",
        "models/stroke_model/metadata",
        "requirements.txt"
    ]
    
    for file in required_files:
        if os.path.exists(file):
            print(f"✓ {file}")
        else:
            print(f"✗ {file} (missing)")
    
    print("\n" + "="*60)
    print("Project test complete!")
    print("="*60)

if __name__ == "__main__":
    test_complete_flow()
