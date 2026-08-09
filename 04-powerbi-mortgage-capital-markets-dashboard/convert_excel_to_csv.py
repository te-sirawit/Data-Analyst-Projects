import pandas as pd
import os
import glob

# Paths
source_dir = '../Datasets'  # Update this to the path where your source Excel files are located
dest_dir = 'raw_data'

# Ensure destination exists
os.makedirs(dest_dir, exist_ok=True)

# Find all Excel files
excel_files = glob.glob(os.path.join(source_dir, '*.xlsx'))

print(f"Found {len(excel_files)} Excel files. Converting to CSV...")

for file in excel_files:
    # Get filename without extension
    base_name = os.path.splitext(os.path.basename(file))[0]
    csv_path = os.path.join(dest_dir, f"{base_name}.csv")
    
    try:
        print(f"Converting {base_name}...")
        df = pd.read_excel(file)
        
        # Clean column names (lowercase, replace spaces with underscores)
        df.columns = [c.strip().lower().replace(' ', '_').replace('-', '_') for c in df.columns]
        
        # Save to CSV
        df.to_csv(csv_path, index=False)
        print(f"  -> Saved to {csv_path}")
    except Exception as e:
        print(f"  -> Error converting {base_name}: {e}")

print("All conversions finished.")
