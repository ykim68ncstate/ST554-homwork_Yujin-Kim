# ST 554 Final Project

## Overview
This project builds a machine learning and streaming pipeline using PySpark to predict power consumption.

## Model
- Data is loaded with pandas and converted to Spark
- Features are processed using:
  - Hour binarization
  - Month one-hot encoding
  - PCA
- An Elastic Net model is trained with 5-fold cross-validation

## Streaming
- A streaming DataFrame reads CSV files from a folder
- The trained model generates predictions and residuals in real time
- Results are written to the console

## Data Simulation
- `produce_streaming_data.py` generates streaming data
- Random samples are written as CSV files every 10 seconds

## How to Run
1. Run the notebook to start the streaming query  
2. Run:
   ```bash
   python produce_streaming_data.py
   ```

## Project Structure

```text
Final_Project/
├── final_project.ipynb
├── power_streaming_data.csv
├── produce_streaming_data.py
└── stream_folder/
