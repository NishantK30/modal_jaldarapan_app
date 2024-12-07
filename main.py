import modal
import os
from src import *
from pathlib import Path


app = modal.App("Jaldarpan_0_1_0",
                secrets=[modal.Secret.from_name("Jaldarpan-secrets")]
                )

volume = modal.Volume.from_name("jaldarpan-models",create_if_missing=True)
model_dir = Path("/models")

image = modal.Image.debian_slim()\
        .pip_install("-r requirements.txt")\
        .copy_local_dir("src","/app/src")
        
@app.function(schedule=modal.Period(days=3))
def biweekly_scheduler():
    opencage_api_key = os.environ["opencage_apikey"]
    postgres_conn = os.environ["postgres_conn"]
    weather_data,rain_array = fetch_weather_data("Mumbai",opencage_api_key)
    
@app.function(gpu="A100",gpu_count=1,volumes={model_dir:volume})
def train_model(config):
    model = "some function"
    model.save(config,model_dir)# to save model, we can also do batch upload using a different method
    #to load a model
    from keras.model import load_model
    model = load_model(model_dir/model_id)#how to get model id
    
    

@app.function(schedule=modal.Period(days=6),
              gpu="T4",
              gpu_count=1)
def weekly_inference_pipeline():
    pass

@app.local_entrypoint
def main():
    biweekly_scheduler.local()
    weekly_inference_pipeline.local()