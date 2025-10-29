from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import xarray as xr
import regionmask
import rioxarray as rxr
import requests
import os

def download_file(url, local_path):
    response = requests.get(url, stream=True)
    with open(local_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded: {os.path.basename(local_path)}")

def clip_ethiopia_to_tiff(input_file, output_file):
    ds = xr.open_dataset(input_file, engine='h5netcdf')
    var_name = list(ds.data_vars)[0]
    sm_data = ds[var_name].squeeze()
    
    countries = regionmask.defined_regions.natural_earth_v5_0_0.countries_110
    mask = countries.mask(sm_data)
    ethiopia_id = countries.map_keys(['Ethiopia'])[0]
    ethiopia_mask = mask == ethiopia_id
    
    sm_ethiopia = sm_data.where(ethiopia_mask, drop=True)
    sm_ethiopia = sm_ethiopia.rio.write_crs("EPSG:4326")
    sm_ethiopia.rio.to_raster(output_file)
    print(f"Clipped to TIFF: {os.path.basename(output_file)}")

def process_ethiopia_data(**context):
    params = context['params']
    year = params['year']
    data_type = params['type']
    month_selection = params['month']
    dag_id = context['dag'].dag_id
    
    # Create folder with DAG name
    output_folder = f"/opt/airflow/dags/{dag_id}"
    os.makedirs(output_folder, exist_ok=True)
    
    base_url = "https://gws-access.jasmin.ac.uk"
    
    if data_type == 'monthly':
        if month_selection == 'all':
            months = range(1, 13)
            print(f"Processing all months for {year}")
        elif '-' in str(month_selection):
            start_month, end_month = map(int, str(month_selection).split('-'))
            if start_month < 1 or end_month > 12 or start_month > end_month:
                raise ValueError(f"Invalid range: {month_selection}. Use format 'start-end' (e.g., '2-8')")
            months = range(start_month, end_month + 1)
            print(f"Processing months {start_month} to {end_month} for {year}")
        else:
            month_num = int(month_selection)
            if month_num < 1 or month_num > 12:
                raise ValueError(f"Invalid month: {month_num}. Must be 1-12, 'all', or range like '2-8'")
            months = [month_num]
            print(f"Processing {year}-{month_num:02d}")
        
        for month in months:
            url = f"{base_url}/public/tamsat/soil_moisture/data/v2.3.1/monthly/{year}/{month:02d}/sm{year}_{month:02d}.v2.3.1.nc"
            temp_file = f"temp_sm{year}_{month:02d}.nc"
            output_file = f"{output_folder}/ethiopia_sm{year}_{month:02d}.tiff"
            
            try:
                download_file(url, temp_file)
                clip_ethiopia_to_tiff(temp_file, output_file)
                os.remove(temp_file)
                print(f"✅ Completed: {year}-{month:02d}")
            except Exception as e:
                print(f"❌ Failed {year}-{month:02d}: {str(e)}")
                continue
    
    print(f"Ethiopia TIFF data saved in folder: {dag_id}")

default_args = {
    'owner': 'ethiopia_project',
    'start_date': datetime(2024, 10, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'ethiopia_flexible_download',
    default_args=default_args,
    description='Flexible Ethiopia soil moisture data download with user parameters',
    schedule_interval=None,
    catchup=False,
    params={
        "year": 2024,
        "type": "monthly", 
        "month": "all"
    }
)

download_task = PythonOperator(
    task_id='download_ethiopia_data',
    python_callable=process_ethiopia_data,
    dag=dag
)