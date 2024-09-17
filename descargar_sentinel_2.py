import boto3
from funciones_sentinel_2 import * 

# Configuring boto3 session
s3 = boto3.resource(
    's3',
    endpoint_url='https://eodata.dataspace.copernicus.eu',
    aws_access_key_id="access_key",
    aws_secret_access_key="secret_key",
    region_name='default')

fecha_inicial = "2024-04-30T00:00:00.000Z"
fecha_final = "2024-05-01T00:00:00.000Z"
poligono = "-66.971000 21.651000, -66.911000 20.761000,-66.043000 20.822000, -66.043000 21.611000, -86.971000 21.651000"
# primero longitud y luego latitud
satelite = "SENTINEL-2"
producto = "S2MSI1C"

df = get_copernicus_image_metadata(
    start_date=fecha_inicial,
    end_date=fecha_final,
    polygon=poligono,
    collection=satelite,
    product_type=producto)

print(df.head(5))

download_copernicus_images(s3, df, start_row=1, target_directory="imagenes_descargadas")
