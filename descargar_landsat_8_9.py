from funciones_landsat_8_9 import *

# Para generar el token, conectarse a https://ers.cr.usgs.gov con las
# credenciales de USGS y seguir la interfaz gráfica
username = "user"
token = "token"

# Obteniendo el api_key necesario para hacer queries
api_key = get_api_key(username, token)
print(f"API Key obtenida: {api_key}\n")

# Ejemplo de búsqueda. Referirse a la documentación de la función
# get_landsat_image_metadata() para ver posibles valores.
fecha_inicial = "2024-04-30T00:00:00.000Z"
fecha_final = "2024-05-01T00:00:00.000Z"
poligono = "-66.971000 21.651000, -66.911000 20.761000, -66.043000 20.822000, -66.043000 21.611000, -86.971000 21.651000"
dataset  = "landsat_ot_c2_l1"
cloud_cover = 68
satellite = 8
sensor_id = None
processing_level = "L1TP"
tier = None

df = get_landsat_image_metadata(
    start_date=fecha_inicial,
    end_date=fecha_final,
    polygon=poligono,
    dataset=dataset,
    cloud_cover=cloud_cover,
    satellite=satellite,
    sensor_id=sensor_id,
    processing_level=processing_level,
    tier=tier,
    api_key=api_key)
print(df.head(5))

# Ejemplo de descarga. Por favor tomar en cuenta que descargar cada imagen
# completa cuesta aproximadamente 2 pesos mexicanos. El target directory se
# creará en la ruta de trabajo si no existe.
download_landsat_products(df, target_directory="landsat")
