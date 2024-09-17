import requests
import pandas as pd
import os
import re

def remove_prefix(string, prefix="/eodata/"):
    '''
    Removes a specific prefix from a string if it is present.
    
    Parameters:
    string (str): The string from which the prefix should be removed.
    prefix (str): The prefix to remove.
    
    Returns:
    str: The string without the prefix if it was present; otherwise,
    returns the original string unchanged.
    '''
    
    if string.startswith(prefix):
        string = string[len(prefix):]
    return string

def extract_imagename(path, pattern=r"(S2[AB]_MSI.{3}_\d+T\d+_N\d+_R\d+_T[a-zA-Z0-9]+)"):
    '''
    Extracts an imagename from a given path using a regular expression.
    The pattern currently matches only
    Sentinel 2 images.
    
    Raises ValueError if no match is found for the pattern in the provided path.

    Parameters:
    path (str): The full path of the file.
    patter: (regex): Regex that matches the imagename contained in the file path.

    Returns:
    str: The part of the path that matches the pattern.
   '''
    
    # Use the regular expression to search for the match
    match = re.search(pattern, path)

    if match:
        imagename = match.group(1)
        return imagename
    else:
        raise ValueError("No match found for the pattern in the provided path.")

def download(bucket, product, target=""):
    '''
    Downloads every file in bucket with provided product as prefix

    Raises FileNotFoundError if no file was found

    Parameters:
    bucket (object): boto3 Resource bucket object
    product (str): Path to product
    target (str): Local catalog for downloaded files. Default current directory.  
    '''
    
    files = bucket.objects.filter(Prefix=product)
    if not list(files):
        raise FileNotFoundError(f"No se encontraron archivos para {product}")
    
    for file in files:
        
        # Creating target directory
        imagename = extract_imagename(file.key)
        imagepath = os.path.join(target, imagename)
        os.makedirs(imagepath, exist_ok=True)
        
        # Downloading file:
        filename = os.path.basename(file.key)
        bucket.download_file(file.key, os.path.join(imagepath, filename))

def get_copernicus_image_metadata(start_date, end_date, polygon,
    collection, cloud_cover=None, product_type=None, srid="4326"): 
    '''
    Creates a Pandas DataFrame containing the metadata for Copernicus images
    intersecting a given polygon within a specified date range.
    This data frame can then be used to download the corresponding images.

    Parameters:
    start_date (str): The start date for the query (format: "2024-04-30T00:00:00.000Z").
    end_date (str): The end date for the query (format: "2024-04-30T00:00:00.000Z").
    polygon (str): The polygon coordinates in "longitude latitude" format, comma-separated.
    collection (str): The name of the collection (e.g., "Sentinel-1").
    cloud_cover (2 digit int): Maximum allowable cloud cover percentage (optional).
    product_type (str): Desired product_type (optional). Example: "S2MSI2A"
    srid (str): The Spatial Reference System Identifier (default is "4326" and
    cannot be changed at the moment).
    
    Returns:
    Pandas DataFrame: df containing the metadata of the found images.
    '''
    
    # Constructing the base query URL
    query_url = (
        "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
        "?$filter="
        f"OData.CSC.Intersects(area=geography'SRID={srid};POLYGON(({polygon}))')"
        f" and ContentDate/Start gt {start_date}"
        f" and ContentDate/Start lt {end_date}"
        f" and Collection/Name eq '{collection}'"
    )
    
    # Adding cloud cover filter if provided
    if cloud_cover is not None:
        query_url += f" and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value lt {cloud_cover})"
    
    # Adding product type filter if provided
    if product_type is not None:
        query_url += f" and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq '{product_type}')"
    
    # Print query for debugging
    # print(query_url)
    
    # Perform the query and get the JSON response
    json_response = requests.get(query_url).json()
    
    # Create a Pandas DataFrame from the JSON response
    df = pd.DataFrame.from_dict(json_response["value"])
    
    # Check if the DataFrame contains data
    if df.empty:
        print("No se encontraron imágenes para los criterios especificados.")
        
    return df

def download_copernicus_images(s3, df, start_row=0, target_directory=""):
    '''
    Downloads images from Copernicus based on the S3 paths in the DataFrame
    created using the get_copernicus_image_metadata() function.
    
    Parameters:
    s3 (boto3.resource): The boto3 S3 resource object
    df (Pandas DataFrame): The DataFrame created using the get_copernicus_image_metadata() function
    start_row (int): The row index from which to start downloading (default is 0).
    target_directory (str): The local directory where images will be downloaded.
    '''
    
    for idx, s3path in enumerate(df["S3Path"][start_row:], start=start_row):
        s3path = remove_prefix(s3path, "/eodata/")
        print(f"Descargando {s3path} (renglón {idx})...")
        try:
            download(s3.Bucket("eodata"), s3path, target=target_directory)
        except Exception as e:
            print(f"Error al descargar {s3path}: {e}")