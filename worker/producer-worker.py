import pika
import sys
from IPython import embed
import json
import io
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS
from openalpr import Alpr
import hashlib
import redis

connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq.us-west1-b.c.assignments-252803.internal'))
channel = connection.channel()

channel.exchange_declare(exchange='toWorker', exchange_type='direct')

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='toWorker', queue=queue_name, routing_key='img_data')

def get_exif_data(image):
    """Returns a dictionary from the exif data of an PIL Image item. Also converts the GPS Tags"""
    exif_data = {}
    info = image._getexif()
    if info:
        for tag, value in info.items():
            decoded = TAGS.get(tag, tag)
            if decoded == "GPSInfo":
                gps_data = {}
                for gps_tag in value:
                    sub_decoded = GPSTAGS.get(gps_tag, gps_tag)
                    gps_data[sub_decoded] = value[gps_tag]
 
                exif_data[decoded] = gps_data
            else:
                exif_data[decoded] = value
 
    return exif_data


def _convert_to_degress(value):
    """Helper function to convert the GPS coordinates stored in the EXIF to degress in float format"""
    deg_num, deg_denom = value[0]
    d = float(deg_num) / float(deg_denom)
 
    min_num, min_denom = value[1]
    m = float(min_num) / float(min_denom)
 
    sec_num, sec_denom = value[2]
    s = float(sec_num) / float(sec_denom)
    
    return d + (m / 60.0) + (s / 3600.0)


def get_lat_lon(exif_data, debug=False):
    """Returns the latitude and longitude, if available, from the provided exif_data (obtained through get_exif_data above)"""
    lat = None
    lon = None
 
    if "GPSInfo" in exif_data:
        gps_info = exif_data["GPSInfo"]
 
        gps_latitude = gps_info.get("GPSLatitude")
        gps_latitude_ref = gps_info.get('GPSLatitudeRef')
        gps_longitude = gps_info.get('GPSLongitude')
        gps_longitude_ref = gps_info.get('GPSLongitudeRef')
 
        if gps_latitude and gps_latitude_ref and gps_longitude and gps_longitude_ref:
            lat = _convert_to_degress(gps_latitude)
            if gps_latitude_ref != "N":                     
                lat *= -1
 
            lon = _convert_to_degress(gps_longitude)
            if gps_longitude_ref != "E":
                lon *= -1
    else:
        if debug:
            print("No EXIF data")
 
    return lat, lon

 
def callback(ch, method, properties, body):
    ioBuffer = io.BytesIO(body)
    img = Image.open(ioBuffer)
    exif_data = get_exif_data(img)
    lat_lon = list(get_lat_lon(exif_data))
    if lat_lon[0] is not None and lat_lon[1] is not None:
        alpr = Alpr('us', '/etc/openalpr/openalpr.conf', '/usr/share/openalpr/runtime_data/')
        f = open("temp_file.jpg", "wb")
        f.write(body)
        f.close()
        results = alpr.recognize_file("temp_file.jpg")
        if len(results['results']) > 0:
            computed_md5 = hashlib.md5(body).hexdigest()
            curr_val = str(lat_lon[0])+":"+str(lat_lon[1])+":"+str(results['results'][0]['plate'])+":"+str(results['results'][0]['confidence'])
            r1 = redis.Redis(host='redis.us-west1-b.c.assignments-252803.internal', port=6379, db=1)
            r1.set(computed_md5,curr_val)
            r3 = redis.Redis(host='redis.us-west1-b.c.assignments-252803.internal', port=6379, db=3)
            r3.set(results['results'][0]['plate'],computed_md5)
            print(curr_val)
            print(computed_md5)
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue=queue_name, on_message_callback=callback)

channel.start_consuming()
