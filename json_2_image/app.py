import json
import uuid
import boto3
from PIL import Image, ImageDraw

s3 = boto3.client('s3')

base_tile_size = 256

class_colors = {
    '1': (255, 0, 0, 100),
    '2': (255, 187, 61, 100),
    '3': (133, 43, 230, 100),
    '4': (100, 112, 255, 100)
}

tile_path = 'tile_images'


def lambda_handler(event, context):
    global bucket, path, zipdata
    event = next(iter(event['Records']))
    bucket = event['s3']['bucket']['name']
    key = event['s3']['object']['key']
    tmp_key = key.replace('/', '')
    tmp_json_path = '/tmp/{}{}'.format(uuid.uuid4(), tmp_key)
    print(tmp_json_path)
    s3.download_file(bucket, key, tmp_json_path)

    file = open(tmp_json_path)
    data = json.load(file)
    for image_info in data:
        img = Image.new('RGBA', (base_tile_size, base_tile_size))
        for dot in image_info['points']:
            shape = [(dot['x'], dot['y']), (dot['x_end'], dot['y_end'])]
            ImageDraw.Draw(img).rectangle(
                shape, fill=class_colors[dot['color']])
        tmp_path = '/tmp/{}_{}_{}.png'.format(
            image_info['zoom_level'], image_info['tile_x'], image_info['tile_y'])
        img.save(tmp_path)
        s3.upload_file(
            tmp_path,
            bucket,
            '{}/{}/{}/{}.png'.format(
                tile_path, image_info['zoom_level'], image_info['tile_x'], image_info['tile_y']),
            ExtraArgs={'ContentType': 'image/png'}
        )
    file.close()
    print('finished {}'.format(tmp_json_path))
