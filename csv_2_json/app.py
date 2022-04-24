import csv
import math
import uuid
import json
import boto3

s3 = boto3.client('s3')

min_zoom_level = 5
max_zoom_level = 15
base_tile_size = 256


def lambda_handler(event, context):
    global bucket, path, zipdata
    event = next(iter(event['Records']))
    bucket = event['s3']['bucket']['name']
    key = event['s3']['object']['key']
    tmpkey = key.replace('/', '')
    tmp_csv_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
    print(tmp_csv_path)
    s3.download_file(bucket, key, tmp_csv_path)

    mesh_csv = {}
    with open(tmp_csv_path, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for line in reader:
            if line:
                mesh_data = line[0].split(',')
                primary_mesh = mesh_data[0][:2]
                if primary_mesh in mesh_csv:
                    mesh_csv[primary_mesh
                             ]['full_meshcode'][mesh_data[0]] = mesh_data[1]
                else:
                    mesh_csv[primary_mesh] = {
                        'primary_mesh': primary_mesh,
                        'full_meshcode': {mesh_data[0]: mesh_data[1]}
                    }

                    pixel_dot_size_list = {}
                    constant_mesh = [primary_mesh + '454205',
                                     primary_mesh + '454206', primary_mesh + '454215']

                    for zoom_level in range(min_zoom_level, max_zoom_level + 1):

                        mesh_image_width = 1024 * pow(2, zoom_level - 2)
                        mesh_image_height = 1024 * pow(2, zoom_level - 2)
                        tmp_x_y_list = []
                        for tmp_mesh in constant_mesh:
                            mesh1 = tmp_mesh[:4]
                            mesh2 = tmp_mesh[4:6]
                            mesh3 = tmp_mesh[6:8]
                            lat = (int(mesh1[:2]) * 80 + int(mesh2[:1])
                                   * 10 + int(mesh3[:1])) * (30 / 3600)
                            lng = (int(mesh1[2:4]) * 80 + int(mesh2[1:2])
                                   * 10 + int(mesh3[1:2])) * (45 / 3600) + 100
                            coor = latLonToOffsets(
                                lat, lng, mesh_image_width, mesh_image_height)
                            tmp_x_y_list.append(coor)
                        width = tmp_x_y_list[1][0] - tmp_x_y_list[0][0]
                        height = tmp_x_y_list[0][1] - tmp_x_y_list[2][1]
                        pixel_dot_size_list[zoom_level] = (
                            0.5 if width <= 0 else width, 0.5 if height <= 0 else height)
                    mesh_csv[primary_mesh]['pixel_dot_size'] = pixel_dot_size_list

    print('finished csv read')
    level_data = {}
    for group_name in mesh_csv:
        for meshcode in mesh_csv[group_name]['full_meshcode']:
            for zoom_level in range(min_zoom_level, max_zoom_level + 1):
                mesh_image_width = 1024 * pow(2, zoom_level - 2)
                mesh_image_height = 1024 * pow(2, zoom_level - 2)
                mesh1 = meshcode[:4]
                mesh2 = meshcode[4:6]
                mesh3 = meshcode[6:8]
                lat = (int(mesh1[:2]) * 80 + int(mesh2[:1])
                       * 10 + int(mesh3[:1])) * (30 / 3600)
                lng = (int(mesh1[2:4]) * 80 + int(mesh2[1:2])
                       * 10 + int(mesh3[1:2])) * (45 / 3600) + 100
                coor = latLonToOffsets(
                    lat, lng, mesh_image_width, mesh_image_height)
                x = coor[0]
                y = coor[1] - \
                    mesh_csv[group_name]['pixel_dot_size'][zoom_level][1]

                tmp_tile_x = int(x / base_tile_size)
                tmp_tile_y = int(y / base_tile_size)

                tmp_point = {
                    'x': x - (base_tile_size * tmp_tile_x),
                    'y': y - (base_tile_size * tmp_tile_y),
                    'x_end': (x - (base_tile_size * tmp_tile_x)) + mesh_csv[group_name]['pixel_dot_size'][zoom_level][0],
                    'y_end': (y - (base_tile_size * tmp_tile_y)) + mesh_csv[group_name]['pixel_dot_size'][zoom_level][1],
                    'color': mesh_csv[group_name]['full_meshcode'][meshcode]
                }

                totalWidth = mesh_csv[group_name]['pixel_dot_size'][zoom_level][0]
                totalHeight = mesh_csv[group_name]['pixel_dot_size'][zoom_level][1]

                max_x_tile_no = math.ceil(
                    (tmp_point['x'] + totalWidth) / base_tile_size)
                max_y_tile_no = math.ceil(
                    (tmp_point['y'] + totalHeight) / base_tile_size)
                if max_x_tile_no == 1 and max_y_tile_no == 1:
                    level_data = add_points_by_zoom_level(
                        level_data, zoom_level, tmp_tile_x, tmp_tile_y, tmp_point)
                elif max_x_tile_no == 2 and max_y_tile_no == 1:
                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x,
                        tmp_tile_y,
                        {
                            'x': tmp_point['x'],
                            'y': tmp_point['y'],
                            'x_end': base_tile_size,
                            'y_end': tmp_point['y_end'],
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x + 1,
                        tmp_tile_y,
                        {
                            'x': 0,
                            'y': tmp_point['y'],
                            'x_end': tmp_point['x_end'] - base_tile_size,
                            'y_end': tmp_point['y_end'],
                            'color': tmp_point['color']
                        }
                    )
                elif max_x_tile_no == 3 and max_y_tile_no == 1:
                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x,
                        tmp_tile_y,
                        {
                            'x': tmp_point['x'],
                            'y': tmp_point['y'],
                            'x_end': base_tile_size,
                            'y_end': tmp_point['y_end'],
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x + 1,
                        tmp_tile_y,
                        {
                            'x': 0,
                            'y': tmp_point['y'],
                            'x_end': base_tile_size,
                            'y_end': tmp_point['y_end'],
                            'color': tmp_point['color']
                        }
                    )
                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x + 2,
                        tmp_tile_y,
                        {
                            'x': 0,
                            'y': tmp_point['y'],
                            'x_end': tmp_point['x_end'] - (base_tile_size * 2),
                            'y_end': tmp_point['y_end'],
                            'color': tmp_point['color']
                        }
                    )
                elif max_x_tile_no == 1 and max_y_tile_no == 2:
                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x,
                        tmp_tile_y,
                        {
                            'x': tmp_point['x'],
                            'y': tmp_point['y'],
                            'x_end': tmp_point['x_end'],
                            'y_end': base_tile_size,
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x,
                        tmp_tile_y + 1,
                        {
                            'x': tmp_point['x'],
                            'y': 0,
                            'x_end': tmp_point['x_end'],
                            'y_end': tmp_point['y_end'] - base_tile_size,
                            'color': tmp_point['color']
                        }
                    )
                elif max_x_tile_no == 1 and max_y_tile_no == 3:
                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x,
                        tmp_tile_y,
                        {
                            'x': tmp_point['x'],
                            'y': tmp_point['y'],
                            'x_end': tmp_point['x_end'],
                            'y_end': base_tile_size,
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x,
                        tmp_tile_y + 1,
                        {
                            'x': tmp_point['x'],
                            'y': 0,
                            'x_end': tmp_point['x_end'],
                            'y_end': base_tile_size,
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x,
                        tmp_tile_y + 2,
                        {
                            'x': tmp_point['x'],
                            'y': 0,
                            'x_end': tmp_point['x_end'],
                            'y_end': tmp_point['y_end'] - (base_tile_size * 2),
                            'color': tmp_point['color']
                        }
                    )
                elif max_x_tile_no == 2 and max_y_tile_no == 2:
                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x,
                        tmp_tile_y,
                        {
                            'x': tmp_point['x'],
                            'y': tmp_point['y'],
                            'x_end': base_tile_size,
                            'y_end': base_tile_size,
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x + 1,
                        tmp_tile_y,
                        {
                            'x': 0,
                            'y': tmp_point['y'],
                            'x_end': tmp_point['x_end'] - base_tile_size,
                            'y_end': base_tile_size,
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x,
                        tmp_tile_y + 1,
                        {
                            'x': tmp_point['x'],
                            'y': 0,
                            'x_end': base_tile_size,
                            'y_end': tmp_point['y_end'] - base_tile_size,
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x + 1,
                        tmp_tile_y + 1,
                        {
                            'x': 0,
                            'y': 0,
                            'x_end': tmp_point['x_end'] - base_tile_size,
                            'y_end': tmp_point['y_end'] - base_tile_size,
                            'color': tmp_point['color']
                        }
                    )
                elif max_x_tile_no == 3 and max_y_tile_no == 2:
                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x,
                        tmp_tile_y,
                        {
                            'x': tmp_point['x'],
                            'y': tmp_point['y'],
                            'x_end': base_tile_size,
                            'y_end': base_tile_size,
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x + 1,
                        tmp_tile_y,
                        {
                            'x': 0,
                            'y': tmp_point['y'],
                            'x_end': base_tile_size,
                            'y_end': base_tile_size,
                            'color': tmp_point['color']
                        }
                    )
                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x + 2,
                        tmp_tile_y,
                        {
                            'x': 0,
                            'y': tmp_point['y'],
                            'x_end': tmp_point['x_end'] - (base_tile_size * 2),
                            'y_end': base_tile_size,
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x,
                        tmp_tile_y + 1,
                        {
                            'x': tmp_point['x'],
                            'y': 0,
                            'x_end': base_tile_size,
                            'y_end': tmp_point['y_end'] - base_tile_size,
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x + 1,
                        tmp_tile_y + 1,
                        {
                            'x': 0,
                            'y': 0,
                            'x_end': base_tile_size,
                            'y_end': tmp_point['y_end'] - base_tile_size,
                            'color': tmp_point['color']
                        }
                    )
                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x + 2,
                        tmp_tile_y + 1,
                        {
                            'x': 0,
                            'y': 0,
                            'x_end': tmp_point['x_end'] - (base_tile_size * 2),
                            'y_end': tmp_point['y_end'] - (base_tile_size * 1),
                            'color': tmp_point['color']
                        }
                    )
                elif max_x_tile_no == 3 and max_y_tile_no == 3:
                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x,
                        tmp_tile_y,
                        {
                            'x': tmp_point['x'],
                            'y': tmp_point['y'],
                            'x_end': base_tile_size,
                            'y_end': base_tile_size,
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x + 1,
                        tmp_tile_y,
                        {
                            'x': 0,
                            'y': tmp_point['y'],
                            'x_end': base_tile_size,
                            'y_end': base_tile_size,
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x + 2,
                        tmp_tile_y,
                        {
                            'x': 0,
                            'y': tmp_point['y'],
                            'x_end': tmp_point['x_end'] - (base_tile_size * 2),
                            'y_end': base_tile_size,
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x,
                        tmp_tile_y + 1,
                        {
                            'x': tmp_point['x'],
                            'y': 0,
                            'x_end': base_tile_size,
                            'y_end': base_tile_size,
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x + 1,
                        tmp_tile_y + 1,
                        {
                            'x': 0,
                            'y': 0,
                            'x_end': base_tile_size,
                            'y_end': base_tile_size,
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x + 2,
                        tmp_tile_y + 1,
                        {
                            'x': 0,
                            'y': 0,
                            'x_end': tmp_point['x_end'] - (base_tile_size * 2),
                            'y_end': base_tile_size,
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x,
                        tmp_tile_y + 2,
                        {
                            'x': tmp_point['x'],
                            'y': 0,
                            'x_end': base_tile_size,
                            'y_end': tmp_point['y_end'] - (base_tile_size * 2),
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x + 1,
                        tmp_tile_y + 2,
                        {
                            'x': 0,
                            'y': 0,
                            'x_end': base_tile_size,
                            'y_end': tmp_point['y_end'] - (base_tile_size * 2),
                            'color': tmp_point['color']
                        }
                    )

                    level_data = add_points_by_zoom_level(
                        level_data,
                        zoom_level,
                        tmp_tile_x + 2,
                        tmp_tile_y + 2,
                        {
                            'x': 0,
                            'y': 0,
                            'x_end': tmp_point['x_end'] - (base_tile_size * 2),
                            'y_end': tmp_point['y_end'] - (base_tile_size * 2),
                            'color': tmp_point['color']
                        }
                    )

    print('finished level_data')

    image_data = []
    for zoom_level in range(min_zoom_level, max_zoom_level + 1):
        for tile_x in level_data[zoom_level]:
            for tile_y in level_data[zoom_level][tile_x]:
                image_data.append({
                    'zoom_level': zoom_level,
                    'tile_x': tile_x,
                    'tile_y': tile_y,
                    'points': level_data[zoom_level][tile_x][tile_y]
                })

    split_size = 10000
    image_data_splited = [image_data[x:x+split_size]
                          for x in range(0, len(image_data), split_size)]
    for index, image_list in enumerate(image_data_splited):
        tmp_path = '/tmp/test_{}.json'.format(index + 1)
        file = open(tmp_path, 'w')
        json.dump(image_list, file)
        file.close()
        s3.upload_file(
            tmp_path,
            bucket,
            'json/test_{}.json'.format(index + 1)
        )


def degrees_to_radians(degrees):
    return (degrees * math.pi) / 180


def latLonToOffsets(latitude, longitude, map_width, map_height):
    FE = 180
    radius = map_width / (2 * math.pi)

    lat_rad = degrees_to_radians(latitude)
    lon_rad = degrees_to_radians(longitude + FE)

    x = lon_rad * radius

    y_from_equator = radius * math.log(math.tan(math.pi / 4 + lat_rad / 2))
    y = map_height / 2 - y_from_equator

    return (x, y)


def add_points_by_zoom_level(level_data, zoom_level, tmp_tile_x, tmp_tile_y, tmp_point):
    if zoom_level in level_data:
        if tmp_tile_x in level_data[zoom_level]:
            if tmp_tile_y in level_data[zoom_level][tmp_tile_x]:
                level_data[zoom_level][tmp_tile_x][tmp_tile_y].append(
                    tmp_point)
            else:
                level_data[zoom_level][tmp_tile_x][tmp_tile_y] = [tmp_point]
        else:
            level_data[zoom_level][tmp_tile_x] = {tmp_tile_y: [tmp_point]}
    else:
        level_data[zoom_level] = {tmp_tile_x: {tmp_tile_y: [tmp_point]}}
    return level_data
