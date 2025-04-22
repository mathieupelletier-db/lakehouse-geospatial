__all__ = [
    "geojson_to_esri_json",
    "convert_geometry"
]

import json

def geojson_to_esri_json(geojson):
    """Convert GeoJSON to ESRI JSON format compatible with ArcGIS."""
    esri_json = {}
    esri_json['spatialReference'] = {'wkid': 4326}  # WGS84

    geojson_type = geojson.get('type')

    if geojson_type == 'FeatureCollection':
        features = geojson.get('features', [])
        esri_json['features'] = []
        for feature in features:
            esri_feature = {}
            esri_feature['attributes'] = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            esri_feature['geometry'] = convert_geometry(geometry)
            esri_json['features'].append(esri_feature)
    elif geojson_type == 'Feature':
        esri_json['attributes'] = geojson.get('properties', {})
        esri_json['geometry'] = convert_geometry(geojson.get('geometry', {}))
    else:
        esri_json['geometry'] = convert_geometry(geojson)

    return esri_json

def convert_geometry(geometry):
    """Convert GeoJSON geometry to ESRI JSON geometry."""
    geom_type = geometry.get('type')
    coords = geometry.get('coordinates')

    if geom_type == 'Point':
        return {'x': coords[0], 'y': coords[1]}
    elif geom_type == 'MultiPoint':
        return {'points': coords}
    elif geom_type == 'LineString':
        return {'paths': [coords]}
    elif geom_type == 'MultiLineString':
        return {'paths': coords}
    elif geom_type == 'Polygon':
        return {'rings': coords}
    elif geom_type == 'MultiPolygon':
        rings = []
        for polygon in coords:
            rings.extend(polygon)
        return {'rings': rings}
    else:
        return {}

# Example usage
"""
with open('input.geojson', 'r', encoding='utf-8') as f:
    geojson_data = json.load(f)

esri_json_result = geojson_to_esri_json(geojson_data)

with open('output_esri.json', 'w', encoding='utf-8') as f:
    json.dump(esri_json_result, f, indent=2)
"""
