# !/usr/bin/env python2

import os
import re
import struct
import urllib2

import shapely


def get_sr_from_web(srid, auth, sr_format):
    """
    Get spatial reference data from spatialreference.org

    Parameters
    ----------
    srid: int
        Spatial Reference ID
    auth: str
        Name of authority {epsg, esri, sr-org}
    sr_format: str
        Desired format of spatial reference data
    """
    _formats = [
        'html',
        'prettywkt',
        'proj4',
        'json',
        'gml',
        'esriwkt',
        'mapfile',
        'mapserverpython',
        'mapnik',
        'mapnikpython',
        'geoserver',
        'postgis',
        'spatialite',  # Derivative of PostGIS
        'proj4js'
        ]

    _authorities = [
        "epsg",
        "esri",
        "sr-org"]

    # Validate inputs
    srid = int(srid)
    auth = auth.lower()
    sr_format = sr_format.lower()
    if auth not in _authorities:
        raise ValueError("{} is not a valid authority".format(auth))
    if sr_format not in _formats:
        raise ValueError("{} is not a valid format".format(sr_format))
    site = "https://spatialreference.org/ref/{0}/{1}/{2}/".format(
        auth, srid, sr_format)

    # SpatiaLite (derive from PostGIS)
    if sr_format == "spatialite":
        site = site.replace("spatialite", "postgis")
        data = urllib2.urlopen(site).read()
        # The srid value has a leading 9 in the PostGIS INSERT statement
        data = re.sub("9{}".format(srid), str(srid), data, count=1)
    else:
        data = urllib2.urlopen(site).read()
    return data


# TODO: Errors on geometries with Z and/or M values
class SpatiaLiteBlobElement(object):
    """
    SpatiaLite Blob Element
    """
    def __init__(self, geom_buffer):
        """
        Decodes a SpatiaLite BLOB geometry into a Spatial Reference and
        Well-Known Binary representation
        See specification: https://www.gaia-gis.it/gaia-sins/BLOB-Geometry.html

        Parameters
        ----------
        geom_buffer: buffer
            The geometry type native to SpatiaLite (BLOB geometry)
        """
        self.blob = geom_buffer
        # Get as bytearray
        array = bytearray(self.blob)
        # List of Big- or Little-Endian identifiers
        endian = [">", "<"][array[1]]

        # Decode the Spatial Reference ID
        self.srid = "{}".format(struct.unpack(endian + 'i', array[2:6])[0])

        # Create WKB from Endian (pos 1) and SpatiaLite-embeded WKB data
        # at pos 39+
        self.wkb = str(geom_buffer[1] + array[39:-1])

    @property
    def as_shapely(self):
        """Return SpatiaLite BLOB as shapely object."""
        return shapely.wkb.loads(self.wkb)

    @property
    def as_wkt(self):
        """Return SpatiaLite BLOB as Well Known Text."""
        return shapely.wkt.dumps(self.as_shapely)

    @property
    def as_ewkt(self):
        """Return SpatiaLite BLOB as Extended Well-Known Text."""
        return "SRID={};{}".format(self.srid, self.as_wkt)

    def __str__(self):
        return self.ewkt
