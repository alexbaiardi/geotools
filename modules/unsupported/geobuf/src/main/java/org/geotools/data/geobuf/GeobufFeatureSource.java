/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2015, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package org.geotools.data.geobuf;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.geotools.api.data.DataStore;
import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.feature.FeatureVisitor;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;

public class GeobufFeatureSource extends ContentFeatureSource {

    private int precision = 6;

    private int dimension = 2;

    public GeobufFeatureSource(ContentEntry entry, Query query, int precision, int dimension) {
        super(entry, query);
        this.precision = precision;
        this.dimension = dimension;
    }

    @Override
    public GeobufDataStore getDataStore() {
        DataStore dataStore = super.getDataStore();
        if (dataStore instanceof GeobufDirectoryDataStore) {
            return ((GeobufDirectoryDataStore) dataStore).getDataStore(entry.getTypeName());
        } else {
            return (GeobufDataStore) dataStore;
        }
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
            throws IOException {
        return new GeobufFeatureReader(getState(), query, precision, dimension);
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        try (InputStream in = new FileInputStream(getDataStore().getFile())) {
            GeobufFeatureCollection geobufFeatureCollection = new GeobufFeatureCollection();
            return geobufFeatureCollection.getBounds(in);
        }
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        try (InputStream in = new FileInputStream(getDataStore().getFile())) {
            GeobufFeatureCollection geobufFeatureCollection = new GeobufFeatureCollection();
            return geobufFeatureCollection.countFeatures(in);
        }
    }

    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        return getDataStore().getFeatureType();
    }

    @Override
    protected boolean handleVisitor(Query query, FeatureVisitor visitor) throws IOException {
        return super.handleVisitor(query, visitor);
    }
}
