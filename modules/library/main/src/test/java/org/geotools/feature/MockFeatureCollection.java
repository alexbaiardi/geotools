/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2003-2008, Open Source Geospatial Foundation (OSGeo)
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
 *
 *    Created on August 12, 2003, 7:29 PM
 */
package org.geotools.feature;

import java.util.Collection;
import java.util.Iterator;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.sort.SortBy;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.geometry.jts.ReferencedEnvelope;

/** @author jamesm */
public class MockFeatureCollection implements SimpleFeatureCollection {

    /** Creates a new instance of MockFeatureCollection */
    public MockFeatureCollection() {}

    @Override
    public void accepts(
            org.geotools.api.feature.FeatureVisitor visitor,
            org.geotools.api.util.ProgressListener progress) {}

    public void addListener(CollectionListener listener) throws NullPointerException {}

    public void close(FeatureIterator<SimpleFeature> close) {}

    public void close(Iterator close) {}

    @Override
    public SimpleFeatureIterator features() {
        return null;
    }

    @Override
    public SimpleFeatureType getSchema() {
        return null;
    }

    public void removeListener(CollectionListener listener) throws NullPointerException {}

    @Override
    public SimpleFeatureCollection sort(SortBy order) {
        return null;
    }

    @Override
    public SimpleFeatureCollection subCollection(Filter filter) {
        return null;
    }

    public Iterator iterator() {
        return null;
    }

    public void purge() {}

    public boolean add(SimpleFeature o) {
        return false;
    }

    public boolean addAll(Collection c) {
        return false;
    }

    public boolean addAll(
            FeatureCollection<? extends SimpleFeatureType, ? extends SimpleFeature> resource) {
        return false;
    }

    public void clear() {}

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public boolean containsAll(Collection c) {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    public boolean remove(Object o) {
        return false;
    }

    public boolean removeAll(Collection c) {
        return false;
    }

    public boolean retainAll(Collection c) {
        return false;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Object[] toArray() {
        return null;
    }

    @Override
    public <O> O[] toArray(O[] a) {
        return null;
    }

    @Override
    public ReferencedEnvelope getBounds() {
        return null;
    }

    @Override
    public String getID() {
        return null;
    }
}
