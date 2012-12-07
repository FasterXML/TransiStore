package com.fasterxml.transistore.dw.util;

import java.util.*;

import javax.ws.rs.core.*;

public class HttpHeadersImpl implements HttpHeaders
{
    protected final MultivaluedMapImpl<String,String> _headers = new MultivaluedMapImpl<String,String>();

    public MediaType mediaType = null;
    public Locale language = null;

    public HttpHeadersImpl addHeader(String key, String value) {
        _headers.add(key, value);
        return this;
    }

    @Override
    public String toString() {
        return "HTTP-headers: "+_headers;
    }
    
    @Override
    public List<String> getRequestHeader(String name) {
        return _headers.get(name);
    }

    @Override
    public MultivaluedMap<String, String> getRequestHeaders() {
        return _headers;
    }

    @Override
    public List<MediaType> getAcceptableMediaTypes() {
        return null;
    }

    @Override
    public List<Locale> getAcceptableLanguages() {
        return null;
    }

    @Override
    public MediaType getMediaType() {
        return mediaType;
    }

    @Override
    public Locale getLanguage() {
        return language;
    }

    @Override
    public Map<String, Cookie> getCookies() {
        return Collections.emptyMap();
    }

    @SuppressWarnings("serial")
    protected final static class MultivaluedMapImpl<K,V>
        extends LinkedHashMap<K,List<V>>
        implements MultivaluedMap<K,V>
    {
        @Override
        public void putSingle(K key, V value) {
            ArrayList<V> vals = new ArrayList<V>(1);
            vals.add(value);
            put(key, vals);
        }

        @Override
        public void add(K key, V value) {
            List<V> vals = get(key);
            if (vals == null) {
                vals = new ArrayList<V>();
                put(key,  vals);
            }
            vals.add(value);
        }

        @Override
        public V getFirst(K key) {
            List<V> vals = get(key);
            if (vals == null || vals.isEmpty()) {
                return null;
            }
            return vals.get(0);
        }
        
    }
}

