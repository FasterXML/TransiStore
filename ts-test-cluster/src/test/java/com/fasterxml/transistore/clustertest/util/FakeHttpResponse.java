package com.fasterxml.transistore.clustertest.util;

import java.util.*;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.service.ServiceResponse;
import com.fasterxml.clustermate.service.msg.FileBackedResponseContentImpl;

@SuppressWarnings("unchecked")
public class FakeHttpResponse extends ServiceResponse
{
    protected int statusCode = 200;
    
    protected Map<String,String> _headers;

    /*
    /**********************************************************************
    /* Basic implementation
    /**********************************************************************
     */

    @Override
    public long getBytesWritten() { return -1L; }

    @Override
    public int getStatus() {
        return statusCode;
    }
    
    @Override
    public FakeHttpResponse set(int code, Object e) {
        statusCode = code;
        return setEntity(e);
    }

    @Override
    public FakeHttpResponse setStatus(int code) {
        statusCode = code;
        return this;
    }

    @Override
    public FakeHttpResponse addHeader(String key, String value) {
        if (_headers == null) {
            _headers = new HashMap<String, String>();
        }
        _headers.put(key, value);
        return this;
    }

    @Override
    public FakeHttpResponse addHeader(String key, int value) {
        return addHeader(key, String.valueOf(value));
    }

    @Override
    public FakeHttpResponse addHeader(String key, long value) {
        return addHeader(key, String.valueOf(value));
    }
    
    @Override
    public FakeHttpResponse setContentType(String contentType) {
        return addHeader(ClusterMateConstants.HTTP_HEADER_CONTENT_TYPE, contentType);
    }

    @Override
    public FakeHttpResponse setContentLength(long length) {
		return addHeader(ClusterMateConstants.HTTP_HEADER_CONTENT_LENGTH, length);
    }
    
    /*
    /**********************************************************************
    /* Extended API
    /**********************************************************************
     */
    
    public boolean hasFile() {
        return (_streamingContent != null) && (((FileBackedResponseContentImpl) _streamingContent).hasFile());
    }
    public boolean hasInlinedData() {
        return (_streamingContent != null) && (((FileBackedResponseContentImpl) _streamingContent).inline());
    }
    
    public FileBackedResponseContentImpl getStreamingContent() {
        return (FileBackedResponseContentImpl) _streamingContent;
    }
    
    public String getHeader(String key) {
        return (_headers == null) ? null : _headers.get(key);
    }
}
