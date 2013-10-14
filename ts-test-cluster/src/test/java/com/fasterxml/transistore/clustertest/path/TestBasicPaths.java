package com.fasterxml.transistore.clustertest.path;

import com.fasterxml.clustermate.service.ServiceRequest;
import com.fasterxml.transistore.clustertest.ClusterTestBase0;
import com.fasterxml.transistore.clustertest.util.FakeHttpRequest;

public class TestBasicPaths extends ClusterTestBase0
{
    public void testSimple()
    {
        ServiceRequest req = new FakeHttpRequest("/ts/store/list");
        // with leading slash, should get one empty Segment first:
        assertEquals("", req.nextPathSegment());

        assertFalse(req.matchPathSegment("tse"));
        assertTrue(req.matchPathSegment("ts"));

        assertFalse(req.matchPathSegment("ts"));
        assertFalse(req.matchPathSegment("sto"));
        assertFalse(req.matchPathSegment("storey"));
        assertTrue(req.matchPathSegment("store"));

        assertTrue(req.matchPathSegment("list"));

        assertFalse(req.matchPathSegment(""));
        assertNull(req.nextPathSegment());
    }
}
