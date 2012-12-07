package com.force.vagabond.clustertest.path;

import com.fasterxml.clustermate.service.ServiceRequest;
import com.force.vagabond.clustertest.ClusterTestBase;
import com.force.vagabond.clustertest.util.FakeHttpRequest;

public class TestVagabondPaths extends ClusterTestBase
{
    public void testSimple()
    {
        ServiceRequest req = new FakeHttpRequest("/v/store/list");
        // with leading slash, should get one empty Segment first:
        assertEquals("", req.nextPathSegment());

        assertFalse(req.matchPathSegment("vee"));
        assertTrue(req.matchPathSegment("v"));

        assertFalse(req.matchPathSegment("v"));
        assertFalse(req.matchPathSegment("sto"));
        assertFalse(req.matchPathSegment("storey"));
        assertTrue(req.matchPathSegment("store"));

        assertTrue(req.matchPathSegment("list"));

        assertFalse(req.matchPathSegment(""));
        assertNull(req.nextPathSegment());
    }
}
