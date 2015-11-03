package org.sagebionetworks.bridge.exporter.dynamo;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.testng.annotations.Test;

public class StudyInfoTest {
    @Test
    public void nullFields() {
        StudyInfo studyInfo = new StudyInfo.Builder().build();
        assertNull(studyInfo.getDataAccessTeamId());
        assertNull(studyInfo.getSynapseProjectId());
    }

    @Test
    public void withFields() {
        StudyInfo studyInfo = new StudyInfo.Builder().withDataAccessTeamId(23L)
                .withSynapseProjectId("test-synapse-project").build();
        assertEquals(studyInfo.getDataAccessTeamId().longValue(), 23);
        assertEquals(studyInfo.getSynapseProjectId(), "test-synapse-project");
    }
}
