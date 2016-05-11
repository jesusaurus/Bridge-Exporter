package org.sagebionetworks.bridge.exporter.dynamo;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class StudyInfoTest {
    @Test
    public void nullDataAccessTeam() {
        StudyInfo studyInfo = new StudyInfo.Builder().withSynapseProjectId("test-synapse-project").build();
        assertNull(studyInfo);
    }

    @Test
    public void nullProject() {
        StudyInfo studyInfo = new StudyInfo.Builder().withDataAccessTeamId(23L).build();
        assertNull(studyInfo);
    }

    @Test
    public void emptyProject() {
        StudyInfo studyInfo = new StudyInfo.Builder().withDataAccessTeamId(23L).withSynapseProjectId("").build();
        assertNull(studyInfo);
    }

    @Test
    public void blankProject() {
        StudyInfo studyInfo = new StudyInfo.Builder().withDataAccessTeamId(23L).withSynapseProjectId("   ").build();
        assertNull(studyInfo);
    }

    @Test
    public void withFields() {
        StudyInfo studyInfo = new StudyInfo.Builder().withDataAccessTeamId(23L)
                .withSynapseProjectId("test-synapse-project").build();
        assertEquals(studyInfo.getDataAccessTeamId().longValue(), 23);
        assertEquals(studyInfo.getSynapseProjectId(), "test-synapse-project");
        assertFalse(studyInfo.getUsesCustomExportSchedule());
    }

    @Test
    public void customExportFalse() {
        StudyInfo studyInfo = new StudyInfo.Builder().withDataAccessTeamId(23L)
                .withSynapseProjectId("test-synapse-project").withUsesCustomExportSchedule(false).build();
        assertFalse(studyInfo.getUsesCustomExportSchedule());
    }

    @Test
    public void customExportTrue() {
        StudyInfo studyInfo = new StudyInfo.Builder().withDataAccessTeamId(23L)
                .withSynapseProjectId("test-synapse-project").withUsesCustomExportSchedule(true).build();
        assertTrue(studyInfo.getUsesCustomExportSchedule());
    }
}
