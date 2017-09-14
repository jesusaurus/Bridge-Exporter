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
        assertEquals(studyInfo.getDataAccessTeamId(), 23);
        assertEquals(studyInfo.getSynapseProjectId(), "test-synapse-project");

        // optional args default to false
        assertFalse(studyInfo.getDisableExport());
        assertFalse(studyInfo.isStudyIdExcludedInExport());
        assertFalse(studyInfo.getUsesCustomExportSchedule());
    }

    @Test
    public void disableExportTrue() {
        StudyInfo studyInfo = new StudyInfo.Builder().withDataAccessTeamId(23L)
                .withSynapseProjectId("test-synapse-project").withDisableExport(true).build();
        assertTrue(studyInfo.getDisableExport());
    }

    @Test
    public void studyIdExcludedInExport() {
        StudyInfo studyInfo = new StudyInfo.Builder().withDataAccessTeamId(23L)
                .withSynapseProjectId("test-synapse-project").withStudyIdExcludedInExport(true).build();
        assertTrue(studyInfo.isStudyIdExcludedInExport());
    }

    @Test
    public void customExportTrue() {
        StudyInfo studyInfo = new StudyInfo.Builder().withDataAccessTeamId(23L)
                .withSynapseProjectId("test-synapse-project").withUsesCustomExportSchedule(true).build();
        assertTrue(studyInfo.getUsesCustomExportSchedule());
    }
}
