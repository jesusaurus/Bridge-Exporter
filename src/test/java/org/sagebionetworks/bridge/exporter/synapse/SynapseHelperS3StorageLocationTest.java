package org.sagebionetworks.bridge.exporter.synapse;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.mockito.ArgumentCaptor;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.sagebionetworks.repo.model.file.UploadDestinationLocation;
import org.sagebionetworks.repo.model.file.UploadType;
import org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting;
import org.sagebionetworks.repo.model.project.ProjectSettingsType;
import org.sagebionetworks.repo.model.project.StorageLocationSetting;
import org.sagebionetworks.repo.model.project.UploadDestinationListSetting;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
public class SynapseHelperS3StorageLocationTest {
    private static final String ATTACHMENTS_BUCKET = "my-attachments-bucket";
    private static final String PROJECT_ID = "syn1234";
    private static final long STORAGE_LOCATION_ID = 5678;

    private SynapseHelper helper;
    private SynapseClient mockClient;

    @BeforeMethod
    public void before() {
        helper = spy(new SynapseHelper());

        // Setup up mocks.
        mockClient = mock(SynapseClient.class);
        helper.setSynapseClient(mockClient);

        // Set up configs.
        helper.setAttachmentBucket(ATTACHMENTS_BUCKET);

        // Set rate limit to something super high so we don't bottleneck.
        helper.setRateLimit(1000);
    }

    @Test
    public void ensureS3StorageLocationInProject_CreateNewStorageLocation() throws Exception {
        // Spy getS3StorageLocationIdForProject() and createOrUpdateS3StorageLocationForProject(), so we don't have to
        // duplicate test logic.
        doReturn(null).when(helper).getS3StorageLocationIdForProject(PROJECT_ID);
        doReturn(STORAGE_LOCATION_ID).when(helper).createOrUpdateS3StorageLocationForProject(eq(PROJECT_ID), any());

        // Execute.
        long storageLocationId = helper.ensureS3StorageLocationInProject(PROJECT_ID);
        assertEquals(storageLocationId, STORAGE_LOCATION_ID);

        // Verify call to create storage location.
        ArgumentCaptor<ExternalS3StorageLocationSetting> locationSettingCaptor = ArgumentCaptor.forClass(
                ExternalS3StorageLocationSetting.class);
        verify(helper).createOrUpdateS3StorageLocationForProject(eq(PROJECT_ID), locationSettingCaptor.capture());
        ExternalS3StorageLocationSetting locationSetting = locationSettingCaptor.getValue();
        assertEquals(locationSetting.getBucket(), ATTACHMENTS_BUCKET);
        assertEquals(locationSetting.getUploadType(), UploadType.S3);
    }

    @Test
    public void ensureS3StorageLocationInProject_ExistingStorageLocation() throws Exception {
        // Spy getS3StorageLocationIdForProject() and createOrUpdateS3StorageLocationForProject(), so we don't have to
        // duplicate test logic.
        doReturn(STORAGE_LOCATION_ID).when(helper).getS3StorageLocationIdForProject(PROJECT_ID);
        doReturn(STORAGE_LOCATION_ID).when(helper).createOrUpdateS3StorageLocationForProject(eq(PROJECT_ID), any());

        // Execute.
        long storageLocationId = helper.ensureS3StorageLocationInProject(PROJECT_ID);
        assertEquals(storageLocationId, STORAGE_LOCATION_ID);

        // Verify no backend calls.
        verify(helper, never()).createOrUpdateS3StorageLocationForProject(any(), any());
    }

    @Test
    public void getS3StorageLocationIdForProject_NoUploadLocations() throws Exception {
        when(mockClient.getUploadDestinationLocations(PROJECT_ID)).thenReturn(new UploadDestinationLocation[0]);
        Long storageLocationId = helper.getS3StorageLocationIdForProject(PROJECT_ID);
        assertNull(storageLocationId);
    }

    @Test
    public void getS3StorageLocationIdForProject_NonS3UploadLocation() throws Exception {
        // Mock Synapse Client.
        UploadDestinationLocation location = new UploadDestinationLocation();
        location.setStorageLocationId(STORAGE_LOCATION_ID);
        location.setUploadType(UploadType.SFTP);
        when(mockClient.getUploadDestinationLocations(PROJECT_ID)).thenReturn(
                new UploadDestinationLocation[] { location });

        // Execute and validate.
        Long storageLocationId = helper.getS3StorageLocationIdForProject(PROJECT_ID);
        assertNull(storageLocationId);
    }

    @Test
    public void getS3StorageLocationIdForProject_DefaultUploadLocation() throws Exception {
        // Mock Synapse Client.
        UploadDestinationLocation location = new UploadDestinationLocation();
        location.setStorageLocationId(1L);
        location.setUploadType(UploadType.S3);
        when(mockClient.getUploadDestinationLocations(PROJECT_ID)).thenReturn(
                new UploadDestinationLocation[] { location });

        // Execute and validate.
        Long storageLocationId = helper.getS3StorageLocationIdForProject(PROJECT_ID);
        assertNull(storageLocationId);
    }

    @Test
    public void getS3StorageLocationIdForProject_MultipleUploadLocations() throws Exception {
        // Mock Synapse Client.
        UploadDestinationLocation sftpLocation = new UploadDestinationLocation();
        sftpLocation.setStorageLocationId(1357L);
        sftpLocation.setUploadType(UploadType.SFTP);

        UploadDestinationLocation s3Location = new UploadDestinationLocation();
        s3Location.setStorageLocationId(STORAGE_LOCATION_ID);
        s3Location.setUploadType(UploadType.S3);

        when(mockClient.getUploadDestinationLocations(PROJECT_ID)).thenReturn(
                new UploadDestinationLocation[] { sftpLocation, s3Location });

        // Execute and validate.
        Long storageLocationId = helper.getS3StorageLocationIdForProject(PROJECT_ID);
        assertEquals(storageLocationId.longValue(), STORAGE_LOCATION_ID);
    }

    @Test
    public void createOrUpdateS3StorageLocationForProject_SettingsAlreadyExist() throws Exception {
        // Mock Synapse calls.
        StorageLocationSetting createdSetting = mock(StorageLocationSetting.class);
        when(createdSetting.getStorageLocationId()).thenReturn(STORAGE_LOCATION_ID);
        when(mockClient.createStorageLocationSetting(any(StorageLocationSetting.class))).thenReturn(createdSetting);

        UploadDestinationListSetting projectSetting = new UploadDestinationListSetting();
        projectSetting.setLocations(new ArrayList<>());
        when(mockClient.getProjectSetting(PROJECT_ID, ProjectSettingsType.upload)).thenReturn(projectSetting);

        // Execute.
        StorageLocationSetting helperInputSetting = mock(StorageLocationSetting.class);
        helper.createOrUpdateS3StorageLocationForProject(PROJECT_ID, helperInputSetting);

        // Verify back-end calls.
        verify(mockClient).createStorageLocationSetting(same(helperInputSetting));

        ArgumentCaptor<UploadDestinationListSetting> updatedProjectSettingCaptor = ArgumentCaptor.forClass(
                UploadDestinationListSetting.class);
        verify(mockClient).updateProjectSetting(updatedProjectSettingCaptor.capture());
        UploadDestinationListSetting updatedProjectSetting = updatedProjectSettingCaptor.getValue();
        List<Long> locationIdList = updatedProjectSetting.getLocations();
        assertEquals(locationIdList.size(), 1);
        assertEquals(locationIdList.get(0).longValue(), STORAGE_LOCATION_ID);
    }

    @Test
    public void createOrUpdateS3StorageLocationForProject_NewSetting() throws Exception {
        // Mock Synapse calls.
        StorageLocationSetting createdSetting = mock(StorageLocationSetting.class);
        when(createdSetting.getStorageLocationId()).thenReturn(STORAGE_LOCATION_ID);
        when(mockClient.createStorageLocationSetting(any(StorageLocationSetting.class))).thenReturn(createdSetting);

        when(mockClient.getProjectSetting(PROJECT_ID, ProjectSettingsType.upload)).thenThrow(SynapseNotFoundException.class);

        // Execute.
        StorageLocationSetting helperInputSetting = mock(StorageLocationSetting.class);
        helper.createOrUpdateS3StorageLocationForProject(PROJECT_ID, helperInputSetting);

        // Verify back-end calls.
        verify(mockClient).createStorageLocationSetting(same(helperInputSetting));

        ArgumentCaptor<UploadDestinationListSetting> createdProjectSettingCaptor = ArgumentCaptor.forClass(
                UploadDestinationListSetting.class);
        verify(mockClient).createProjectSetting(createdProjectSettingCaptor.capture());
        UploadDestinationListSetting createdProjectSetting = createdProjectSettingCaptor.getValue();
        assertEquals(createdProjectSetting.getProjectId(), PROJECT_ID);
        assertEquals(createdProjectSetting.getSettingsType(), ProjectSettingsType.upload);

        List<Long> locationIdList = createdProjectSetting.getLocations();
        assertEquals(locationIdList.size(), 2);
        assertEquals(locationIdList.get(0).longValue(), SynapseHelper.DEFAULT_STORAGE_LOCATION_ID);
        assertEquals(locationIdList.get(1).longValue(), STORAGE_LOCATION_ID);
    }
}
