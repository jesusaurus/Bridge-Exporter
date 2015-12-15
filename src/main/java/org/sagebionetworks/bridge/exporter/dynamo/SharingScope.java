package org.sagebionetworks.bridge.exporter.dynamo;

/** Enum for user sharing scopes. */
// TODO consolidate this with BridgePF ParticipantOption.SharingScope
public enum SharingScope {
    /** Data should not be shared. */
    NO_SHARING,

    /** Data should be shared sparsely. */
    SPONSORS_AND_PARTNERS,

    /** Data should be shared broadly. */
    ALL_QUALIFIED_RESEARCHERS,
}
