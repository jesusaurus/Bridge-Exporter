package org.sagebionetworks.bridge.exporter.sharing;

import java.util.EnumSet;
import java.util.Set;

import org.sagebionetworks.bridge.exporter.sharing.SharingScope;

// TODO doc
public enum BridgeExporterSharingMode {
    ALL(EnumSet.allOf(SharingScope.class)),
    SHARED(EnumSet.of(SharingScope.SPONSORS_AND_PARTNERS, SharingScope.ALL_QUALIFIED_RESEARCHERS)),
    PUBLIC_ONLY(EnumSet.of(SharingScope.ALL_QUALIFIED_RESEARCHERS));

    private final Set<SharingScope> acceptedScopeSet;

    BridgeExporterSharingMode(Set<SharingScope> acceptedScopeSet) {
        this.acceptedScopeSet = acceptedScopeSet;
    }

    public boolean shouldFilterScope(SharingScope sharingScope) {
        return !acceptedScopeSet.contains(sharingScope);
    }
}
