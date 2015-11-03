package org.sagebionetworks.bridge.exporter.request;

import java.util.EnumSet;
import java.util.Set;

import org.sagebionetworks.bridge.exporter.dynamo.SharingScope;

/**
 * Configuration for whether Bridge-EX should export all data, shared data, or public data. These somewhat correspond
 * to no_sharing, sponsors_and_partners (sparsely shared), and all_qualified_researchers (broadly shared). Each
 * sharing mode also includes less restrictive sharing options.
 */
public enum BridgeExporterSharingMode {
    /** Export all data. */
    ALL(EnumSet.allOf(SharingScope.class)),

    /** Export both sparsely (sponsors_and_partners) and broadly (all_qualified_researchers) shared data. */
    SHARED(EnumSet.of(SharingScope.SPONSORS_AND_PARTNERS, SharingScope.ALL_QUALIFIED_RESEARCHERS)),

    /** Export broadly shared data (all_qualfiied_researchers) only. */
    PUBLIC_ONLY(EnumSet.of(SharingScope.ALL_QUALIFIED_RESEARCHERS));

    private final Set<SharingScope> acceptedScopeSet;

    BridgeExporterSharingMode(Set<SharingScope> acceptedScopeSet) {
        this.acceptedScopeSet = acceptedScopeSet;
    }

    /** Returns true if the sharing scope should *excluded* from export. */
    public boolean shouldFilterScope(SharingScope sharingScope) {
        return !acceptedScopeSet.contains(sharingScope);
    }
}
