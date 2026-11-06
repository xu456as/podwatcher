package store

import (
	"encoding/json"
	"os"

	"k8s.io/klog/v2"
)

// ImportLegacy loads records from the pre-single-file applications and pod
// record files into the current store. It is a one-time migration, intended to
// run when no state snapshot exists yet; the source files are never deleted.
// Records are indexed and then passed through the normal eviction pass so the
// merged model converges immediately.
func (s *Store) ImportLegacy(appFile, podFile string) {
	applications := readLegacyApplications(appFile)
	pods := readLegacyPods(podFile)
	if len(applications) == 0 && len(pods) == 0 {
		return
	}

	s.mu.Lock()
	s.ingestAppsLocked(applications)
	for i := range pods {
		rec := pods[i]
		uid := rec.UID
		if uid == "" {
			uid = rec.Namespace + "/" + rec.Name
		}
		s.pods[uid] = &rec
		if rec.Role == "driver" {
			s.driverByApp[rec.ApplicationID] = uid
		}
	}
	appEvicted := s.evictAppsLocked()
	podEvicted := s.evictPodsLocked()
	s.dirty = true
	s.mu.Unlock()

	klog.Infof("Migrated %d applications and %d pod records from legacy files (%d evicted on convergence); sources left in place: %s, %s",
		len(applications), len(pods), appEvicted+podEvicted, appFile, podFile)
}

// readLegacyApplications parses the old applications file, accepting both the
// bare JSON array layout and the transitional layout that wrapped the array
// with a monotonic revision counter.
func readLegacyApplications(file string) []ApplicationRecord {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil
	}
	var records []ApplicationRecord
	if err := json.Unmarshal(data, &records); err == nil {
		return records
	}
	var wrapped struct {
		Revision int64               `json:"revision"`
		Records  []ApplicationRecord `json:"records"`
	}
	if err := json.Unmarshal(data, &wrapped); err != nil {
		klog.Warningf("Failed to parse legacy applications file %s: %v", file, err)
		return nil
	}
	return wrapped.Records
}

// readLegacyPods parses the old bare-array pod records file.
func readLegacyPods(file string) []PodRecord {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil
	}
	var records []PodRecord
	if err := json.Unmarshal(data, &records); err != nil {
		klog.Warningf("Failed to parse legacy pod records file %s: %v", file, err)
		return nil
	}
	return records
}
