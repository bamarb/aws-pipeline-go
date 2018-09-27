//Package georedis port of node georedis to go lang using radix.v3
package geostore

import radix "github.com/mediocregopher/radix.v3"

// Order an enum for sort order
type Order int

const (
	// ASC sort order ascending
	ASC Order = iota
	// DESC sort order descending
	DESC
)

// Distance an enum for Distance units
type Distance int

const (
	// FT Distance in feet
	FT Distance = iota
	// M Distance in Meters
	M
	// KM Distance in Kilo Meters
	KM
	// MI Distance in Miles
	MI
)

// QueryOptions for nearby queries
type QueryOptions struct {
	WithCoords    bool
	WithHashes    bool
	WithDistances bool
	Sort          Order
	Units         Distance
	Count         int
}

//GeoRadiusRequest type is used to make requests for near by stores
type GeoRadiusRequest struct {
	idx    string
	lat    string
	lng    string
	radius string
}

//GeoRadiusDistInfo holds radius info with distance
type GeoRadiusDistInfo struct {
	LocID    string
	Distance float64
}

// GeoLocationStore Wrappers around Geo* Commands
type GeoLocationStore interface {
	AddOrUpdateLocations(indexName string, locs ...string) (int64, error)
	DeleteLocations(indexName string, locs ...string) (int64, error)
	NearbyWithDist(indexName string, lat string,
		lng string, radius string) ([]GeoRadiusDistInfo, error)
	Count(indexName string) (int64, error)
}

// NewGeoLocationStore  ctor
func NewGeoLocationStore(pool *radix.Pool) GeoLocationStore {
	return &redisLocationStore{pool}
}
