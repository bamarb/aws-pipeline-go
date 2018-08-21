//Package georedis port of node georedis to go lang using radix.v3
package geostore

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

// GeoLocationStore Wrappers around Geo* Commands
type GeoLocationStore interface {
	AddOrUpdateLocations(indexName string, locs ...string) (int64, error)
	DeleteLocations(indexName string, locs ...string) (int64, error)
	Nearby(indexName string, lat string, lng string, radius string) ([]string, error)
	Count(indexName string) (int64, error)
}
