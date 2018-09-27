package geostore

import (
	"strconv"

	"github.com/mediocregopher/radix.v3"
)

var defaultOpts = []string{"m", "WITHDIST", "COUNT", "100", "ASC"}

type redisLocationStore struct {
	p *radix.Pool
}

func (rs *redisLocationStore) AddOrUpdateLocations(idx string, locs ...string) (int64, error) {
	var ret int64
	err := rs.p.Do(radix.FlatCmd(&ret, "GEOADD", idx, locs))
	return ret, err
}

func (rs *redisLocationStore) DeleteLocations(idx string, locids ...string) (int64, error) {
	var ret int64
	err := rs.p.Do(radix.FlatCmd(&ret, "ZREM", idx, locids))
	return ret, err
}

func (rs *redisLocationStore) Count(idx string) (int64, error) {
	var ret int64
	err := rs.p.Do(radix.Cmd(&ret, "ZCARD", idx))
	return ret, err
}

func (rs *redisLocationStore) NearbyWithDist(idx string,
	lat string, lng string, radius string) ([]GeoRadiusDistInfo, error) {
	var resp [][]string
	err := rs.p.Do(radix.FlatCmd(&resp, "GEORADIUS", idx, lng, lat, radius, defaultOpts))
	if err != nil {
		return nil, err
	}
	ret := make([]GeoRadiusDistInfo, len(resp))
	for i := 0; i < len(resp); i++ {
		dist, _ := strconv.ParseFloat(resp[i][1], 64)
		ret[i] = GeoRadiusDistInfo{LocID: resp[i][0], Distance: dist}
	}
	return ret, err
}

/*
func (rs *redisLocationStore) MultiNearbyWithDist(reqs []GeoRadiusRequest) ([][]GeoRadiusDistInfo, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	ret := make([][]GeoRadiusDistInfo, len(reqs))
	//Initialze the inner slice

}
*/
