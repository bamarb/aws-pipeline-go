package geostore

import (
	"github.com/mediocregopher/radix.v3"
)

const defaultOpts string = "m WITHDIST ASC COUNT 100"

type redisLocationStore struct {
	p *radix.Pool
}

func (rs *redisLocationStore) AddLocations(idx string, locs ...string) (int64, error) {
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

func (rs *redisLocationStore) Nearby(idx string, lat string, lng string, radius string) ([]string, error) {
	var ret []string
	err := rs.p.Do(radix.Cmd(&ret, "GEORADIUS", idx, lng, lat, radius, defaultOpts))
	return ret, err
}
